"""
Analyze block lifecycles from parsed log records.

Reads records.json from logs/<experiment>/ and analyzes block lifecycles,
showing the sequence of events for each block.

Usage:
    python analyse_lifecycle.py <experiment_name> [OPTIONS]

Options:
    --mode MODE               Analysis mode: 'signatures' (default) or 'slowest'
    --samples [N]            Show N samples per type signature group (default: 1)
    --limit N                 Only show top N signatures by block count (default: all)
    --min-block-size SIZE     Only include blocks >= SIZE bytes (e.g. 100000 or 100K)
    --max-block-size SIZE     Only include blocks <= SIZE bytes (e.g. 100000 or 100K)
    --skip-block-full         Skip all records with type 'block_full'
"""

from datetime import datetime
from pathlib import Path
from typing import List, Optional
import argparse
import json
import sys

from log_types import (
    LogRecord,
    dict_to_record,
    c_label,
    c_value,
    c_ok,
    c_warn,
    c_dim,
    parse_size_arg,
    size_to_k_suffix,
    group_records_by_block_id,
    get_block_size,
    has_validator_session,
    filter_records_by_block_size,
)


# ---------------------------------------------------------------------------
# Type signature helpers
# ---------------------------------------------------------------------------

def get_type_signature(records: List[LogRecord]) -> tuple[tuple[str, int | str], ...]:
    """
    Get the type signature for a block's records.
    
    Returns a tuple of (type, count) pairs, sorted by type name.
    This can be used as a dictionary key to group blocks by their type pattern.
    
    Note: block_full count is always normalized to '*' since the exact count varies.
    """
    type_counts: dict[str, int] = {}
    for rec in records:
        type_counts[rec.type] = type_counts.get(rec.type, 0) + 1
    
    # Normalize: block_full count is always '*'
    result: list[tuple[str, int | str]] = []
    for typ, cnt in sorted(type_counts.items()):
        if typ == "block_full":
            result.append((typ, "*"))
        else:
            result.append((typ, cnt))
    
    return tuple(result)


def format_type_signature(sig: tuple[tuple[str, int | str], ...]) -> str:
    """Format a type signature as a human-readable string."""
    return ", ".join(f"{typ}={cnt}" for typ, cnt in sig)


# ---------------------------------------------------------------------------
# Lifecycle printing
# ---------------------------------------------------------------------------

def print_block_lifecycle(block_id: str, records: List[LogRecord]) -> None:
    """
    Print the lifecycle of a single block showing all events in chronological order.
    
    Args:
        block_id: The block identifier
        records: List of LogRecord for this block, already sorted by timestamp
    """
    if not records:
        print(f"  {c_warn('No records')}")
        return
    
    # Use this block's first event as the origin
    origin_ts = min(r.start_ts for r in records)
    
    print(f"\n{c_label('Block:')} {c_value(block_id)} { 'size:'} {c_value(get_block_size(records))}")
    print(f"  {c_dim('Total events:')} {len(records)}")
    
    # Count by stage and type
    stage_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    node_set: set[int] = set()
    
    for rec in records:
        stage_counts[rec.stage] = stage_counts.get(rec.stage, 0) + 1
        type_counts[rec.type] = type_counts.get(rec.type, 0) + 1
        node_set.add(rec.node_id)
    
    print(f"  {c_dim('Stages:')} {', '.join(f'{k}={v}' for k, v in sorted(stage_counts.items()))}")
    print(f"  {c_dim('Types:')} {', '.join(f'{k}={v}' for k, v in sorted(type_counts.items()))}")
    print(f"  {c_dim('Nodes involved:')} {len(node_set)}")
    print(f"  {c_dim('Started at:')} {origin_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

    # Timeline
    print(f"  {c_dim('Timeline:')}")
    for i, rec in enumerate(records):
        rel_start = (rec.start_ts - origin_ts).total_seconds()
        rel_end = (rec.end_ts - origin_ts).total_seconds()
        
        # Color code by stage
        if rec.stage == "compress":
            stage_str = c_ok(rec.stage)
        else:
            stage_str = c_warn(rec.stage)
        
        called_from_str = f" ({rec.called_from})" if rec.called_from else ""
        
        print(
            f"    {i+1:3d}. "
            f"[{rel_start:+8.3f}s -> {rel_end:+8.3f}s] "
            f"node={rec.node_id:<12s} "
            f"{stage_str:<20s} "
            f"{rec.type}{called_from_str}"
        )


def print_lifecycles_by_type_signature(
    records: List[LogRecord],
    show_sample_per_group: int = 1,
    min_events: int = 2,
    limit: Optional[int] = None,
) -> None:
    """
    Group blocks by their type signature and print statistics for each group.
    
    A type signature is the combination of types and their occurrence counts.
    For example: (block_broadcast=25, block_candidate_broadcast=48)
    
    Args:
        records: All log records (should be pre-filtered by block size if needed)
        show_sample_per_group: Number of sample lifecycles to show per signature group (0 to skip)
        min_events: Minimum number of events required to include a block
        limit: Only show top N signatures by block count (None = show all)
    """
    grouped = group_records_by_block_id(records)
    
    # Filter blocks with minimum events
    filtered = {
        k: v for k, v in grouped.items() 
        if len(v) >= min_events 
        # and not has_validator_session(v)
    }
    
    if not filtered:
        print(f"{c_warn('No blocks found matching criteria')}")
        return
    
    # Group blocks by their type signature
    by_signature: dict[tuple[tuple[str, int], ...], list[tuple[str, List[LogRecord]]]] = {}
    for block_id, block_records in filtered.items():
        sig = get_type_signature(block_records)
        by_signature.setdefault(sig, []).append((block_id, block_records))
    
    # Count skipped blocks
    validator_session_blocks = sum(1 for v in grouped.values() if has_validator_session(v))
    
    print(f"\n{'='*80}")
    print(f"{c_label('BLOCK LIFECYCLES BY TYPE SIGNATURE')}")
    print(f"{'='*80}")
    print(f"  {c_dim('Total unique blocks:')} {len(filtered)} {c_dim(f'(skipped {validator_session_blocks} validator_session)')}")
    print(f"  {c_dim('Unique type signatures:')} {len(by_signature)}")
    
    # Sort signatures by number of blocks (most common first)
    sorted_signatures = sorted(by_signature.items(), key=lambda x: len(x[1]), reverse=True)
    
    # Apply limit if specified
    if limit is not None and limit > 0:
        sorted_signatures = sorted_signatures[:limit]
    
    for sig, blocks in sorted_signatures:
        total_events = sum(len(recs) for _, recs in blocks)
        print(f"\n{'-'*80}")
        print(f"{c_label('Signature:')} {c_value(format_type_signature(sig))}")
        print(f"  {c_dim('Blocks with this signature:')} {len(blocks)}")
        print(f"  {c_dim('Total events:')} {total_events}")
        
        if show_sample_per_group > 0:
            # Show sample lifecycles from this group
            # Sort by earliest timestamp to get representative samples
            sorted_blocks = sorted(blocks, key=lambda x: min(r.start_ts for r in x[1]))
            samples = sorted_blocks[:show_sample_per_group]
            
            for block_id, block_records in samples:
                print_block_lifecycle(block_id, block_records)


# ---------------------------------------------------------------------------
# Records loading
# ---------------------------------------------------------------------------

def load_records_from_json(experiment_name: str, base_dir: str = "logs") -> List[LogRecord]:
    """
    Load parsed records from records.json.
    
    Args:
        experiment_name: Name of the experiment
        base_dir: Base directory for logs
    
    Returns:
        List of LogRecord objects
    """
    records_path = Path(base_dir) / experiment_name / "records.json"
    
    if not records_path.exists():
        raise FileNotFoundError(
            f"records.json not found: {records_path}\n"
            f"Run 'python parse_logs.py {experiment_name}' first."
        )
    
    with open(records_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return [dict_to_record(d) for d in data["records"]]


def print_slowest_blocks(
    records: List[LogRecord],
    limit: Optional[int] = None,
) -> None:
    """
    Show the slowest blocks by total duration (first event start to last event end).

    Args:
        records: All log records (should be pre-filtered)
        limit: Maximum number of blocks to show (None = show all)
    """
    grouped = group_records_by_block_id(records)

    if not grouped:
        print(f"{c_warn('No blocks found')}")
        return

    # Calculate duration for each block
    block_durations = []
    for block_id, block_records in grouped.items():
        if not block_records:
            continue

        # Sort records by start time
        sorted_records = sorted(block_records, key=lambda r: r.start_ts)

        # Calculate total duration from first start to last end
        first_start = sorted_records[0].start_ts
        last_end = max(r.end_ts for r in sorted_records)
        total_duration = (last_end - first_start).total_seconds()

        block_durations.append((block_id, sorted_records, total_duration))

    # Sort by duration (slowest first)
    block_durations.sort(key=lambda x: x[2], reverse=True)

    # Apply limit if specified
    if limit is not None and limit > 0:
        block_durations = block_durations[:limit]

    print(f"\n{'='*80}")
    print(f"{c_label('SLOWEST BLOCKS BY TOTAL DURATION')}")
    print(f"{'='*80}")
    print(f"  {c_dim('Total blocks analyzed:')} {len(grouped)}")
    print(f"  {c_dim('Showing top:')} {len(block_durations)}")

    for i, (block_id, block_records, duration) in enumerate(block_durations, 1):
        print(f"\n{'-'*80}")
        print(f"{c_label(f'#{i} Slowest Block')} - {c_value(f'{duration:.3f}s total')}")
        print_block_lifecycle(block_id, block_records)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Analyze block lifecycles from records.json."""
    parser = argparse.ArgumentParser(
        description="Analyze block lifecycles from records.json.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("experiment_name", help="Experiment directory under logs/")
    parser.add_argument(
        "--mode",
        choices=("signatures", "slowest"),
        default="signatures",
        help="Analysis mode: signatures (default) or slowest",
    )
    parser.add_argument(
        "--samples",
        nargs="?",
        const=1,
        default=1,
        type=int,
        help="Show N samples per type signature group (default: 1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only show top N results (signatures or slowest blocks)",
    )
    parser.add_argument(
        "--min-block-size",
        help="Only include blocks >= SIZE bytes (e.g. 100000 or 100K)",
    )
    parser.add_argument(
        "--max-block-size",
        help="Only include blocks <= SIZE bytes (e.g. 100000 or 100K)",
    )
    parser.add_argument(
        "--skip-block-full",
        action="store_true",
        help="Skip all records with type 'block_full'",
    )

    args = parser.parse_args()

    experiment_name = args.experiment_name
    samples_per_sig = args.samples
    sig_limit = args.limit
    skip_block_full = args.skip_block_full
    mode = args.mode

    if samples_per_sig < 0:
        parser.error("--samples must be >= 0")
    if sig_limit is not None and sig_limit <= 0:
        parser.error("--limit must be > 0")

    min_block_size = 0
    max_block_size = 0
    if args.min_block_size:
        try:
            min_block_size = parse_size_arg(args.min_block_size)
        except Exception as exc:
            parser.error(f"invalid --min-block-size: {exc}")
    if args.max_block_size:
        try:
            max_block_size = parse_size_arg(args.max_block_size)
        except Exception as exc:
            parser.error(f"invalid --max-block-size: {exc}")
    if min_block_size > 0 and max_block_size > 0 and min_block_size > max_block_size:
        parser.error("--min-block-size cannot be greater than --max-block-size")
    
    print(f"{c_label('Experiment:')} {c_value(experiment_name)}")

    records = load_records_from_json(experiment_name)
    print(f"{c_label('Total records loaded:')} {c_value(str(len(records)))}")
    
    # Filter records by block size early (affects all subsequent processing)
    if min_block_size > 0 or max_block_size > 0:
        records = filter_records_by_block_size(records, min_block_size, max_block_size)
        size_filter_msg = []
        if min_block_size > 0:
            size_filter_msg.append(f">= {size_to_k_suffix(min_block_size)}")
        if max_block_size > 0:
            size_filter_msg.append(f"<= {size_to_k_suffix(max_block_size)}")
        print(f"{c_label('Records after size filter')} ({', '.join(size_filter_msg)}): {c_value(str(len(records)))}")

    # Filter out block_full records if requested
    if skip_block_full:
        original_count = len(records)
        records = [r for r in records if r.type != "block_full"]
        skipped_count = original_count - len(records)
        print(f"{c_label('Records after skipping block_full')}: {c_value(str(len(records)))} {c_dim(f'(skipped {skipped_count} block_full records)')}")
    
    # Show results based on mode
    if mode == "signatures":
        print_lifecycles_by_type_signature(records, show_sample_per_group=samples_per_sig, min_events=2, limit=sig_limit)
    elif mode == "slowest":
        print_slowest_blocks(records, limit=sig_limit)


if __name__ == "__main__":
    main()
