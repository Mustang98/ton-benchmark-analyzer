"""
Analyze block lifecycles from parsed log records.

Reads records.json from logs/<experiment>/ and analyzes block lifecycles,
showing the sequence of events for each block.

Usage:
    python analyse_lifecycle.py <experiment_name> [OPTIONS]

Options:
    --lifecycle [N]           Show N block lifecycles (default: 5)
    --lifecycle-by-sig [N]    Group lifecycles by type signature, show N samples per group (default: 1)
    --min-block-size SIZE     Only include blocks >= SIZE bytes (e.g. 100000 or 100K)
    --max-block-size SIZE     Only include blocks <= SIZE bytes (e.g. 100000 or 100K)
"""

from datetime import datetime
from pathlib import Path
from typing import List, Optional
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

def print_block_lifecycle(block_id: str, records: List[LogRecord], origin_ts: Optional[datetime] = None) -> None:
    """
    Print the lifecycle of a single block showing all events in chronological order.
    
    Args:
        block_id: The block identifier
        records: List of LogRecord for this block, already sorted by timestamp
        origin_ts: Optional reference timestamp to show relative times
    """
    if not records:
        print(f"  {c_warn('No records')}")
        return
    
    # Use the earliest timestamp as origin if not provided
    if origin_ts is None:
        origin_ts = min(r.start_ts for r in records)
    
    print(f"\n{c_label('Block:')} {c_value(block_id[:40])}{'...' if len(block_id) > 40 else ''}")
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
            f"node={rec.node_id:2d} "
            f"{stage_str:<20s} "
            f"{rec.type}{called_from_str}"
        )


def print_lifecycles_by_type_signature(
    records: List[LogRecord],
    show_sample_per_group: int = 1,
    min_events: int = 2,
) -> None:
    """
    Group blocks by their type signature and print statistics for each group.
    
    A type signature is the combination of types and their occurrence counts.
    For example: (block_broadcast=25, block_candidate_broadcast=48)
    
    Args:
        records: All log records (should be pre-filtered by block size if needed)
        show_sample_per_group: Number of sample lifecycles to show per signature group (0 to skip)
        min_events: Minimum number of events required to include a block
    """
    grouped = group_records_by_block_id(records)
    
    # Filter blocks with minimum events and skip validator_session blocks
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
    
    # Global origin for relative timestamps
    global_origin = min(min(r.start_ts for r in recs) for recs in filtered.values())
    
    # Sort signatures by number of blocks (most common first)
    sorted_signatures = sorted(by_signature.items(), key=lambda x: len(x[1]), reverse=True)
    
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
                print_block_lifecycle(block_id, block_records, origin_ts=global_origin)


def print_sample_lifecycles(
    records: List[LogRecord],
    num_samples: int = 5,
    min_events: int = 2,
    sort_by: str = "events",
) -> None:
    """
    Print lifecycle summaries for a sample of blocks.
    
    Args:
        records: All log records (should be pre-filtered by block size if needed)
        num_samples: Number of block lifecycles to print
        min_events: Minimum number of events required to include a block
        sort_by: How to select blocks - "events" (most events), "random", "earliest"
    """
    grouped = group_records_by_block_id(records)
    
    # Filter blocks with minimum events and skip validator_session blocks
    filtered = {
        k: v for k, v in grouped.items() 
        if len(v) >= min_events 
        # and not has_validator_session(v)
    }
    
    if not filtered:
        print(f"{c_warn('No blocks found matching criteria')}")
        return
    
    # Count skipped blocks
    validator_session_blocks = sum(1 for v in grouped.values() if has_validator_session(v))
    
    print(f"\n{'='*80}")
    print(f"{c_label('BLOCK LIFECYCLES')}")
    print(f"{'='*80}")
    print(f"  {c_dim('Total unique blocks:')} {len(filtered)} {c_dim(f'(skipped {validator_session_blocks} validator_session)')}")
    
    # Select blocks based on sort_by
    if sort_by == "events":
        # Sort by number of events (descending)
        selected = sorted(filtered.items(), key=lambda x: len(x[1]), reverse=True)[:num_samples]
    elif sort_by == "earliest":
        # Sort by earliest timestamp
        selected = sorted(filtered.items(), key=lambda x: min(r.start_ts for r in x[1]))[:num_samples]
    else:  # random
        import random
        items = list(filtered.items())
        random.shuffle(items)
        selected = items[:num_samples]
    
    # Global origin for relative timestamps
    global_origin = min(min(r.start_ts for r in recs) for recs in filtered.values())
    
    for block_id, block_records in selected:
        print_block_lifecycle(block_id, block_records, origin_ts=global_origin)


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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Analyze block lifecycles from records.json."""
    if len(sys.argv) < 2:
        print("Usage: python analyse_lifecycle.py <experiment_name> [OPTIONS]")
        print("Options:")
        print("  --lifecycle [N]           Show N block lifecycles (default: 5)")
        print("  --lifecycle-by-sig [N]    Group lifecycles by type signature, show N samples per group (default: 1)")
        print("  --min-block-size SIZE     Only include blocks >= SIZE bytes (e.g. 100000 or 100K)")
        print("  --max-block-size SIZE     Only include blocks <= SIZE bytes (e.g. 100000 or 100K)")
        sys.exit(1)

    experiment_name = sys.argv[1]
    
    # Check for --lifecycle flag
    show_lifecycle = False
    num_lifecycles = 5
    if "--lifecycle" in sys.argv:
        show_lifecycle = True
        lifecycle_idx = sys.argv.index("--lifecycle")
        if lifecycle_idx + 1 < len(sys.argv) and sys.argv[lifecycle_idx + 1].isdigit():
            num_lifecycles = int(sys.argv[lifecycle_idx + 1])
    
    # Check for --lifecycle-by-sig flag
    show_lifecycle_by_sig = False
    samples_per_sig = 1
    if "--lifecycle-by-sig" in sys.argv:
        show_lifecycle_by_sig = True
        sig_idx = sys.argv.index("--lifecycle-by-sig")
        if sig_idx + 1 < len(sys.argv) and sys.argv[sig_idx + 1].isdigit():
            samples_per_sig = int(sys.argv[sig_idx + 1])
    
    # Check for --min-block-size flag
    min_block_size = 0
    if "--min-block-size" in sys.argv:
        size_idx = sys.argv.index("--min-block-size")
        if size_idx + 1 < len(sys.argv):
            min_block_size = parse_size_arg(sys.argv[size_idx + 1])
    
    # Check for --max-block-size flag
    max_block_size = 0
    if "--max-block-size" in sys.argv:
        size_idx = sys.argv.index("--max-block-size")
        if size_idx + 1 < len(sys.argv):
            max_block_size = parse_size_arg(sys.argv[size_idx + 1])
    
    # Default to --lifecycle-by-sig if no specific mode requested
    if not show_lifecycle and not show_lifecycle_by_sig:
        show_lifecycle_by_sig = True
    
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
    
    # Show lifecycles grouped by type signature if requested
    if show_lifecycle_by_sig:
        print_lifecycles_by_type_signature(records, show_sample_per_group=samples_per_sig, min_events=2)
    elif show_lifecycle:
        print_sample_lifecycles(records, num_samples=num_lifecycles, min_events=2, sort_by="events")


if __name__ == "__main__":
    main()
