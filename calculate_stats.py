"""
Calculate statistics from parsed log records.

Reads records.json from logs/<experiment>/ and computes aggregated statistics
for each (type, called_from) combination, writing results to stats/<experiment>.json.

Usage:
    python calculate_stats.py <experiment_name> [OPTIONS]

Output:
    stats/<experiment_name>.json
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import json
import sys

import numpy as np

from log_types import (
    LogRecord,
    dict_to_record,
    c_label,
    c_value,
    c_ok,
    c_warn,
    c_dim,
)


# ---------------------------------------------------------------------------
# Statistics dataclass
# ---------------------------------------------------------------------------

@dataclass
class TypeCalledFromStats:
    """
    Aggregated statistics for a given (type, called_from) combination.

    All time coordinates are expressed as seconds (float) relative to a
    chosen origin (e.g. the earliest timestamp seen in the dataset).
    """
    num_blocks: int
    block_size_points: list[tuple[float, int, str]]
    compression_percent_points: list[tuple[float, float, str]]
    broadcast_time_avg_points: list[tuple[float, float, str]]
    broadcast_time_full_points: list[tuple[float, float, str]]
    broadcast_time_66p_points: list[tuple[float, float, str]]
    compression_time_points: list[tuple[float, float, str]]
    decompression_time_points: list[tuple[float, float, str]]
    block_size_by_id: dict[str, int]


# ---------------------------------------------------------------------------
# Grouping functions
# ---------------------------------------------------------------------------

def group_by_type_called_from_and_block(
    records: List[LogRecord],
) -> dict[tuple[str, Optional[str]], dict[str, List[LogRecord]]]:
    """
    Group records first by (type, called_from) pair and then by block_id.

    Returns:
        {
          (type, called_from): {
              block_id: [LogRecord, ...],
              ...
          },
          ...
        }
    """
    grouped: dict[tuple[str, Optional[str]], dict[str, List[LogRecord]]] = {}

    for rec in records:
        key = (rec.type, rec.called_from)
        by_block = grouped.setdefault(key, {})
        by_block.setdefault(rec.block_id, []).append(rec)

    # Sort each inner list by START timestamp for convenience.
    for by_block in grouped.values():
        for recs in by_block.values():
            recs.sort(key=lambda r: r.start_ts)

    return grouped


# ---------------------------------------------------------------------------
# Statistics computation
# ---------------------------------------------------------------------------

def compute_stats_for_type_called_from(
    by_block: dict[str, List[LogRecord]],
    earliest_ts_global: datetime,
) -> TypeCalledFromStats:
    """
    Compute statistics for a single (type, called_from) combination.

    Returns:
        TypeCalledFromStats with:
          - num_blocks
          - per-block time series (block size, compression %, broadcast times)
          - per-record time series for compress/decompress durations
    """
    num_blocks = len(by_block)

    # Accumulate with absolute datetimes first, then shift to origin.
    block_size_points_dt: list[tuple[datetime, int, str]] = []
    compression_percent_points_dt: list[tuple[datetime, float, str]] = []
    broadcast_time_avg_points_dt: list[tuple[datetime, float, str]] = []
    broadcast_time_full_points_dt: list[tuple[datetime, float, str]] = []
    broadcast_time_66p_points_dt: list[tuple[datetime, float, str]] = []
    compression_time_points_dt: list[tuple[datetime, float, str]] = []
    decompression_time_points_dt: list[tuple[datetime, float, str]] = []
    block_size_by_id: dict[str, int] = {}
    cnt_blocks_with_several_sizes = 0
    cnt_blocks_with_no_compress_records = 0
    cnt_blocks_with_no_decompress_records = 0
    cnt_blocks_with_no_original_size = 0
    cnt_blocks_with_no_compressed_size = 0
    
    for block_id, recs in by_block.items():
        # Compression time data: all compress records
        for rec in recs:
            if rec.stage == "compress":
                compression_time_points_dt.append((rec.end_ts, rec.duration_sec, block_id))
            elif rec.stage == "decompress":
                decompression_time_points_dt.append((rec.end_ts, rec.duration_sec, block_id))

        # Broadcast time data: requires both compress and decompress records.
        compress_ts = sorted([r.start_ts for r in recs if r.stage == "compress"])
        decompress_ts = sorted([r.end_ts for r in recs if r.stage == "decompress"])
        if compress_ts and decompress_ts:
            # Use the earliest compress start as the block's timestamp.
            ts_block = min(compress_ts)

            earliest_compress_ts = min(compress_ts)
            latest_decompress_ts = max(decompress_ts)

            # Full broadcast time: first compress START to last decompress END.
            full_broadcast = (latest_decompress_ts - earliest_compress_ts).total_seconds()
            broadcast_time_full_points_dt.append((ts_block, full_broadcast, block_id))

            # Average broadcast time: first compress START to average decompress END.
            avg_decomp_seconds = sum(dt.timestamp() for dt in decompress_ts) / len(decompress_ts)
            avg_decomp_dt = datetime.fromtimestamp(avg_decomp_seconds)
            avg_broadcast = (avg_decomp_dt - earliest_compress_ts).total_seconds()
            broadcast_time_avg_points_dt.append((ts_block, avg_broadcast, block_id))

            # 66th percentile broadcast time over decompression END times
            decomp_secs = np.array([(dt - earliest_compress_ts).total_seconds() for dt in decompress_ts])
            broadcast_time_66p_points_dt.append((ts_block, float(np.percentile(decomp_secs, 66)), block_id))
        else:
            if len(compress_ts) == 0:
                cnt_blocks_with_no_compress_records += 1
            if len(decompress_ts) == 0:
                cnt_blocks_with_no_decompress_records += 1
            continue

        # Work per block, using any compress records with valid sizes.
        all_original_size = {r.original_size for r in recs if r.original_size is not None and r.original_size > 0}
        all_compressed_size = {r.compressed_size for r in recs if r.compressed_size is not None and r.compressed_size > 0}
        
        if len(all_original_size) == 0:
            cnt_blocks_with_no_original_size += 1
        if len(all_compressed_size) == 0:
            cnt_blocks_with_no_compressed_size += 1
        if len(all_original_size) == 0 or len(all_compressed_size) == 0:
            continue
        
        if len(all_original_size) != 1 or len(all_compressed_size) != 1:
            cnt_blocks_with_several_sizes += 1

        original_size = next(iter(all_original_size))
        compressed_size = next(iter(all_compressed_size))

        # Block size data
        block_size_points_dt.append((ts_block, original_size, block_id))
        block_size_by_id[block_id] = original_size

        # Compression percent data
        compression_percent = (original_size - compressed_size) / original_size
        compression_percent_points_dt.append((ts_block, compression_percent, block_id))

    # Shift all time coordinates to floats (seconds since earliest_ts_global).
    def shift_points_to_origin(
        points: list[tuple[datetime, float | int, str]],
    ) -> list[tuple[float, float | int, str]]:
        return sorted(
            [((ts - earliest_ts_global).total_seconds(), value, block_id) for ts, value, block_id in points],
        )

    if cnt_blocks_with_no_compress_records > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_no_compress_records} blocks have no compress records")
    if cnt_blocks_with_no_decompress_records > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_no_decompress_records} blocks have no decompress records")
    
    if cnt_blocks_with_several_sizes > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_several_sizes} blocks have several sizes")
    
    if cnt_blocks_with_no_original_size > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_no_original_size} blocks have no original size")
    if cnt_blocks_with_no_compressed_size > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_no_compressed_size} blocks have no compressed size")

    return TypeCalledFromStats(
        num_blocks=num_blocks,
        block_size_points=shift_points_to_origin(block_size_points_dt),
        compression_percent_points=shift_points_to_origin(compression_percent_points_dt),
        broadcast_time_avg_points=shift_points_to_origin(broadcast_time_avg_points_dt),
        broadcast_time_full_points=shift_points_to_origin(broadcast_time_full_points_dt),
        broadcast_time_66p_points=shift_points_to_origin(broadcast_time_66p_points_dt),
        compression_time_points=shift_points_to_origin(compression_time_points_dt),
        decompression_time_points=shift_points_to_origin(decompression_time_points_dt),
        block_size_by_id=block_size_by_id,
    )


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
    """Calculate statistics from records.json and write to stats/*.json."""
    if len(sys.argv) < 2:
        print("Usage: python calculate_stats.py <experiment_name> [OPTIONS]")
        sys.exit(1)

    experiment_name = sys.argv[1]
    
    print(f"{c_label('Experiment:')} {c_value(experiment_name)}")

    records = load_records_from_json(experiment_name)
    print(f"{c_label('Total records loaded:')} {c_value(str(len(records)))}")
    
    grouped_to = group_by_type_called_from_and_block(records)
    print(f"{c_label('(type, called_from) combinations:')} {c_value(str(len(grouped_to)))}")

    # Find the earliest timestamp over all data (start_ts and end_ts),
    # so we can shift all time coordinates relative to this origin.
    if not records:
        raise RuntimeError("No records found after filtering")
    earliest_ts_global = min(min(rec.start_ts, rec.end_ts) for rec in records)

    # Collect stats per (type, called_from) for JSON export.
    stats_payload: dict[str, object] = {
        "experiment_name": experiment_name,
        "earliest_ts_global": earliest_ts_global.isoformat(),
        "type_called_from_stats": {},
    }

    to_stats: dict[str, dict[str, object]] = {}

    for (typ, cf), by_block in sorted(
        grouped_to.items(), key=lambda item: (item[0][0] or "", str(item[0][1]))
    ):
        # Print header before computing (so warnings appear under the right header)
        type_label = f"Type={c_value(typ)}"
        if cf is not None:
            type_label += f", called_from={c_value(cf)}"
        print(f"\n{type_label}")
        
        stats = compute_stats_for_type_called_from(by_block, earliest_ts_global)

        # Key for this (type, called_from) pair in the JSON.
        key = f"{typ or 'None'}__{cf or 'None'}"
        to_stats[key] = {
            "type": typ,
            "called_from": cf,
            "num_blocks": stats.num_blocks,
            "block_size_points": stats.block_size_points,
            "compression_percent_points": stats.compression_percent_points,
            "broadcast_time_avg_points": stats.broadcast_time_avg_points,
            "broadcast_time_full_points": stats.broadcast_time_full_points,
            "broadcast_time_66p_points": stats.broadcast_time_66p_points,
            "compression_time_points": stats.compression_time_points,
            "decompression_time_points": stats.decompression_time_points,
            "block_size_by_id": stats.block_size_by_id,
        }

        # Print summary after computing
        print(f"  {c_dim('Block ids:')} {stats.num_blocks}")
        print(f"  {c_dim('Compression records:')} {len(stats.compression_time_points)}")
        print(f"  {c_dim('Decompression records:')} {len(stats.decompression_time_points)}")

    stats_payload["type_called_from_stats"] = to_stats

    # Ensure stats directory exists and write JSON file named after the experiment.
    stats_dir = Path("stats")
    stats_dir.mkdir(parents=True, exist_ok=True)
    out_path = stats_dir / f"{experiment_name}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(stats_payload, f, indent=2)
    print(f"\n{c_ok('Done.')} Stats written to {c_value(str(out_path))}")


if __name__ == "__main__":
    main()
