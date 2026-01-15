"""
Parse benchmark logs from an experiment directory.

Reads the single benchmark.log file from logs/<experiment>/ and, for each
Broadcast_benchmark record, extracts:
    - node_id      (0-23, zero-based index of the node from devnet-XX hostname)
    - start_ts     (datetime: end_ts - time_sec)
    - end_ts       (datetime from the log line timestamp)
    - block_id
    - type         (candidate, block_full, block_broadcast, etc.)
    - stage        (compress or decompress)
    - called_from  (public, fast-sync, validator_session, etc.)
    - compression  (compressed, none)
    - original_size
    - compressed_size
    - duration_sec (time_sec from the log)

The main entry point is `read_logs_from_experiment`, which returns a flat list
of objects (dataclass instances) with these fields.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta
import json
import os
import re
import sys

import numpy as np


BENCHMARK_MARKER = "Broadcast_benchmark"


# ---------------------------------------------------------------------------
# ANSI color helpers
# ---------------------------------------------------------------------------

def _use_color() -> bool:
    if os.environ.get("NO_COLOR") is not None:
        return False
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


_COLOR = _use_color()


def _c(text: str, code: str) -> str:
    if not _COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"


def c_label(text: str) -> str:
    return _c(text, "1;36")  # bold cyan


def c_value(text: str) -> str:
    return _c(text, "1;37")  # bold white


def c_ok(text: str) -> str:
    return _c(text, "1;32")  # bold green


def c_warn(text: str) -> str:
    return _c(text, "1;33")  # bold yellow


def c_dim(text: str) -> str:
    return _c(text, "2")  # dim


@dataclass
class LogRecord:
    node_id: int              # 0–23
    start_ts: datetime        # START timestamp of the operation (end_ts - duration)
    end_ts: datetime          # END timestamp of the operation (from log line)
    block_id: str
    stage: str                # compress or decompress
    type: str                 # normalized logical type (e.g. candidate, block_full)
    called_from: Optional[str]  # public, fast-sync, validator_session, etc.
    compression: str          # compressed, none, etc.
    original_size: Optional[int]
    compressed_size: Optional[int]
    duration_sec: float       # duration from time_sec field


@dataclass
class TypeCalledFromStats:
    """
    Aggregated statistics for a given (type, called_from) combination.

    All time coordinates are expressed as seconds (float) relative to a
    chosen origin (e.g. the earliest timestamp seen in the dataset).
    """
    num_blocks: int
    block_size_points: list[tuple[float, int]]
    compression_percent_points: list[tuple[float, float]]
    broadcast_time_avg_points: list[tuple[float, float]]
    broadcast_time_full_points: list[tuple[float, float]]
    broadcast_time_66p_points: list[tuple[float, float]]
    compression_time_points: list[tuple[float, float]]
    decompression_time_points: list[tuple[float, float]]


# Match the timestamp in the 3rd bracketed group.
# The first two groups can contain spaces or other characters (e.g. "[ 3][t10]").
_TS_RE = re.compile(r"\[[^\]]+\]\[[^\]]+\]\[([^\]]+)\]")
_BLOCK_ID_RE = re.compile(r"block_id=([^ ]+)")
_CALLED_FROM_RE = re.compile(r"called_from=([^\s]+)")
_TIME_SEC_RE = re.compile(r"time_sec=([^\s]+)")
_COMPRESSION_RE = re.compile(r"compression=([^\s]+)")
_ORIGINAL_SIZE_RE = re.compile(r"original_size=(\d+)")
_COMPRESSED_SIZE_RE = re.compile(r"compressed_size=(\d+)")
_NODE_RE = re.compile(r"devnet-(\d+)")


def _extract_type(line: str) -> Optional[str]:
    """Return the raw token immediately after the BENCHMARK_MARKER, or None."""
    idx = line.find(BENCHMARK_MARKER)
    if idx != -1:
        rest = line[idx + len(BENCHMARK_MARKER):].strip()
        if not rest:
            return None
        return rest.split()[0]
    return None


def _split_type(raw_type: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Split a raw type token into (stage, logical_type).

    Rules:
        - stage is derived from the first word (split by "_"):
            * deserialize -> "decompress"
            * serialize   -> "compress"
        - type is the rest, starting from the second word:
            raw: "deserialize_candidate"
              -> stage = "decompress"
              -> type  = "candidate"
            raw: "serialize_block_broadcast"
              -> stage = "compress"
              -> type  = "block_broadcast"
    """
    if not raw_type:
        return None, None

    parts = raw_type.split("_")
    if not parts:
        return None, raw_type

    first = parts[0].lower()
    rest_parts = parts[1:] or ["unknown"]

    if first in ("decompress", "deserialize"):
        stage: Optional[str] = "decompress"
    elif first in ("compress", "serialize"):
        stage = "compress"
    else:
        # Unknown prefix: keep everything as-is in the type, but stage is None.
        return None, raw_type

    logical_type = "_".join(rest_parts) if rest_parts else None

    return stage, logical_type


def _extract_timestamp(line: str) -> str:
    """
    Extract the timestamp from the third bracketed group:
        [ 4][t20][2026-01-13 21:16:11.352179434][...]
    """
    m = _TS_RE.search(line)
    return m.group(1) if m else ""


def _extract_block_id(line: str) -> str:
    """Extract the block_id=... token (without trailing spaces)."""
    m = _BLOCK_ID_RE.search(line)
    return m.group(1) if m else ""


def _extract_node_id(line: str) -> int:
    """
    Extract node ID from the line prefix (e.g., 'devnet-05' -> 4 zero-indexed).
    Returns -1 if not found.
    """
    m = _NODE_RE.search(line)
    if m:
        return int(m.group(1)) - 1  # Convert to 0-indexed
    return -1


def _timestamp_to_datetime(ts: str) -> datetime:
    """
    Convert a timestamp string like '2026-01-13 21:16:11.352179434'
    into a datetime, trimming excess fractional digits if needed.
    """
    if "." in ts:
        base, frac = ts.split(".", 1)
        # datetime supports up to 6 microsecond digits; pad/trim accordingly.
        frac = (frac + "000000")[:6]
        ts_norm = f"{base}.{frac}"
        return datetime.strptime(ts_norm, "%Y-%m-%d %H:%M:%S.%f")
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")


def _extract_called_from(line: str) -> Optional[str]:
    """
    Extract called_from from a log line.

    Values: public, private, fast-sync, validator_session, etc.
    """
    m = _CALLED_FROM_RE.search(line)
    if not m:
        return None
    return m.group(1)


def _extract_compression(line: str) -> Optional[str]:
    """Extract compression type (compressed, none)."""
    m = _COMPRESSION_RE.search(line)
    if not m:
        return None
    return m.group(1)


def _extract_time_sec(line: str) -> Optional[float]:
    """Extract duration in seconds from time_sec field."""
    m = _TIME_SEC_RE.search(line)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _extract_int(line: str, pattern_re: re.Pattern[str]) -> Optional[int]:
    """Generic helper to extract an integer using a compiled regex."""
    m = pattern_re.search(line)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _parse_line(line: str) -> LogRecord:
    """
    Parse a single log line into a LogRecord.
    
    Raises ValueError if the line is not a valid benchmark record or
    required fields (type, time_sec) are missing.
    """
    if BENCHMARK_MARKER not in line:
        raise ValueError(f"Line does not contain {BENCHMARK_MARKER}")

    ts_str = _extract_timestamp(line)
    if not ts_str:
        raise ValueError("Missing timestamp in line")
    
    node_id = _extract_node_id(line)
    block_id = _extract_block_id(line)
    raw_type = _extract_type(line)
    stage, log_type = _split_type(raw_type)
    
    if not log_type:
        raise ValueError(f"Missing type after {BENCHMARK_MARKER}")
    if not stage:
        raise ValueError(f"Could not determine stage (compress/decompress) from type '{raw_type}'")
    
    called_from = _extract_called_from(line)
    compression = _extract_compression(line)
    if not compression:
        raise ValueError("Missing compression field")
    
    time_sec = _extract_time_sec(line)
    if time_sec is None:
        raise ValueError("Missing time_sec field")
    
    original_size = _extract_int(line, _ORIGINAL_SIZE_RE)
    compressed_size = _extract_int(line, _COMPRESSED_SIZE_RE)

    # end_ts is the timestamp from the log line
    # start_ts = end_ts - time_sec
    end_ts = _timestamp_to_datetime(ts_str)
    start_ts = end_ts - timedelta(seconds=time_sec)

    return LogRecord(
        node_id=node_id,
        start_ts=start_ts,
        end_ts=end_ts,
        block_id=block_id,
        stage=stage,
        type=log_type,
        called_from=called_from,
        compression=compression,
        original_size=original_size,
        compressed_size=compressed_size,
        duration_sec=time_sec,
    )


def read_logs_from_experiment(
    experiment_name: str, base_dir: str = "logs"
) -> List[LogRecord]:
    """
    Read all logs from a given experiment directory and return a flat list
    of parsed LogRecord objects.
    
    Reads from: <base_dir>/<experiment_name>/benchmark.log
    """
    experiment_dir = Path(base_dir) / experiment_name
    benchmark_log = experiment_dir / "benchmark.log"
    
    if not benchmark_log.exists():
        raise FileNotFoundError(f"Benchmark log not found: {benchmark_log}")

    records: List[LogRecord] = []
    
    
    with open(benchmark_log, "r", encoding="utf-8", errors="replace") as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line:
                continue
            
            if BENCHMARK_MARKER not in line:
                continue
            
            try:
                rec = _parse_line(line)
                records.append(rec)
            except ValueError as e:
                raise ValueError(f"Line {line_num}: {e}\n  {line[:150]}") from None

    return records


def group_records_by_block_id(records: List[LogRecord]) -> dict[str, List[LogRecord]]:
    """
    Group records by block_id.

    For each block_id, returns a list of all related records, sorted by timestamp.
    Records without a block_id are skipped.
    """
    grouped: dict[str, List[LogRecord]] = {}

    for rec in records:
        if not rec.block_id:
            continue
        grouped.setdefault(rec.block_id, []).append(rec)

    # Sort each block's records by END timestamp (lexicographically works for the given format)
    for recs in grouped.values():
        recs.sort(key=lambda r: r.end_ts)

    return grouped


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

    # Sort each inner list by END timestamp for convenience.
    for by_block in grouped.values():
        for recs in by_block.values():
            recs.sort(key=lambda r: r.start_ts)

    return grouped


def compute_stats_for_type_called_from(
    by_block: dict[str, List[LogRecord]],
    earliest_ts_global: datetime,
) -> TypeCalledFromStats:
    """
    Compute statistics for a single (type, called_from) combination.

    Returns:
        TypeCalledFromStats with:
          - num_blocks
          - per‑block time series (block size, compression %, broadcast times)
          - per‑record time series for compress/decompress durations
    """
    num_blocks = len(by_block) #+

    # Accumulate with absolute datetimes first, then shift to origin.
    block_size_points_dt: list[tuple[datetime, int]] = []
    compression_percent_points_dt: list[tuple[datetime, float]] = []
    broadcast_time_avg_points_dt: list[tuple[datetime, float]] = []
    broadcast_time_full_points_dt: list[tuple[datetime, float]] = []
    broadcast_time_66p_points_dt: list[tuple[datetime, float]] = []
    compression_time_points_dt: list[tuple[datetime, float]] = []
    decompression_time_points_dt: list[tuple[datetime, float]] = []
    cnt_blocks_with_several_sizes = 0
    cnt_blocks_with_no_compress_records = 0
    cnt_blocks_with_no_decompress_records = 0
    for block_id, recs in by_block.items():
        # 6) Compression time data: all compress records
        for rec in recs:
            if rec.stage == "compress":
                compression_time_points_dt.append((rec.end_ts, rec.duration_sec))
            elif rec.stage == "decompress":
                decompression_time_points_dt.append((rec.end_ts, rec.duration_sec))

        # 1–5 work per block, using any compress records with valid sizes.
        all_original_size = {r.original_size for r in recs if r.original_size is not None and r.original_size > 0}
        all_compressed_size = {r.compressed_size for r in recs if r.compressed_size is not None and r.compressed_size > 0}
        if len(all_original_size) == 0 or len(all_compressed_size) == 0:
            raise ValueError(f"No data or compressed size for block {block_id}")
        if len(all_original_size) != 1 or len(all_compressed_size) != 1:
            cnt_blocks_with_several_sizes += 1
            # print(f"Warning: Block {block_id} has several sizes: {all_original_size}")
            

        original_size = next(iter(all_original_size))
        compressed_size = next(iter(all_compressed_size))

        # Use the earliest compress start as the block's timestamp.
        ts_block = min(r.start_ts for r in recs)

        # 2) Block size data
        block_size_points_dt.append((ts_block, original_size))

        # 3) Compression percent data
        compression_percent = (original_size - compressed_size) / original_size
        compression_percent_points_dt.append((ts_block, compression_percent))

        # 4–5) Broadcast time data: requires both compress and decompress records.
        compress_ts = sorted([r.start_ts for r in recs if r.stage == "compress"])
        decompress_ts = sorted([r.end_ts for r in recs if r.stage == "decompress"])
        if compress_ts and decompress_ts:
            earliest_compress_ts = min(compress_ts)
            latest_decompress_ts = max(decompress_ts)

            # Full broadcast time: first compress START to last decompress END.
            full_broadcast = (latest_decompress_ts - earliest_compress_ts).total_seconds()
            broadcast_time_full_points_dt.append((ts_block, full_broadcast))

            # Average broadcast time: first compress START to average decompress END.
            avg_decomp_seconds = sum(dt.timestamp() for dt in decompress_ts) / len(decompress_ts)
            avg_decomp_dt = datetime.fromtimestamp(avg_decomp_seconds)
            avg_broadcast = (avg_decomp_dt - earliest_compress_ts).total_seconds()
            broadcast_time_avg_points_dt.append((ts_block, avg_broadcast))

            # 66th percentile broadcast time over decompression END times
            decomp_secs = np.array([(dt - earliest_compress_ts).total_seconds() for dt in decompress_ts])
            broadcast_time_66p_points_dt.append((ts_block, np.percentile(decomp_secs, 66)))
        else:
            if len(compress_ts) == 0:
                cnt_blocks_with_no_compress_records += 1
            if len(decompress_ts) == 0:
                cnt_blocks_with_no_decompress_records += 1

    # Shift all time coordinates to floats (seconds since earliest_ts_global).
    def shift_points_to_origin(points: list[tuple[datetime, float | int]]) -> list[tuple[float, float | int]]:
        return sorted([((ts - earliest_ts_global).total_seconds(), value) for ts, value in points])

    if cnt_blocks_with_no_compress_records > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_no_compress_records} blocks have no compress records")
    if cnt_blocks_with_no_decompress_records > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_no_decompress_records} blocks have no decompress records")
    
    if cnt_blocks_with_several_sizes > 0:
        print(f"  {c_warn('WARNING:')} {cnt_blocks_with_several_sizes} blocks have several sizes")
    
    return TypeCalledFromStats(
        num_blocks=num_blocks,
        block_size_points=shift_points_to_origin(block_size_points_dt),
        compression_percent_points=shift_points_to_origin(compression_percent_points_dt),
        broadcast_time_avg_points=shift_points_to_origin(broadcast_time_avg_points_dt),
        broadcast_time_full_points=shift_points_to_origin(broadcast_time_full_points_dt),
        broadcast_time_66p_points=shift_points_to_origin(broadcast_time_66p_points_dt),
        compression_time_points=shift_points_to_origin(compression_time_points_dt),
        decompression_time_points=shift_points_to_origin(decompression_time_points_dt),
    )


def main() -> None:
    """Simple CLI: print distribution of logs per block_id with examples."""
    if len(sys.argv) < 2:
        print("Usage: python process_logs.py <experiment_name>")
        sys.exit(1)

    experiment_name = sys.argv[1]
    print(f"{c_label('Experiment:')} {c_value(experiment_name)}")

    records = read_logs_from_experiment(experiment_name)
    print(f"{c_label('Total records:')} {c_value(str(len(records)))}")

    grouped_to = group_by_type_called_from_and_block(records)
    print(f"{c_label('(type, called_from) combinations:')} {c_value(str(len(grouped_to)))}")

    # Find the earliest timestamp over all data (start_ts and end_ts),
    # so we can shift all time coordinates relative to this origin.
    if not records:
        raise RuntimeError("No records parsed from logs")
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
