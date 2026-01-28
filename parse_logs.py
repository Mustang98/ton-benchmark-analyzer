"""
Parse benchmark logs from experiment directories.

Reads benchmark.log files and extracts structured data to records.json and records.js.

Usage:
    python parse_logs.py <experiment_name> [--timing]    # Parse single experiment
    python parse_logs.py [--timing]                      # Parse all experiments (generate missing records.json)

Output:
    logs/<experiment_name>/records.json
    logs/<experiment_name>/records.js
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import multiprocessing as mp
import os
import re
import sys
import time

from log_types import LogRecord, extract_short_block_id, c_label, c_value, c_ok, c_warn


BENCHMARK_MARKER = "Broadcast_benchmark"
_EPOCH = datetime(1970, 1, 1)


# ---------------------------------------------------------------------------
# Regex patterns for parsing log lines
# ---------------------------------------------------------------------------

# Match the timestamp in the 3rd bracketed group.
# The first two groups can contain spaces or other characters (e.g. "[ 3][t10]").
_TS_RE = re.compile(r"\[[^\]]+\]\[[^\]]+\]\[([^\]]+)\]")
_BLOCK_ID_RE = re.compile(r"block_id=([^ ]+)")
_CALLED_FROM_RE = re.compile(r"called_from=([^\s]+)")
_TIME_SEC_RE = re.compile(r"time_sec=([^\s]+)")
_COMPRESSION_RE = re.compile(r"compression=([^\s]+)")
_ORIGINAL_SIZE_RE = re.compile(r"original_size=(\d+)")
_COMPRESSED_SIZE_RE = re.compile(r"compressed_size=(\d+)")
# Match node ID from various formats: devnet-05, ton-tval-12, etc.
_NODE_RE = re.compile(r"(devnet-\d+|ton-tval-\d+)")


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _extract_type(line: str, marker_idx: Optional[int] = None) -> Optional[str]:
    """Return the raw token immediately after the BENCHMARK_MARKER, or None."""
    if marker_idx is None:
        marker_idx = line.find(BENCHMARK_MARKER)
    if marker_idx == -1:
        return None
    start = marker_idx + len(BENCHMARK_MARKER)
    # Skip spaces quickly (avoid full .strip()).
    line_len = len(line)
    while start < line_len and line[start] == " ":
        start += 1
    if start >= line_len:
        return None
    end = line.find(" ", start)
    if end == -1:
        end = line_len - 1 if line_len and line[-1] == "\n" else line_len
    return line[start:end] or None


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
    if logical_type == "candidate_data":
        logical_type = "candidate"

    return stage, logical_type


def _extract_timestamp(line: str) -> str:
    """
    Extract the timestamp from the third bracketed group:
        [ 4][t20][2026-01-13 21:16:11.352179434][...]
    """
    # Fast path: walk bracket groups without regex.
    pos = 0
    for idx in range(3):
        start = line.find("[", pos)
        if start == -1:
            break
        end = line.find("]", start + 1)
        if end == -1:
            break
        if idx == 2:
            return line[start + 1:end]
        pos = end + 1
    # Fallback to regex for unexpected formats.
    m = _TS_RE.search(line)
    return m.group(1) if m else ""


def _extract_block_id(line: str) -> str:
    """Extract the block_id=... token (without trailing spaces)."""
    m = _BLOCK_ID_RE.search(line)
    return m.group(1) if m else ""


def _extract_node_id(line: str) -> str:
    """
    Extract node ID from the line prefix.

    Supports formats:
        - 'devnet-05' -> "devnet-05"
        - 'ton-tval-12' -> "ton-tval-12"

    Returns empty string if not found.
    """
    idx = line.find("devnet-")
    if idx != -1:
        start = idx
        idx += len("devnet-")
        end = idx
        line_len = len(line)
        while end < line_len and line[end].isdigit():
            end += 1
        if end > idx:
            return line[start:end]
    idx = line.find("ton-tval-")
    if idx != -1:
        start = idx
        idx += len("ton-tval-")
        end = idx
        line_len = len(line)
        while end < line_len and line[end].isdigit():
            end += 1
        if end > idx:
            return line[start:end]
    # Fallback to regex for unexpected formats.
    m = _NODE_RE.search(line)
    if m:
        return m.group(1)  # Return the full node identifier
    return ""


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


def _timestamp_to_epoch_us(ts: str) -> int:
    """
    Convert timestamp string to microseconds since epoch (naive).
    Uses fromisoformat after trimming fractional digits to 6.
    """
    if "." in ts:
        base, frac = ts.split(".", 1)
        frac = (frac + "000000")[:6]
        ts_norm = f"{base}.{frac}"
    else:
        ts_norm = ts
    dt = datetime.fromisoformat(ts_norm)
    return int((dt - _EPOCH).total_seconds() * 1_000_000)


def _parse_line_fields(
    line: str,
    marker_idx: Optional[int] = None,
) -> tuple[str, str, str, str, str, Optional[str], str, float, Optional[int], Optional[int]]:
    """
    Parse a line and return (ts_str, node_id, block_id, stage, log_type, called_from, compression, time_sec, original_size, compressed_size).
    Raises ValueError when mandatory fields are missing.
    """
    if marker_idx is None:
        marker_idx = line.find(BENCHMARK_MARKER)
    if marker_idx == -1:
        raise ValueError(f"Line does not contain {BENCHMARK_MARKER}")

    ts_str = _extract_timestamp(line)
    if not ts_str:
        raise ValueError("Missing timestamp in line")

    node_id = _extract_node_id(line)
    full_block_id: Optional[str] = None
    block_id: Optional[str] = None
    called_from: Optional[str] = None
    compression: Optional[str] = None
    time_sec: Optional[float] = None
    original_size: Optional[int] = None
    compressed_size: Optional[int] = None

    # Linear scan of tokens after the marker:
    # <type> block_id=... called_from=... compression=... time_sec=... original_size=... compressed_size=...
    pos = marker_idx + len(BENCHMARK_MARKER)
    line_len = len(line)
    while pos < line_len and line[pos] == " ":
        pos += 1
    if pos >= line_len:
        raise ValueError("Missing type after marker")
    end = line.find(" ", pos)
    if end == -1:
        end = line_len
        if line_len and line[-1] == "\n":
            end -= 1
    raw_type = line[pos:end] or None
    stage, log_type = _split_type(raw_type)
    if not log_type:
        raise ValueError(f"Missing type after {BENCHMARK_MARKER}")
    if not stage:
        raise ValueError(f"Could not determine stage (compress/decompress) from type '{raw_type}'")

    pos = end + 1
    while pos < line_len:
        while pos < line_len and line[pos] == " ":
            pos += 1
        if pos >= line_len:
            break
        end = line.find(" ", pos)
        if end == -1:
            end = line_len
            if line_len and line[-1] == "\n":
                end -= 1

        eq = line.find("=", pos, end)
        if eq != -1:
            key = line[pos:eq]
            val = line[eq + 1:end]
            if key == "block_id":
                full_block_id = val
                block_id = extract_short_block_id(full_block_id)
            elif key == "called_from":
                called_from = val
            elif key == "compression":
                compression = _normalize_compression(val)
            elif key == "time_sec":
                try:
                    time_sec = float(val)
                except ValueError:
                    time_sec = None
            elif key == "original_size":
                if val.isdigit():
                    original_size = int(val)
            elif key == "compressed_size":
                if val.isdigit():
                    compressed_size = int(val)

        pos = end + 1

    if not block_id:
        raise ValueError("Missing block_id")
    if not called_from and log_type == "candidate":
        called_from = "validator_session"
    if not compression:
        raise ValueError("Missing compression field")
    if time_sec is None:
        raise ValueError("Missing time_sec field")

    return ts_str, node_id, block_id, stage, log_type, called_from, compression, time_sec, original_size, compressed_size


def _extract_called_from(line: str) -> Optional[str]:
    """
    Extract called_from from a log line.

    Values: public, private, fast-sync, validator_session, etc.
    """
    m = _CALLED_FROM_RE.search(line)
    if not m:
        return None
    return m.group(1)


def _normalize_compression(compression: str) -> str:
    if compression.startswith("compressedV2") and len(compression) > len("compressedV2"):
        next_char = compression[len("compressedV2")]
        if next_char != "_":
            return f"compressedV2_{compression[len('compressedV2'):]}"
    return compression


def _extract_compression(line: str) -> Optional[str]:
    """Extract compression type (compressed, none)."""
    m = _COMPRESSION_RE.search(line)
    if not m:
        return None
    return _normalize_compression(m.group(1))


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


# ---------------------------------------------------------------------------
# Line parsing
# ---------------------------------------------------------------------------

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
    full_block_id = _extract_block_id(line)
    block_id = extract_short_block_id(full_block_id)
    raw_type = _extract_type(line)
    stage, log_type = _split_type(raw_type)
    
    if not log_type:
        raise ValueError(f"Missing type after {BENCHMARK_MARKER}")
    if not stage:
        raise ValueError(f"Could not determine stage (compress/decompress) from type '{raw_type}'")
    
    called_from = _extract_called_from(line)
    if log_type == "candidate" and not called_from:
        called_from = "validator_session"
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
        full_block_id=full_block_id,
        stage=stage,
        type=log_type,
        called_from=called_from,
        compression=compression,
        original_size=original_size,
        compressed_size=compressed_size,
        duration_sec=time_sec,
    )


# ---------------------------------------------------------------------------
# Main parsing function
# ---------------------------------------------------------------------------

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


@dataclass
class ValueMap:
    values: List[Any]
    index_by_value: Dict[Any, int]

    def get_index(self, value: Any) -> int:
        if value in self.index_by_value:
            return self.index_by_value[value]
        idx = len(self.values)
        self.values.append(value)
        self.index_by_value[value] = idx
        return idx


@dataclass
class BlockBucket:
    block_id: str
    size_map: List[Tuple[Optional[int], Optional[int]]]
    size_index: Dict[Tuple[Optional[int], Optional[int]], int]
    records: List[List[int]]

    def get_size_index(self, original_size: Optional[int], compressed_size: Optional[int]) -> int:
        key = (original_size, compressed_size)
        if key in self.size_index:
            return self.size_index[key]
        idx = len(self.size_map)
        self.size_map.append(key)
        self.size_index[key] = idx
        return idx


# ---------------------------------------------------------------------------
# Parallel parsing helpers
# ---------------------------------------------------------------------------

_PARALLEL_MIN_LINES = 20000


def _get_worker_count() -> int:
    return 16
    raw = os.environ.get("PARSE_WORKERS")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return max(1, os.cpu_count() or 1)


def _chunk_size(total: int, workers: int) -> int:
    if total <= 0:
        return 1
    target = max(1000, total // (workers * 8))
    return min(50000, target)


def _iter_chunks(items: List[Tuple[int, str, int]], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _parse_lines_chunk(
    chunk: List[Tuple[int, str, int]],
) -> List[Tuple[str, str, str, str, Optional[str], str, Optional[int], Optional[int], int, int]]:
    parsed: List[Tuple[str, str, str, str, Optional[str], str, Optional[int], Optional[int], int, int]] = []
    for line_num, line, marker_idx in chunk:
        try:
            (
                ts_str,
                node_id,
                block_id,
                stage,
                log_type,
                called_from,
                compression,
                time_sec,
                original_size,
                compressed_size,
            ) = _parse_line_fields(line, marker_idx=marker_idx)
        except ValueError as e:
            raise ValueError(f"Line {line_num}: {e}\n  {line[:150]}") from None

        end_us = _timestamp_to_epoch_us(ts_str)
        duration_us = int(round(time_sec * 1_000_000))
        start_us = end_us - duration_us
        parsed.append(
            (
                node_id,
                block_id,
                stage,
                log_type,
                called_from,
                compression,
                original_size,
                compressed_size,
                start_us,
                duration_us,
            )
        )
    return parsed


def parse_and_write_compressed(
    experiment_name: str,
    base_dir: str = "logs",
    timing: bool = False,
) -> tuple[Path, Path]:
    """
    Parse benchmark.log and write compressed records.json and records.js.
    """
    experiment_dir = Path(base_dir) / experiment_name
    benchmark_log = experiment_dir / "benchmark.log"
    out_path = experiment_dir / "records.json"

    if not benchmark_log.exists():
        raise FileNotFoundError(f"Benchmark log not found: {benchmark_log}")

    timing_stats = {
        "total_s": 0.0,
        "loop_s": 0.0,
        "parse_s": 0.0,
        "ts_s": 0.0,
        "map_s": 0.0,
        "adjust_s": 0.0,
        "write_s": 0.0,
        "lines_total": 0,
        "lines_matched": 0,
        "records_total": 0,
    }
    t_total_start = time.perf_counter()

    node_map = ValueMap(values=[], index_by_value={})
    stage_map = ValueMap(values=[], index_by_value={})
    type_map = ValueMap(values=[], index_by_value={})
    called_from_map = ValueMap(values=[], index_by_value={})
    compression_map = ValueMap(values=[], index_by_value={})

    blocks: Dict[str, BlockBucket] = {}
    total_records = 0
    ts0_us: Optional[int] = None

    t_read_start = time.perf_counter()
    with open(benchmark_log, "r", encoding="utf-8", errors="replace") as f:
        all_lines = [line.rstrip("\n") for line in f]
    if timing:
        timing_stats["read_s"] = time.perf_counter() - t_read_start

    t_loop_start = time.perf_counter()
    t_find_start = time.perf_counter()
    matched_lines: List[Tuple[int, str, int]] = []
    for line_num, line in enumerate(all_lines, 1):
        timing_stats["lines_total"] += 1
        marker_idx = line.find(BENCHMARK_MARKER)
        if marker_idx == -1:
            continue
        matched_lines.append((line_num, line, marker_idx))
    if timing:
        timing_stats["find_s"] = time.perf_counter() - t_find_start
    timing_stats["lines_matched"] = len(matched_lines)

    t_parse_start = time.perf_counter() if timing else None
    parsed_records: List[Tuple[str, str, str, str, Optional[str], str, Optional[int], Optional[int], int, int]] = []
    used_parallel = False
    if matched_lines:
        workers = _get_worker_count()
        print(f"Using workers: {workers}")
        if workers > 1 and len(matched_lines) >= _PARALLEL_MIN_LINES:
            used_parallel = True
            chunk_size = _chunk_size(len(matched_lines), workers)
            with mp.Pool(processes=workers) as pool:
                for chunk in pool.imap(_parse_lines_chunk, _iter_chunks(matched_lines, chunk_size), chunksize=1):
                    parsed_records.extend(chunk)
        else:
            parsed_records = _parse_lines_chunk(matched_lines)
    if timing:
        timing_stats["parse_s"] = time.perf_counter() - t_parse_start

    t_map_start = time.perf_counter() if timing else None
    for (
        node_id,
        block_id,
        stage,
        log_type,
        called_from,
        compression,
        original_size,
        compressed_size,
        start_us,
        duration_us,
    ) in parsed_records:
        if ts0_us is None or start_us < ts0_us:
            ts0_us = start_us

        block = blocks.get(block_id)
        if block is None:
            block = BlockBucket(block_id=block_id, size_map=[], size_index={}, records=[])
            blocks[block_id] = block

        node_idx = node_map.get_index(node_id)
        stage_idx = stage_map.get_index(stage)
        type_idx = type_map.get_index(log_type)
        called_from_idx = called_from_map.get_index(called_from)
        compression_idx = compression_map.get_index(compression)
        size_idx = block.get_size_index(original_size, compressed_size)

        block.records.append(
            [
                node_idx,
                start_us,
                duration_us,
                stage_idx,
                type_idx,
                called_from_idx,
                compression_idx,
                size_idx,
            ]
        )
        total_records += 1
    
    if timing:
        timing_stats["map_s"] = time.perf_counter() - t_map_start
    timing_stats["loop_s"] = time.perf_counter() - t_loop_start

    t_adjust_start = time.perf_counter() if timing else None
    if ts0_us is None:
        ts0_us = 0
    for block in blocks.values():
        for rec in block.records:
            rec[1] -= ts0_us
    if timing:
        timing_stats["adjust_s"] = time.perf_counter() - t_adjust_start

    blocks_payload: List[list] = []
    for block_id in sorted(blocks.keys()):
        block = blocks[block_id]
        size_map_serialized = [[orig, comp] for orig, comp in block.size_map]
        blocks_payload.append([block.block_id, size_map_serialized, block.records])

    payload = {
        "version": 1,
        "experiment_name": experiment_name,
        "source_records": str(benchmark_log),
        "compressed_at": datetime.now().isoformat(),
        "ts0": (_EPOCH + timedelta(microseconds=ts0_us)).isoformat() if ts0_us else None,
        "units": {
            "start_us": "microseconds since ts0",
            "duration_us": "microseconds",
        },
        "record_fields": [
            "node_idx",
            "start_us",
            "duration_us",
            "stage_idx",
            "type_idx",
            "called_from_idx",
            "compression_idx",
            "size_idx",
        ],
        "size_fields": ["original_size", "compressed_size"],
        "block_fields": ["block_id", "size_map", "records"],
        "maps": {
            "node_id": node_map.values,
            "stage": stage_map.values,
            "type": type_map.values,
            "called_from": called_from_map.values,
            "compression": compression_map.values,
        },
        "total_records": total_records,
        "total_blocks": len(blocks_payload),
        "blocks": blocks_payload,
    }

    t_write_start = time.perf_counter() if timing else None
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    out_path.write_text(payload_json, encoding="utf-8")

    out_js = experiment_dir / "records.js"
    js = (
        "window.__compressed_records = window.__compressed_records || {};\n"
        f"window.__compressed_records[{json.dumps(experiment_name, ensure_ascii=False)}] = "
        f"{payload_json};\n"
    )
    out_js.write_text(js, encoding="utf-8")
    if timing:
        timing_stats["write_s"] = time.perf_counter() - t_write_start

    if timing:
        timing_stats["records_total"] = total_records
        timing_stats["total_s"] = time.perf_counter() - t_total_start
        io_s = timing_stats["loop_s"] - timing_stats["parse_s"] - timing_stats["ts_s"] - timing_stats["map_s"]
        print(f"{c_label('Timing')} total={timing_stats['total_s']:.3f}s \n"
              f"loop={timing_stats['loop_s']:.3f}s io~{io_s:.3f}s \n"
              f"parse={timing_stats['parse_s']:.3f}s ts={timing_stats['ts_s']:.3f}s \n"
              f"map={timing_stats['map_s']:.3f}s adjust={timing_stats['adjust_s']:.3f}s \n"
              f"write={timing_stats['write_s']:.3f}s\n"
              f"read={timing_stats['read_s']:.3f}s\n"
              f"find={timing_stats['find_s']:.3f}s")
        print(f"{c_label('Counts')} lines={timing_stats['lines_total']} \n"
              f"matched={timing_stats['lines_matched']} records={timing_stats['records_total']}")
        if used_parallel:
            print(f"{c_label('Note')} parse includes timestamp conversion (parallel)")

    return out_path, out_js


def parse_single_experiment(experiment_name: str, timing: bool = False) -> None:
    """Parse a single experiment and write records.json and records.js."""
    print(f"{c_label('Experiment:')} {c_value(experiment_name)}")

    out_json, out_js = parse_and_write_compressed(experiment_name, timing=timing)
    print(f"{c_ok('Done.')} Records written to {c_value(str(out_json))}")
    print(f"{c_ok('Done.')} JS payload written to {c_value(str(out_js))}")


def parse_all_experiments(base_dir: str = "logs", timing: bool = False) -> None:
    """Parse all experiments that don't have records.json yet."""
    experiments: List[str] = []
    logs_path = Path(base_dir)

    if not logs_path.exists():
        print(f"{c_warn('Logs directory not found:')} {c_value(str(logs_path))}")
        return

    # Discover experiments directly inside logs/
    for item in logs_path.iterdir():
        if not item.is_dir() or item.name.startswith('.'):
            continue

        if ((item / "benchmark.log").exists() and
            (item / "info.json").exists() and
            not (item / "records.json").exists()):
            rel_path = item.relative_to(logs_path)
            experiments.append(str(rel_path))

    experiments = sorted(experiments)
    print(f"{c_label('Found experiments needing parsing:')} {c_value(str(len(experiments)))}")

    parsed_count = 0
    for experiment in experiments:
        try:
            parse_single_experiment(experiment, timing=timing)
            parsed_count += 1
        except Exception as e:
            print(f"{c_warn('✗ Failed:')} {str(e)}")

    print(f"\n{c_ok('Summary:')} Parsed {c_value(str(parsed_count))} experiments")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Parse benchmark logs and write records.json and records.js."""
    args = sys.argv[1:]
    timing = False
    if "--timing" in args:
        timing = True
        args = [arg for arg in args if arg != "--timing"]

    if len(args) < 1:
        # No experiment specified - parse all experiments
        parse_all_experiments(timing=timing)
    else:
        # Parse single experiment
        experiment_name = args[0]
        parse_single_experiment(experiment_name, timing=timing)


if __name__ == "__main__":
    main()
