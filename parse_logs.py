"""
Parse benchmark logs from experiment directories.

Reads benchmark.log files and extracts structured data to records.json.

Usage:
    python parse_logs.py <experiment_name>    # Parse single experiment
    python parse_logs.py                      # Parse all experiments (generate missing records.json)

Output:
    logs/<experiment_name>/records.json
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import json
import re
import sys

from log_types import LogRecord, record_to_dict, extract_short_block_id, c_label, c_value, c_ok


BENCHMARK_MARKER = "Broadcast_benchmark"


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
    # if logical_type == "candidate_data":
    #     logical_type = "candidate"

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


def _extract_node_id(line: str) -> str:
    """
    Extract node ID from the line prefix.

    Supports formats:
        - 'devnet-05' -> "devnet-05"
        - 'ton-tval-12' -> "ton-tval-12"

    Returns empty string if not found.
    """
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
    compression = m.group(1)
    if compression.startswith("compressedV2") and len(compression) > len("compressedV2"):
        next_char = compression[len("compressedV2")]
        if next_char != "_":
            compression = f"compressedV2_{compression[len('compressedV2'):]}"
    return compression


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
    # if log_type == "candidate" and not called_from:
        # called_from = "validator_session"
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


def write_records_json(
    experiment_name: str,
    records: List[LogRecord],
    base_dir: str = "logs",
) -> Path:
    """
    Write parsed records to a JSON file in the experiment directory.
    
    Args:
        experiment_name: Name of the experiment
        records: List of parsed LogRecord objects
        base_dir: Base directory for logs
    
    Returns:
        Path to the written JSON file
    """
    experiment_dir = Path(base_dir) / experiment_name
    out_path = experiment_dir / "records.json"
    
    payload = {
        "experiment_name": experiment_name,
        "parsed_at": datetime.now().isoformat(),
        "total_records": len(records),
        "records": [record_to_dict(rec) for rec in records],
    }
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    
    return out_path


def parse_single_experiment(experiment_name: str) -> None:
    """Parse a single experiment and write records.json."""
    print(f"{c_label('Experiment:')} {c_value(experiment_name)}")

    records = read_logs_from_experiment(experiment_name)
    print(f"{c_label('Total records parsed:')} {c_value(str(len(records)))}")

    out_path = write_records_json(experiment_name, records)
    print(f"{c_ok('Done.')} Records written to {c_value(str(out_path))}")


def parse_all_experiments(base_dir: str = "logs") -> None:
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
            parse_single_experiment(experiment)
            parsed_count += 1
        except Exception as e:
            print(f"{c_warn('✗ Failed:')} {str(e)}")

    print(f"\n{c_ok('Summary:')} Parsed {c_value(str(parsed_count))} experiments")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Parse benchmark logs and write records.json."""
    if len(sys.argv) < 2:
        # No experiment specified - parse all experiments
        parse_all_experiments()
    else:
        # Parse single experiment
        experiment_name = sys.argv[1]
        parse_single_experiment(experiment_name)


if __name__ == "__main__":
    main()
