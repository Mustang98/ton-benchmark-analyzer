"""
Shared types and utilities for log processing scripts.

This module contains:
- LogRecord dataclass for representing parsed log entries
- ANSI color helpers for terminal output
- Common utility functions for filtering and grouping records
- JSON serialization helpers
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional
import os
import sys


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


# ---------------------------------------------------------------------------
# LogRecord dataclass
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# JSON serialization helpers
# ---------------------------------------------------------------------------

def record_to_dict(rec: LogRecord) -> dict:
    """Convert a LogRecord to a JSON-serializable dictionary."""
    d = asdict(rec)
    d["start_ts"] = rec.start_ts.isoformat()
    d["end_ts"] = rec.end_ts.isoformat()
    return d


def dict_to_record(d: dict) -> LogRecord:
    """Convert a dictionary (from JSON) back to a LogRecord."""
    return LogRecord(
        node_id=d["node_id"],
        start_ts=datetime.fromisoformat(d["start_ts"]),
        end_ts=datetime.fromisoformat(d["end_ts"]),
        block_id=d["block_id"],
        stage=d["stage"],
        type=d["type"],
        called_from=d.get("called_from"),
        compression=d["compression"],
        original_size=d.get("original_size"),
        compressed_size=d.get("compressed_size"),
        duration_sec=d["duration_sec"],
    )


# ---------------------------------------------------------------------------
# Size argument parsing
# ---------------------------------------------------------------------------

def parse_size_arg(size_str: str) -> int:
    """Parse a size argument like '100K', '1M', or '100000' into bytes."""
    size_str = size_str.upper()
    if size_str.endswith("K"):
        return int(size_str[:-1]) * 1000
    elif size_str.endswith("M"):
        return int(size_str[:-1]) * 1000000
    else:
        return int(size_str)


def size_to_k_suffix(size_bytes: int) -> str:
    """Convert size in bytes to a K suffix string (e.g., 60228 -> '60K')."""
    return f"{round(size_bytes / 1000)}K"


# ---------------------------------------------------------------------------
# Record grouping and filtering utilities
# ---------------------------------------------------------------------------

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

    # Sort each block's records by END timestamp
    for recs in grouped.values():
        recs.sort(key=lambda r: r.end_ts)

    return grouped


def get_block_size(records: List[LogRecord]) -> int:
    """Get the original size of a block from its records. Returns 0 if not found."""
    for rec in records:
        if rec.original_size and rec.original_size > 0:
            return rec.original_size
    return 0


def has_validator_session(records: List[LogRecord]) -> bool:
    """Check if any record in the list has called_from=validator_session."""
    return any(rec.called_from == "validator_session" for rec in records)


def filter_records_by_block_size(
    records: List[LogRecord],
    min_block_size: int = 0,
    max_block_size: int = 0,
) -> List[LogRecord]:
    """
    Filter records to only include those from blocks within the size range.
    
    Args:
        records: All log records
        min_block_size: Minimum block size in bytes (0 = no minimum)
        max_block_size: Maximum block size in bytes (0 = no maximum)
    
    Returns:
        Filtered list of records
    """
    if min_block_size == 0 and max_block_size == 0:
        return records
    
    # Group by block_id to determine block sizes
    grouped = group_records_by_block_id(records)
    
    # Find block_ids that match the size criteria
    matching_block_ids: set[str] = set()
    for block_id, block_records in grouped.items():
        size = get_block_size(block_records)
        if size == 0:
            continue  # Skip blocks with unknown size
        if min_block_size > 0 and size < min_block_size:
            continue
        if max_block_size > 0 and size > max_block_size:
            continue
        matching_block_ids.add(block_id)
    
    # Filter records to only include matching blocks
    return [rec for rec in records if rec.block_id in matching_block_ids]
