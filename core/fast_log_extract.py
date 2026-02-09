#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime
from typing import Optional, Tuple

CHUNK = 1 << 20              # 1 MiB copy chunks
PIPE_CHUNK = 8 << 20         # 8 MiB chunks when piping to grep
PROBE_BACK = 256 * 1024      # bytes to scan backward for a line start
PROBE_FWD = 512 * 1024       # bytes to scan forward for a header in a probe window
MAX_FIRST_SCAN = 8 * 1024 * 1024  # scan up to 8 MiB from start to detect net

# Timestamp bytes at beginning (fixed +00:00, no fractional seconds assumed per spec)
TS_BYTES_RE = rb"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00"


def parse_ts(ts: str) -> datetime:
    # expects '+00:00' (per spec); datetime.fromisoformat handles it
    return datetime.fromisoformat(ts)


def pread(fd: int, size: int, offset: int) -> bytes:
    return os.pread(fd, size, offset)


def fsize(fd: int) -> int:
    return os.fstat(fd).st_size


def find_line_start(fd: int, offset: int) -> int:
    if offset <= 0:
        return 0
    start = max(0, offset - PROBE_BACK)
    buf = pread(fd, offset - start, start)
    idx = buf.rfind(b"\n")
    return 0 if idx < 0 else start + idx + 1


def detect_net_and_header_regex(fd: int) -> re.Pattern[bytes]:
    """
    Determine whether log uses 'devnet' or 'testnet' by finding the first header line.
    Returns a compiled bytes regex that matches only header lines.
    """
    buf = pread(fd, MAX_FIRST_SCAN, 0)
    # Try both nets; require full header structure.
    # hostname: \S+ (no spaces)
    # net: devnet|testnet
    # trailing ':' required
    rx_any = re.compile(
        rb"(?m)^(" + TS_BYTES_RE + rb")\s+(\S+)\s+(devnet|testnet):"
    )
    m = rx_any.search(buf)
    if not m:
        raise RuntimeError("Failed to detect header format in initial scan window.")

    net = m.group(3)  # b"devnet" or b"testnet"
    # Now compile a regex for this net only, capturing timestamp and host.
    rx = re.compile(
        rb"(?m)^(" + TS_BYTES_RE + rb")\s+(\S+)\s+" + re.escape(net) + rb":"
    )
    return rx


def first_header_at_or_after(fd: int, offset: int, header_rx: re.Pattern[bytes]) -> Optional[Tuple[int, bytes]]:
    """
    From an arbitrary offset, return (header_offset, ts_bytes) for the first header
    found at or after the beginning of the line containing offset.
    Searches within a forward probe window. Returns None if not found in window.
    """
    size = fsize(fd)
    line0 = find_line_start(fd, offset)

    # Read forward window from line0
    read_len = min(PROBE_FWD, max(0, size - line0))
    if read_len == 0:
        return None
    buf = pread(fd, read_len, line0)

    m = header_rx.search(buf)
    if not m:
        return None
    hdr_off = line0 + m.start()
    ts_bytes = m.group(1)
    return hdr_off, ts_bytes


def lower_bound_header(fd: int, target: datetime, header_rx: re.Pattern[bytes]) -> int:
    """
    Return file offset of the earliest header whose timestamp >= target.
    Binary search over file offsets with local probing to find next header.
    """
    size = fsize(fd)
    lo = 0
    hi = size
    best = size  # if target after last header, returns EOF

    while lo < hi:
        mid = (lo + hi) // 2
        found = first_header_at_or_after(fd, mid, header_rx)

        if found is None:
            # No header in probe window; move right (increase mid).
            # Ensure progress: skip ahead by PROBE_FWD (or to hi).
            lo = min(size, mid + PROBE_FWD)
            continue

        hdr_off, ts_b = found
        try:
            ts = parse_ts(ts_b.decode("ascii"))
        except Exception:
            # Treat unparseable as "too small"; move right past this header
            lo = max(lo + 1, hdr_off + 1)
            continue

        if ts >= target:
            best = hdr_off
            hi = hdr_off
        else:
            # Move right; next search start after this header
            lo = max(lo + 1, hdr_off + 1)

    return best


def upper_bound_header(fd: int, target: datetime, header_rx: re.Pattern[bytes]) -> int:
    """
    Return file offset of the earliest header whose timestamp > target.
    Binary search over file offsets with local probing to find next header.
    """
    size = fsize(fd)
    lo = 0
    hi = size
    best = size

    while lo < hi:
        mid = (lo + hi) // 2
        found = first_header_at_or_after(fd, mid, header_rx)

        if found is None:
            lo = min(size, mid + PROBE_FWD)
            continue

        hdr_off, ts_b = found
        try:
            ts = parse_ts(ts_b.decode("ascii"))
        except Exception:
            lo = max(lo + 1, hdr_off + 1)
            continue

        if ts > target:
            best = hdr_off
            hi = hdr_off
        else:
            lo = max(lo + 1, hdr_off + 1)

    return best


def opaque_copy_range(fd_in: int, fd_out: int, start: int, end: int) -> None:
    """
    Copy bytes [start, end) using os.pread + os.write in CHUNK chunks.
    """
    if end <= start:
        return
    remaining = end - start
    off = start
    while remaining > 0:
        n = CHUNK if remaining >= CHUNK else remaining
        buf = pread(fd_in, n, off)
        if not buf:
            break
        os.write(fd_out, buf)
        off += len(buf)
        remaining -= len(buf)


def rg_filter_range(
    fd_in: int,
    fd_out: int,
    start: int,
    end: int,
    marker: str,
) -> None:
    """
    Stream [start, end) into rg -F for fast filtering.
    """
    if end <= start:
        return

    os.lseek(fd_in, start, os.SEEK_SET)
    remaining = end - start

    env = os.environ.copy()
    env["LC_ALL"] = "C"

    proc = subprocess.Popen(
        ["rg", "-F", "--color", "never", "--no-line-number", marker],
        stdin=subprocess.PIPE,
        stdout=fd_out,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    assert proc.stdin is not None

    while remaining > 0:
        to_read = PIPE_CHUNK if remaining >= PIPE_CHUNK else remaining
        chunk = os.read(fd_in, to_read)
        if not chunk:
            break
        remaining -= len(chunk)
        proc.stdin.write(chunk)

    proc.stdin.close()
    rc = proc.wait()
    # rg exit codes: 0 = matches, 1 = no matches, 2 = error
    if rc not in (0, 1):
        raise RuntimeError(f"rg failed with exit code {rc}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract a time range from a huge multiline log by binary searching header lines, optionally keep only marker lines."
    )
    ap.add_argument("path", help="Input log file path")
    ap.add_argument("--start", required=True, help="Start timestamp (inclusive), e.g. 2026-01-24T13:31:39+00:00")
    ap.add_argument("--end", required=True, help="End timestamp (inclusive), e.g. 2026-01-24T13:33:39+00:00")
    ap.add_argument("--out", default="-", help="Output file (default: stdout)")
    ap.add_argument("--marker", default=None, help="Line marker to keep (omit to keep all lines)")
    args = ap.parse_args()

    start_dt = parse_ts(args.start)
    end_dt = parse_ts(args.end)
    if end_dt <= start_dt:
        print("error: --end must be greater than --start", file=sys.stderr)
        return 2

    marker_str = args.marker

    fd_in = os.open(args.path, os.O_RDONLY)
    try:
        header_rx = detect_net_and_header_regex(fd_in)

        start_off = lower_bound_header(fd_in, start_dt, header_rx)
        end_off = upper_bound_header(fd_in, end_dt, header_rx)

        # If end_off == EOF but we still want until EOF, keep it.
        # If start_off == EOF, nothing to copy.
        if start_off >= fsize(fd_in):
            return 0
        if end_off > fsize(fd_in):
            end_off = fsize(fd_in)
        if end_off < start_off:
            end_off = start_off

        if args.out == "-":
            fd_out = sys.stdout.fileno()
            if marker_str is None:
                opaque_copy_range(fd_in, fd_out, start_off, end_off)
            else:
                rg_filter_range(fd_in, fd_out, start_off, end_off, marker_str)
        else:
            fd_out = os.open(args.out, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
            try:
                if marker_str is None:
                    opaque_copy_range(fd_in, fd_out, start_off, end_off)
                else:
                    rg_filter_range(fd_in, fd_out, start_off, end_off, marker_str)
            finally:
                os.close(fd_out)

    finally:
        os.close(fd_in)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
