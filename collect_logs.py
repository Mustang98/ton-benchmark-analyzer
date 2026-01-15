#!/usr/bin/env python3
"""
Collect benchmark lines from centralized devnet logs.

Reads devnet_YYYY-MM-DD.log files from devnet-log.toncenter.com:/var/log/devnet,
filters ONLY lines containing "Broadcast_benchmark" within a [start, end] time
window extracted from the given dashboard URL, and writes:

  logs/<experiment_name>/benchmark.log
  logs/<experiment_name>/info.json

Usage:
  python3 collect_logs.py <experiment_name> <dashboard_url>
"""

from __future__ import annotations

import json
import os
import sys
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse, parse_qs, unquote


LOG_HOST = "devnet-log.toncenter.com"
LOG_USER = "vallas"
REMOTE_DIR = "/var/log/devnet"
NEEDLE = "Broadcast_benchmark"


def _use_color() -> bool:
    # Respect the informal NO_COLOR convention and avoid ANSI when not a TTY.
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


def c_ok(text: str) -> str:
    return _c(text, "1;32")  # bold green


def c_warn(text: str) -> str:
    return _c(text, "1;33")  # bold yellow


def c_err(text: str) -> str:
    return _c(text, "1;31")  # bold red


def die(msg: str) -> None:
    # stderr coloring is best-effort; if stderr isn't a TTY, it will just show plain text.
    prefix = "Error"
    if os.environ.get("NO_COLOR") is None:
        try:
            if sys.stderr.isatty():
                prefix = _c(prefix, "1;31")
        except Exception:
            pass
    print(f"{prefix}: {msg}", file=sys.stderr)
    sys.exit(1)


def usage() -> None:
    print(
        "Usage:\n"
        "  python3 collect_logs.py <experiment_name> <dashboard_url>\n\n"
        "Example:\n"
        '  python3 collect_logs.py current_compr "http://devnet-01.toncenter.com:8000/?start=2026-01-13T21%3A15%3A33Z&end=2026-01-13T22%3A15%3A42Z"\n',
        file=sys.stderr,
    )


def parse_iso_utc(ts: str) -> datetime:
    """
    Parse ISO timestamp with optional 'Z' (UTC) and return an aware UTC datetime.
    """
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError as e:
        raise ValueError(f"invalid ISO timestamp '{ts}': {e}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_z(dt: datetime) -> str:
    """
    Format UTC datetime as ISO with trailing 'Z'.
    Keeps seconds if no fractional part; otherwise milliseconds.
    """
    dt = dt.astimezone(timezone.utc)
    if dt.microsecond:
        s = dt.isoformat(timespec="milliseconds")
    else:
        s = dt.isoformat(timespec="seconds")
    return s.replace("+00:00", "Z")


def to_log_prefix(dt: datetime) -> str:
    """
    Format UTC datetime to match the log's first token prefix:
      2026-01-13T21:37:06+00:00
    """
    dt = dt.astimezone(timezone.utc)
    # Keep seconds precision; logs in example have no fractional part in first token.
    return dt.isoformat(timespec="seconds")


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError("end is earlier than start")

    def dates(self) -> Iterable[date]:
        d = self.start.date()
        last = self.end.date()
        while d <= last:
            yield d
            d += timedelta(days=1)


def parse_dashboard_url(url: str) -> Window:
    url = url.strip().strip('"').strip("'")
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    def one(key: str) -> str:
        if key not in qs or not qs[key]:
            raise ValueError(f"missing '{key}' query parameter in URL")
        return qs[key][0]

    start_raw = unquote(one("start"))
    end_raw = unquote(one("end"))
    start_dt = parse_iso_utc(start_raw)
    end_dt = parse_iso_utc(end_raw)
    return Window(start=start_dt, end=end_dt)


def ensure_local_outputs(experiment: str) -> tuple[Path, Path, Path]:
    exp_dir = Path("logs") / experiment
    exp_dir.mkdir(parents=True, exist_ok=True)
    bench_log = exp_dir / "benchmark.log"
    info_json = exp_dir / "info.json"
    return exp_dir, bench_log, info_json


def write_info_json(info_path: Path, experiment: str, url: str, w: Window) -> None:
    payload = {"name": experiment, "url": url, "start": to_z(w.start), "end": to_z(w.end)}
    info_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def ssh_extract_file(remote_file: str, start_cmp: str, end_cmp: str, out_fh) -> None:
    """
    Stream filtered lines from a remote file to out_fh.

    Filtering logic (fast path):
      - assumes remote file is ordered by timestamp
      - skips until >= start
      - exits as soon as > end
      - only prints lines containing NEEDLE
    """
    remote_script = r"""
set -euo pipefail
file="$1"
start="$2"
end="$3"

if [ ! -f "$file" ]; then
  exit 0
fi

# First narrow down to benchmark lines with grep (fast C implementation),
# then apply time-window filtering on that much smaller subset via awk.

tmp="$(mktemp /tmp/bench_grep_XXXXXX)"
cleanup() { rm -f "$tmp" >/dev/null 2>&1 || true; }
trap cleanup EXIT

set +e
LC_ALL=C grep -F "%s" "$file" >"$tmp"
rc_grep=$?
set -e

# grep exit codes: 0 = matches, 1 = no matches, 2 = error
if [ "$rc_grep" -eq 1 ]; then
  exit 0
fi
if [ "$rc_grep" -ne 0 ]; then
  exit "$rc_grep"
fi

LC_ALL=C awk -v s="$start" -v e="$end" '
  $1 < s { next }
  $1 > e { exit }
  { print }
' "$tmp"
""" % (NEEDLE.replace('"', '\\"'))

    proc = subprocess.run(
        ["ssh", f"{LOG_USER}@{LOG_HOST}", "bash", "-s", "--", remote_file, start_cmp, end_cmp],
        input=remote_script.encode("utf-8"),
        stdout=out_fh,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"ssh/awk failed for {remote_file}: {err or 'unknown error'}")


def day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    start = datetime.combine(d, time(0, 0, 0), tzinfo=timezone.utc)
    # Use end-of-day with microseconds to make inclusive comparisons safe.
    end = datetime.combine(d, time(23, 59, 59, 999999), tzinfo=timezone.utc)
    return start, end


def main() -> None:
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)

    experiment = sys.argv[1]
    dashboard_url = sys.argv[2]

    try:
        w = parse_dashboard_url(dashboard_url)
    except Exception as e:
        die(str(e))

    _, bench_log, info_json = ensure_local_outputs(experiment)
    write_info_json(info_json, experiment, dashboard_url, w)

    print(f"{c_label('Experiment')}: {experiment}")
    print(f"{c_label('Time window')}: {to_z(w.start)} .. {to_z(w.end)}")
    
    # Overwrite benchmark.log if exists.
    bench_log.unlink(missing_ok=True)

    total_lines = 0
    with bench_log.open("ab") as out_fh:
        for d in w.dates():
            remote_file = f"{REMOTE_DIR}/devnet_{d.isoformat()}.log"

            day_start, day_end = day_bounds_utc(d)
            s = max(w.start, day_start)
            e = min(w.end, day_end)

            # If the window doesn't overlap this day (shouldn't happen), skip.
            if e < s:
                continue

            s_cmp = to_log_prefix(s)
            e_cmp = to_log_prefix(e)

            print(f"  {c_label('•')} {remote_file} {c_warn(f'({s_cmp} .. {e_cmp})')}")
            try:
                ssh_extract_file(remote_file, s_cmp, e_cmp, out_fh)
            except Exception as ex:
                die(str(ex))

    # Count lines cheaply.
    try:
        total_lines = sum(1 for _ in bench_log.open("rb"))
    except Exception:
        total_lines = -1

    print(f"{c_ok('Done')}. {c_label('Lines')}: {total_lines}")


if __name__ == "__main__":
    main()

