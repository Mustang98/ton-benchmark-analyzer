#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import posixpath
import subprocess
import sys
import tempfile
import time
from cache import DiskCache
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, unquote, urlparse

_ROOT_DIR = Path(__file__).resolve().parents[1]
_CORE_DIR = _ROOT_DIR / "core"
sys.path.insert(0, str(_CORE_DIR))

from parse_logs import BENCHMARK_MARKER, build_compressed_payload_from_log

_CACHE = DiskCache(Path("/var/cache/broadcast-benchmark"), max_entries=100)


def parse_iso_utc(ts: str) -> datetime:
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    if dt.microsecond:
        s = dt.isoformat(timespec="milliseconds")
    else:
        s = dt.isoformat(timespec="seconds")
    return s.replace("+00:00", "Z")


def to_log_prefix(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
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


def day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    start = datetime.combine(d, dt_time(0, 0, 0), tzinfo=timezone.utc)
    end = datetime.combine(d, dt_time(23, 59, 59, 999999), tzinfo=timezone.utc)
    return start, end


def make_handler(
    root_dir: Path,
    log_dir: Path,
    file_prefix: str,
    fast_script: Path,
    static_logs_dir: Path,
):
    class BroadcastHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, directory=str(root_dir), **kwargs)
            self._static_logs_dir = static_logs_dir

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/get_benchmark_data":
                self.handle_broadcasts(parsed)
                return
            super().do_GET()

        def handle_broadcasts(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            start_raw = (qs.get("start") or [None])[0]
            end_raw = (qs.get("end") or [None])[0]
            if not start_raw or not end_raw:
                self.send_json_error(400, "missing 'start' or 'end' query parameter")
                return
            try:
                start_dt = parse_iso_utc(start_raw)
                end_dt = parse_iso_utc(end_raw)
            except Exception as exc:
                self.send_json_error(400, f"invalid timestamp: {exc}")
                return
            if end_dt <= start_dt:
                self.send_json_error(400, "end must be greater than start")
                return

            window = Window(start=start_dt, end=end_dt)
            start_key = to_z(start_dt)
            end_key = to_z(end_dt)
            experiment_name = f"devnet {start_key}..{end_key}"

            cached_payload = _CACHE.get(start_key, end_key)
            if cached_payload is not None:
                print(f"Cache hit for {start_key} .. {end_key}")
                self.send_gzip_json(cached_payload, compresslevel=3)
                return

            try:
                collect_start = time.perf_counter()
                print(f"Collecting logs: {start_key} .. {end_key}")
                with tempfile.TemporaryDirectory(prefix="ton_benchmark_") as tmp_dir:
                    bench_log = Path(tmp_dir) / "benchmark.log"
                    with bench_log.open("ab") as out_fh:
                        for d in window.dates():
                            log_path = log_dir / f"{file_prefix}_{d.isoformat()}.log"
                            if not log_path.exists():
                                continue

                            day_start, day_end = day_bounds_utc(d)
                            s = max(window.start, day_start)
                            e = min(window.end, day_end)
                            if e <= s:
                                continue

                            cmd = [
                                sys.executable,
                                str(fast_script),
                                str(log_path),
                                "--start",
                                to_log_prefix(s),
                                "--end",
                                to_log_prefix(e),
                                "--marker",
                                BENCHMARK_MARKER,
                            ]
                            proc = subprocess.run(
                                cmd,
                                stdout=out_fh,
                                stderr=subprocess.PIPE,
                                check=False,
                            )
                            if proc.returncode != 0:
                                err = proc.stderr.decode("utf-8", errors="replace").strip()
                                self.send_json_error(
                                    500,
                                    f"fast_log_extract failed for {log_path.name}: {err or 'unknown error'}",
                                )
                                return

                    collect_end = time.perf_counter()
                    print(f"Collected logs in {collect_end - collect_start:.2f}s")

                    parse_start = time.perf_counter()
                    print("Parsing logs...")
                    payload = None
                    payload, payload_json, _ = build_compressed_payload_from_log(
                        bench_log,
                        experiment_name,
                        timing=False,
                    )
                parse_end = time.perf_counter()
                total_records = payload.get("total_records") if isinstance(payload, dict) else None
                total_blocks = payload.get("total_blocks") if isinstance(payload, dict) else None
                detail = []
                if total_records is not None:
                    detail.append(f"records={total_records}")
                if total_blocks is not None:
                    detail.append(f"blocks={total_blocks}")
                suffix = f" ({', '.join(detail)})" if detail else ""
                print(f"Parsed logs in {parse_end - parse_start:.2f}s{suffix}")
            except Exception as exc:
                self.send_json_error(500, f"server error: {exc}")
                return

            _CACHE.put(start_key, end_key, payload_json)
            try:
                self.send_gzip_json(payload_json, compresslevel=3)
            except (BrokenPipeError, ConnectionResetError):
                print(f"Client disconnected before response for {start_key} .. {end_key}")
                return

        def send_gzip_json(self, payload_json: str, compresslevel: int = 3) -> None:
            raw = payload_json.encode("utf-8")
            body = gzip.compress(raw, compresslevel=compresslevel)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Uncompressed-Length", str(len(raw)))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json_error(self, status: int, message: str) -> None:
            payload = {"error": message}
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def translate_path(self, path: str) -> str:
            parsed = urlparse(path).path
            if parsed.startswith("/logs/"):
                rel = posixpath.normpath(unquote(parsed[len("/logs/"):]))
                rel = rel.lstrip("/")
                if rel.startswith(".."):
                    return str(self._static_logs_dir)
                return str((self._static_logs_dir / rel).resolve())
            return super().translate_path(path)

        def log_message(self, format: str, *args) -> None:
            super().log_message(format, *args)

    return BroadcastHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve benchmark logs via HTTP.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8081, help="Bind port (default: 8081)")
    parser.add_argument(
        "--root",
        default=str(_ROOT_DIR / "web"),
        help="Static files root directory",
    )
    parser.add_argument(
        "--log-dir",
        default="/var/log/devnet",
        help="Directory with devnet logs (default: /var/log/devnet)",
    )
    parser.add_argument(
        "--file-prefix",
        default="devnet",
        help="Log file prefix (default: devnet)",
    )
    parser.add_argument(
        "--static-logs",
        default=str(_ROOT_DIR / "logs"),
        help="Directory for local logs (default: <repo>/logs)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root_dir = Path(args.root).resolve()
    log_dir = Path(args.log_dir).resolve()
    static_logs_dir = Path(args.static_logs).resolve()
    fast_script = _ROOT_DIR / "core" / "fast_log_extract.py"

    if not fast_script.exists():
        raise SystemExit(f"fast_log_extract.py not found at {fast_script}")

    handler_cls = make_handler(root_dir, log_dir, args.file_prefix, fast_script, static_logs_dir)
    server = ThreadingHTTPServer((args.host, args.port), handler_cls)
    print(f"Serving on http://{args.host}:{args.port} (root={root_dir})")
    server.serve_forever()


if __name__ == "__main__":
    main()
