#!/usr/bin/env python3
"""
Fetch compressed benchmark records from the remote HTTP server and save them
as logs/<experiment>/records.json.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "http://devnet-log.toncenter.com:8080/get_benchmark_data"
DEFAULT_TIMEOUT_S = 300
DEFAULT_NETWORK = "devnet"


def die(msg: str) -> None:
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch records.json from devnet-log server.")
    parser.add_argument("experiment", help="Local experiment directory name (logs/<experiment>/records.json)")
    parser.add_argument("--start", help="Start timestamp (passed through to server, no parsing)")
    parser.add_argument("--end", help="End timestamp (passed through to server, no parsing)")
    parser.add_argument("--dashboard", help="Dashboard URL with start/end query params")
    return parser.parse_args()


def _extract_dashboard_query(url: str) -> str:
    url = url.strip().strip('"').strip("'")
    if "?" not in url:
        die("missing '?' in --dashboard URL")
    _, query = url.split("?", 1)
    if not query:
        die("empty query string in --dashboard URL")
    return query


def _extract_query_range(query: str) -> tuple[str, str]:
    qs = parse_qs(query)
    start = (qs.get("start") or [None])[0]
    end = (qs.get("end") or [None])[0]
    if not start or not end:
        die("missing 'start' or 'end' in --dashboard URL query")
    return unquote(start), unquote(end)


def build_url(args: argparse.Namespace) -> tuple[str, str, str, str | None]:
    start = args.start
    end = args.end
    dashboard_url = args.dashboard
    if args.dashboard:
        query = _extract_dashboard_query(args.dashboard)
        start, end = _extract_query_range(query)
    else:
        if not start or not end:
            die("Both --start and --end are required")
        query = urlencode({"start": start, "end": end})
    return f"{DEFAULT_BASE_URL}?{query}", start, end, dashboard_url


def fetch_payload(url: str, timeout_s: int) -> str:
    req = Request(url, headers={"Accept-Encoding": "gzip"})
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            body = resp.read()
            encoding = (resp.headers.get("Content-Encoding") or "").lower()
    except HTTPError as exc:
        detail = ""
        try:
            raw = exc.read()
            if raw:
                text = raw.decode("utf-8", errors="replace").strip()
                try:
                    payload = json.loads(text)
                    detail = payload.get("error") or text
                except json.JSONDecodeError:
                    detail = text
        except Exception:
            detail = ""
        suffix = f": {detail}" if detail else ""
        die(f"Request failed: {exc.code} {exc.reason}{suffix}")
    except URLError as exc:
        die(f"Request failed: {exc.reason}")

    if encoding == "gzip":
        body = gzip.decompress(body)
    return body.decode("utf-8")


def main() -> None:
    args = parse_args()
    url, start, end, dashboard_url = build_url(args)
    base_dir = Path(__file__).resolve().parents[1] / "logs"
    experiment_dir = base_dir / args.experiment
    experiment_dir.mkdir(parents=True, exist_ok=True)
    out_path = experiment_dir / "records.json"

    print(f"Fetching: {url}")
    payload_json = fetch_payload(url, DEFAULT_TIMEOUT_S)
    out_path.write_text(payload_json, encoding="utf-8")
    out_js = experiment_dir / "records.js"
    js = (
        "window.__compressed_records = window.__compressed_records || {};\n"
        f"window.__compressed_records[{json.dumps(args.experiment, ensure_ascii=False)}] = "
        f"{payload_json};\n"
    )
    out_js.write_text(js, encoding="utf-8")
    info_path = experiment_dir / "info.json"
    info = {
        "name": args.experiment,
        "url": dashboard_url,
        "start": start,
        "end": end,
        "network": DEFAULT_NETWORK,
    }
    info_path.write_text(json.dumps(info, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()

