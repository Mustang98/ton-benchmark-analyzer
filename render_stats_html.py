#!/usr/bin/env python3
"""
Render an interactive HTML report from a stats JSON file produced by process_logs.py.

Usage:
  # Preferred: pass experiment names (JSON will be read from stats/<name>.json)
  python3 render_stats_html.py <experiment_name> [more_experiment_names ...]

Outputs:
  For each input: renders/<experiment>.html
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional


def mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def safe_key(type_val: Any, called_from_val: Any) -> str:
    t = "None" if type_val is None else str(type_val)
    cf = "None" if called_from_val is None else str(called_from_val)
    key = f"{t}__{cf}"
    # sanitize for HTML id
    safe = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in key)
    return safe


def to_js(o: Any) -> str:
    # Use separators to reduce size a bit; ensure ASCII to avoid surprises
    return json.dumps(o, ensure_ascii=False, separators=(",", ":"))


def compute_averages_for_pair(pair_stats: Dict[str, Any]) -> Dict[str, float]:
    def avg_from_points(points_key: str) -> float:
        pts = pair_stats.get(points_key, [])
        if not isinstance(pts, list):
            return 0.0
        vals = [float(y) for _, y in pts if isinstance(_, (int, float))]
        return mean(vals)

    return {
        "num_blocks": float(pair_stats.get("num_blocks", 0)),
        "avg_block_size_bytes": avg_from_points("block_size_points"),
        "avg_compression_percent": avg_from_points("compression_percent_points"),
        "avg_broadcast_time_avg_s": avg_from_points("broadcast_time_avg_points"),
        "avg_broadcast_time_full_s": avg_from_points("broadcast_time_full_points"),
        "avg_broadcast_time_66p_s": avg_from_points("broadcast_time_66p_points"),
        "avg_compression_time_s": avg_from_points("compression_time_points"),
        "avg_decompression_time_s": avg_from_points("decompression_time_points"),
    }


def build_html(title: str, experiments: List[Dict[str, Any]]) -> str:
    # experiments: [{"name": str, "stats": dict}]
    # Build union of (type, called_from) pairs across experiments
    union_map: Dict[str, Dict[str, Any]] = {}
    exp_pairs: List[Tuple[str, Dict[str, Any]]] = []
    for exp in experiments:
        tos: Dict[str, Dict[str, Any]] = exp["stats"].get("type_called_from_stats", {})
        for k, pair in tos.items():
            t = pair.get("type")
            cf = pair.get("called_from")
            label = f"{t}" if cf in (None, "None") else f"{t} ({cf})"
            union_map[k] = {"type": t, "called_from": cf, "label": label}
    # Sorted union keys by (type, called_from)
    union_keys: List[str] = sorted(
        union_map.keys(), key=lambda k: (str(union_map[k]["type"] or ""), str(union_map[k]["called_from"] or ""))
    )

    # Short experiment names (first token of filename stem)
    exp_short_names: List[str] = [
        (Path(exp["path"]).stem.split("_")[0] if "path" in exp else str(exp["name"]).split("_")[0])
        for exp in experiments
    ]

    # Rows spec for averages table
    rows_spec = [
        ("num_blocks", "Blocks"),
        ("avg_block_size_bytes", "Avg block size (KB)"),
        ("avg_compression_percent", "Avg compression percent"),
        ("avg_broadcast_time_avg_s", "Avg broadcast time (avg)"),
        ("avg_broadcast_time_66p_s", "Avg broadcast time (66p)"),
        ("avg_broadcast_time_full_s", "Avg broadcast time (full)"),
        ("avg_compression_time_s", "Avg compression time (ms)"),
        ("avg_decompression_time_s", "Avg decompression time (ms)"),
    ]

    # Prepare JS data blobs for plots: per pair, per metric multi-series
    plots_data: Dict[str, Dict[str, Any]] = {}
    for k in union_keys:
        meta = union_map[k]
        entry = {
            "label": meta["label"],
            "block_size_series": [],
            "compression_percent_series": [],
            "broadcast_time_avg_series": [],
            "broadcast_time_full_series": [],
            "broadcast_time_66p_series": [],
            "compression_time_series": [],
            "decompression_time_series": [],
        }
        for exp in experiments:
            exp_name = exp["name"]
            tos: Dict[str, Dict[str, Any]] = exp["stats"].get("type_called_from_stats", {})
            pair = tos.get(k)
            if not pair:
                continue
            entry["block_size_series"].append({"name": exp_name, "points": pair.get("block_size_points", [])})
            entry["compression_percent_series"].append({"name": exp_name, "points": pair.get("compression_percent_points", [])})
            entry["broadcast_time_avg_series"].append({"name": exp_name, "points": pair.get("broadcast_time_avg_points", [])})
            entry["broadcast_time_full_series"].append({"name": exp_name, "points": pair.get("broadcast_time_full_points", [])})
            entry["broadcast_time_66p_series"].append({"name": exp_name, "points": pair.get("broadcast_time_66p_points", [])})
            entry["compression_time_series"].append({"name": exp_name, "points": pair.get("compression_time_points", [])})
            entry["decompression_time_series"].append({"name": exp_name, "points": pair.get("decompression_time_points", [])})
        plots_data[k] = entry

    # Build HTML
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.26.2.min.js"></script>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen,
                   Ubuntu, Cantarell, "Fira Sans", "Droid Sans", "Helvetica Neue",
                   Arial, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";
      margin: 0;
      padding: 0 16px 48px 16px;
      color: #1f2937;
      background: #f9fafb;
    }}
    h1 {{
      margin: 16px 0 12px 0;
      font-size: 22px;
    }}
    .card {{
      background: white;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06);
      padding: 16px;
      margin: 16px 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid #e5e7eb;
      padding: 8px 10px;
      text-align: right;
      white-space: nowrap;
    }}
    /* Visual separators between (type, called_from) column groups */
    .group-start {{ border-left: 2px solid #9ca3af !important; }}
    /* Highlight best values */
    .best-cell {{ background: #dcfce7; color: #065f46; font-weight: 600; }}
    .other-cell {{ background: #fee2e2; color: #991b1b; }}
    th:first-child, td:first-child {{
      text-align: left;
    }}
    thead th {{
      position: sticky;
      top: 0;
      background: #f3f4f6;
      z-index: 1;
    }}
    .tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 8px 0 16px 0;
    }}
    .tab-btn {{
      padding: 8px 12px;
      border-radius: 6px;
      border: 1px solid #e5e7eb;
      background: white;
      cursor: pointer;
    }}
    .tab-btn.active {{
      background: #111827;
      color: white;
      border-color: #111827;
    }}
    .tab-content {{
      display: none;
    }}
    .tab-content.active {{
      display: block;
    }}
    .plot {{
      width: 100%;
      height: 320px;
    }}
  </style>
</head>
<body>
  <h1>{title}</h1>

  <div class="card">
    <h2 style="margin-top:0">Averages overview</h2>
    <div style="overflow:auto; max-height: 60vh;">
      <table>
        <thead>
          <tr>
            <th>Parameter</th>"""
    # Header row 1: Parameter + each (type, called_from) spanning N experiments
    for k in union_keys:
        label = union_map[k]["label"]
        html += f"<th class=\"group-start\" colspan=\"{len(experiments)}\">{label}</th>"
    html += "</tr>"
    # Header row 2: empty cell + per-pair subcolumns named by experiment short names
    html += "<tr><th></th>"
    for _ in union_keys:
        for idx, short in enumerate(exp_short_names):
            cls = " class=\"group-start\"" if idx == 0 else ""
            html += f"<th{cls}>{short}</th>"
    html += "</tr></thead><tbody>"

    for key_name, row_label in rows_spec:
        html += f"<tr><td>{row_label}</td>"
        # For each pair in union, compute best subcolumn index when applicable
        for k in union_keys:
            # Gather raw values per experiment for this pair/row
            group_vals: List[Optional[float]] = []
            for exp in experiments:
                tos: Dict[str, Dict[str, Any]] = exp["stats"].get("type_called_from_stats", {})
                pair = tos.get(k)
                if not pair:
                    group_vals.append(None)
                    continue
                avg_map = compute_averages_for_pair(pair)
                val = avg_map.get(key_name, None)
                group_vals.append(val if isinstance(val, (int, float)) else None)

            # Decide best index: higher is better for compression percent, lower for broadcast times
            best_idx: Optional[int] = None
            if key_name in ("avg_compression_percent",):
                # pick max among non-None
                best_val = None
                for idx, v in enumerate(group_vals):
                    if v is None:
                        continue
                    if best_val is None or v > best_val:
                        best_val = v
                        best_idx = idx
            elif key_name in ("avg_broadcast_time_avg_s", "avg_broadcast_time_full_s", "avg_broadcast_time_66p_s"):
                # pick min among non-None
                best_val = None
                for idx, v in enumerate(group_vals):
                    if v is None:
                        continue
                    if best_val is None or v < best_val:
                        best_val = v
                        best_idx = idx

            # Emit cells per experiment with formatting and highlighting
            for idx, exp in enumerate(experiments):
                tos: Dict[str, Dict[str, Any]] = exp["stats"].get("type_called_from_stats", {})
                pair = tos.get(k)
                base_cls = " group-start" if idx == 0 else ""

                if not pair:
                    cls_attr = f" class=\"{base_cls.strip()}\"" if base_cls else ""
                    html += f"<td{cls_attr}>-</td>"
                    continue

                avg_map = compute_averages_for_pair(pair)
                val = avg_map.get(key_name, None)

                # Determine highlighting for relevant rows
                extra_cls = ""
                if best_idx is not None and isinstance(val, (int, float)):
                    if idx == best_idx:
                        extra_cls = " best-cell"
                    else:
                        extra_cls = " other-cell"

                cls_combo = (base_cls + extra_cls).strip()
                cls_attr = f" class=\"{cls_combo}\"" if cls_combo else ""

                if key_name == "num_blocks":
                    cell = f"{int(val or 0)}"
                elif key_name == "avg_block_size_bytes":
                    vv = float(val or 0.0)
                    kb = int(round(vv / 1024.0)) if vv else 0
                    cell = f"{kb} KB"
                elif "percent" in key_name:
                    vv = float(val or 0.0)
                    cell = f"{vv*100:.2f}%"
                elif key_name in ("avg_broadcast_time_avg_s", "avg_broadcast_time_full_s", "avg_broadcast_time_66p_s"):
                    vv = float(val or 0.0)
                    cell = f"{vv:.2f} s"
                elif key_name in ("avg_compression_time_s", "avg_decompression_time_s"):
                    vv = float(val or 0.0)
                    cell = f"{vv*1000:.2f} ms"
                else:
                    vv = float(val or 0.0)
                    cell = f"{vv:.3f}"
                html += f"<td{cls_attr}>{cell}</td>"
        html += "</tr>"
    html += "</tbody></table></div></div>"

    # Tabs
    html += '<div class="card"><h2 style="margin-top:0">Details</h2>'
    # Smoothing controls
    html += """
    <div style="display:flex; align-items:center; gap:12px; margin:8px 0 12px 0;">
      <label for="smooth-range" style="min-width: 220px;">Averaging window (seconds)</label>
      <input id="smooth-range" type="range" min="0" max="600" step="1" value="100" />
      <input id="smooth-number" type="number" min="0" max="600" step="1" value="100" style="width:80px;" />
      <span style="color:#6b7280;">0 = show all raw points</span>
      <label style="margin-left:16px; display:flex; align-items:center; gap:8px;">
        <input id="trim-edges" type="checkbox" checked />
        <span>Trim first 5 min and last 2 min</span>
      </label>
    </div>
    """
    html += '<div class="tabs" id="tabs">'
    for k in union_keys:
        label = union_map[k]["label"]
        html += f'<button class="tab-btn" data-tab="{k}">{label}</button>'
    html += "</div>"

    # Tab contents
    for k in union_keys:
        label = union_map[k]["label"]
        html += f"""
  <div class="tab-content" id="tab-{k}">
    <h3 style="margin:8px 0 16px 0">{label}</h3>
    <div id="{k}-block_size" class="plot"></div>
    <div id="{k}-compression_percent" class="plot"></div>
    <div id="{k}-broadcast_avg" class="plot"></div>
    <div id="{k}-broadcast_66p" class="plot"></div>
    <div id="{k}-broadcast_full" class="plot"></div>
    <div id="{k}-compression_time" class="plot"></div>
    <div id="{k}-decompression_time" class="plot"></div>
  </div>
"""
    html += "</div>"  # end card

    # Inject data and JS
    html += "<script>\n"
    html += "const PLOTS_DATA = " + to_js(plots_data) + ";\n"
    html += r"""
let averagingWindowSec = 60; // default 1 minute
let trimEdges = true; // default trim enabled

function aggregatePoints(points, windowSec) {
  if (!points || points.length === 0) return [];
  const sorted = points.slice().sort((a,b) => a[0] - b[0]);
  let minT = sorted[0][0];
  let maxT = sorted[sorted.length - 1][0];
  let allowedMin = trimEdges ? (minT + 300) : minT;  // +5 min
  let allowedMax = trimEdges ? (maxT - 120)  : maxT;  // -2 min
  if (allowedMax < allowedMin) return [];

  if (!windowSec || windowSec <= 0) {
    return sorted.filter(([t,_]) => t >= allowedMin && t <= allowedMax);
  }

  const buckets = new Map();
  for (const [t, v] of sorted) {
    if (t < allowedMin || t > allowedMax) continue;
    const b = Math.floor(t / windowSec);
    const cur = buckets.get(b);
    if (!cur) {
      buckets.set(b, { sum: v, count: 1, sumT: t });
    } else {
      cur.sum += v;
      cur.count += 1;
      cur.sumT += t;
    }
  }
  const out = [];
  const keys = Array.from(buckets.keys()).sort((a,b)=>a-b);
  for (const k of keys) {
    const {sum: s, count: c, sumT: st} = buckets.get(k);
    const tAvg = st / c;
    out.push([tAvg, s / c]);
  }
  return out;
}

function plotSeriesMulti(divId, title, xLabel, yLabel, seriesList, yTickFormat = null, valueScale = 1.0) {
  const traces = [];
  for (const s of (seriesList || [])) {
    const ptsRaw = Array.isArray(s.points) ? s.points : [];
    const scaled = (valueScale === 1.0) ? ptsRaw : ptsRaw.map(p => [p[0], p[1] * valueScale]);
    const pts = aggregatePoints(scaled, averagingWindowSec);
    const x = pts.map(p => p[0]);
    const y = pts.map(p => p[1]);
    traces.push({ x, y, type: 'scatter', mode: 'lines', name: s.name, line: { width: 2 } });
  }
  const layout = {
    title: title,
    margin: {l: 50, r: 20, t: 40, b: 40},
    xaxis: { title: xLabel },
    yaxis: { title: yLabel, rangemode: 'tozero' },
  };
  if (yTickFormat) { layout.yaxis.tickformat = yTickFormat; }
  Plotly.newPlot(divId, traces, layout, {displayModeBar: false, responsive: true});
}

function renderTab(key) {
  const item = PLOTS_DATA[key];
  if (!item) return;
  plotSeriesMulti(`${key}-block_size`, `${item.label} - Block size`, "t (s)", "size (bytes)", item.block_size_series);
  plotSeriesMulti(`${key}-compression_percent`, `${item.label} - Compression %`, "t (s)", "percent", item.compression_percent_series, '.2f', 100.0);
  plotSeriesMulti(`${key}-broadcast_avg`, `${item.label} - Broadcast (avg)`, "t (s)", "seconds", item.broadcast_time_avg_series, '.2f');
  plotSeriesMulti(`${key}-broadcast_66p`, `${item.label} - Broadcast (66p)`, "t (s)", "seconds", item.broadcast_time_66p_series, '.2f');
  plotSeriesMulti(`${key}-broadcast_full`, `${item.label} - Broadcast (full)`, "t (s)", "seconds", item.broadcast_time_full_series, '.2f');
  plotSeriesMulti(`${key}-compression_time`, `${item.label} - Compression time`, "t (s)", "milliseconds", item.compression_time_series, '.2f', 1000.0);
  plotSeriesMulti(`${key}-decompression_time`, `${item.label} - Decompression time`, "t (s)", "milliseconds", item.decompression_time_series, '.2f', 1000.0);
}

function setActiveTab(key) {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === key);
  });
  document.querySelectorAll('.tab-content').forEach(div => {
    div.classList.toggle('active', div.id === 'tab-' + key);
  });
}

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const key = btn.dataset.tab;
    setActiveTab(key);
    renderTab(key);
  });
});

// Controls
const smoothRange = document.getElementById('smooth-range');
const smoothNumber = document.getElementById('smooth-number');
const trimCheckbox = document.getElementById('trim-edges');
function applyAveragingChange(val) {
  const v = Math.max(0, Math.min(600, Number(val) || 0));
  averagingWindowSec = v;
  smoothRange.value = String(v);
  smoothNumber.value = String(v);
  const active = document.querySelector('.tab-btn.active');
  if (active) {
    const key = active.dataset.tab;
    renderTab(key);
  }
}
smoothRange.addEventListener('input', e => applyAveragingChange(e.target.value));
smoothNumber.addEventListener('change', e => applyAveragingChange(e.target.value));
trimCheckbox.addEventListener('change', e => {
  trimEdges = !!e.target.checked;
  const active = document.querySelector('.tab-btn.active');
  if (active) {
    const key = active.dataset.tab;
    renderTab(key);
  }
});

// Activate the first tab by default
const firstBtn = document.querySelector('.tab-btn');
if (firstBtn) {
  const key = firstBtn.dataset.tab;
  setActiveTab(key);
  renderTab(key);
}
</script>
</body>
</html>
"""
    return html


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: python3 render_stats_html.py <experiment_name> [more_experiment_names ...]\n"
            "       (reads stats from stats/<experiment_name>.json)"
        )
        sys.exit(1)

    args = sys.argv[1:]
    stats_dir = Path("stats")

    # For convenience, allow either plain experiment names or explicit JSON paths.
    #   - "current_compr_1mb_network" -> stats/current_compr_1mb_network.json
    #   - "stats/foo.json" or "/abs/path/foo.json" -> used as‑is
    json_paths: List[Path] = []
    for arg in args:
        p = Path(arg)
        if p.suffix == ".json" or p.is_absolute() or "/" in arg:
            json_paths.append(p)
        else:
            json_paths.append(stats_dir / f"{arg}.json")

    for p in json_paths:
        if not p.exists() or not p.is_file():
            print(f"Error: JSON file not found: {p}")
            sys.exit(1)

    experiments: List[Dict[str, Any]] = []
    names: List[str] = []
    for json_path in json_paths:
        with open(json_path, "r", encoding="utf-8") as f:
            stats = json.load(f)
        name = stats.get("experiment_name") or json_path.stem
        experiments.append({"name": name, "stats": stats, "path": json_path})
        names.append(name)

    title = "Stats Report - " + ", ".join(names)
    html = build_html(title, experiments)

    # Write HTML(s) into the dedicated "renders" directory.
    renders_dir = Path("renders")
    renders_dir.mkdir(parents=True, exist_ok=True)

    if len(names) == 1:
        out_path = renders_dir / f"{names[0]}.html"
    else:
        combined = "__".join(names)[:120]
        out_path = renders_dir / f"{combined}.html"

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote HTML report to {out_path}")


if __name__ == "__main__":
    main()


