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
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


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


def build_html(title: str, experiments: List[Dict[str, Any]]) -> str:
    # experiments: [{"name": str, "stats": dict}]
    # Build union of (type, called_from) pairs across experiments
    union_map: Dict[str, Dict[str, Any]] = {}
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
        (Path(exp["path"]).stem if "path" in exp else str(exp["name"]))
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
            "block_size_by_id_series": [],
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
            base_ts = exp["stats"].get("earliest_ts_global", None)  # ISO timestamp string
            tos: Dict[str, Dict[str, Any]] = exp["stats"].get("type_called_from_stats", {})
            pair = tos.get(k)
            if not pair:
                entry["block_size_by_id_series"].append({})
                entry["block_size_series"].append({"name": exp_name, "base_ts": base_ts, "points": []})
                entry["compression_percent_series"].append({"name": exp_name, "base_ts": base_ts, "points": []})
                entry["broadcast_time_avg_series"].append({"name": exp_name, "base_ts": base_ts, "points": []})
                entry["broadcast_time_full_series"].append({"name": exp_name, "base_ts": base_ts, "points": []})
                entry["broadcast_time_66p_series"].append({"name": exp_name, "base_ts": base_ts, "points": []})
                entry["compression_time_series"].append({"name": exp_name, "base_ts": base_ts, "points": []})
                entry["decompression_time_series"].append({"name": exp_name, "base_ts": base_ts, "points": []})
                continue
            entry["block_size_by_id_series"].append(pair.get("block_size_by_id", {}))
            entry["block_size_series"].append({"name": exp_name, "base_ts": base_ts, "points": pair.get("block_size_points", [])})
            entry["compression_percent_series"].append({"name": exp_name, "base_ts": base_ts, "points": pair.get("compression_percent_points", [])})
            entry["broadcast_time_avg_series"].append({"name": exp_name, "base_ts": base_ts, "points": pair.get("broadcast_time_avg_points", [])})
            entry["broadcast_time_full_series"].append({"name": exp_name, "base_ts": base_ts, "points": pair.get("broadcast_time_full_points", [])})
            entry["broadcast_time_66p_series"].append({"name": exp_name, "base_ts": base_ts, "points": pair.get("broadcast_time_66p_points", [])})
            entry["compression_time_series"].append({"name": exp_name, "base_ts": base_ts, "points": pair.get("compression_time_points", [])})
            entry["decompression_time_series"].append({"name": exp_name, "base_ts": base_ts, "points": pair.get("decompression_time_points", [])})
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
    /* Timeline slider */
    .timeline-container {{
      background: white;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06);
      padding: 16px;
      margin: 16px 0;
    }}
    .timeline-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }}
    .timeline-header h2 {{
      margin: 0;
      font-size: 16px;
    }}
    .timeline-labels {{
      display: flex;
      gap: 24px;
      font-size: 13px;
      color: #374151;
    }}
    .timeline-labels span {{
      font-family: monospace;
    }}
    .timeline-track {{
      position: relative;
      height: 40px;
      background: #e5e7eb;
      border-radius: 6px;
      cursor: pointer;
      user-select: none;
    }}
    .timeline-selection {{
      position: absolute;
      top: 0;
      height: 100%;
      background: #3b82f6;
      opacity: 0.3;
      border-radius: 6px;
    }}
    .timeline-handle {{
      position: absolute;
      top: 0;
      width: 12px;
      height: 100%;
      background: #1d4ed8;
      border-radius: 4px;
      cursor: ew-resize;
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      font-size: 10px;
      font-weight: bold;
      z-index: 2;
    }}
    .timeline-handle:hover {{
      background: #1e40af;
    }}
    .timeline-handle.left {{
      border-top-right-radius: 0;
      border-bottom-right-radius: 0;
    }}
    .timeline-handle.right {{
      border-top-left-radius: 0;
      border-bottom-left-radius: 0;
    }}
  </style>
</head>
<body>
  <h1>{title}</h1>

  <div class="timeline-container">
    <div class="timeline-header">
      <h2>Time Range Filter</h2>
      <div class="timeline-labels">
        <span>L: <span id="timeline-label-left">--</span></span>
        <span>R: <span id="timeline-label-right">--</span></span>
        <span>Duration: <span id="timeline-duration">--</span></span>
      </div>
    </div>
    <div class="timeline-track" id="timeline-track">
      <div class="timeline-selection" id="timeline-selection"></div>
      <div class="timeline-handle left" id="timeline-handle-left">L</div>
      <div class="timeline-handle right" id="timeline-handle-right">R</div>
    </div>
  </div>

  <div class="card" id="averages-card">
    <h2 style="margin-top:0">Averages overview</h2>
    <div id="averages-table-container" style="overflow:auto; max-height: 60vh;"></div>
  </div>"""

    # Tabs
    html += '<div class="card"><h2 style="margin-top:0">Details</h2>'
    # Smoothing controls
    html += """
    <div style="display:flex; align-items:center; gap:12px; margin:8px 0 12px 0; flex-wrap:wrap;">
      <label for="smooth-range" style="min-width: 220px;">Averaging window (seconds)</label>
      <input id="smooth-range" type="range" min="0" max="600" step="1" value="100" />
      <input id="smooth-number" type="number" min="0" max="600" step="1" value="100" style="width:80px;" />
      <span style="color:#6b7280;">0 = show all raw points</span>
      <label for="block-size-range" style="min-width: 220px; margin-left:16px;">Min block size (KB)</label>
      <input id="block-size-range" type="range" min="0" max="0" step="1" value="0" />
      <input id="block-size-number" type="number" min="0" max="0" step="1" value="0" style="width:80px;" />
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
    # Prepare additional data for JS
    rows_spec_js = [
        ["num_blocks", "Blocks"],
        ["avg_block_size_bytes", "Avg block size (KB)"],
        ["avg_compression_percent", "Avg compression percent"],
        ["avg_broadcast_time_avg_s", "Avg broadcast time (avg)"],
        ["avg_broadcast_time_66p_s", "Avg broadcast time (66p)"],
        ["avg_broadcast_time_full_s", "Avg broadcast time (full)"],
        ["avg_compression_time_s", "Avg compression time (ms)"],
        ["avg_decompression_time_s", "Avg decompression time (ms)"],
    ]
    
    html += "<script>\n"
    html += "const PLOTS_DATA = " + to_js(plots_data) + ";\n"
    html += "const UNION_KEYS = " + to_js(union_keys) + ";\n"
    html += "const UNION_MAP = " + to_js(union_map) + ";\n"
    html += "const EXP_SHORT_NAMES = " + to_js(exp_short_names) + ";\n"
    html += "const ROWS_SPEC = " + to_js(rows_spec_js) + ";\n"
    html += r"""
let averagingWindowSec = 60; // default 1 minute
let trimEdges = true; // default trim enabled
let minBlockSizeBytes = 0; // 0 = no filter
const USE_ABSOLUTE_TIME = EXP_SHORT_NAMES.length === 1;

// Timeline range (in data coordinates - seconds from start)
let globalMinT = Infinity;
let globalMaxT = -Infinity;
let rangeMinT = 0;  // current left bound
let rangeMaxT = 0;  // current right bound
let globalBaseTsMs = null;  // base timestamp in milliseconds for converting to actual dates

let globalMinBlockSize = Infinity;
let globalMaxBlockSize = -Infinity;

// Compute global time range from all data
function computeGlobalTimeRange() {
  let earliestAbsoluteMs = Infinity;

  for (const key of Object.keys(PLOTS_DATA)) {
    const item = PLOTS_DATA[key];
    const allSeries = [
      ...(item.block_size_series || []),
      ...(item.compression_percent_series || []),
      ...(item.broadcast_time_avg_series || []),
      ...(item.broadcast_time_full_series || []),
      ...(item.broadcast_time_66p_series || []),
      ...(item.compression_time_series || []),
      ...(item.decompression_time_series || []),
    ];
    for (const s of allSeries) {
      const baseMs = USE_ABSOLUTE_TIME && s.base_ts ? new Date(s.base_ts).getTime() : 0;
      for (const [t, _] of (s.points || [])) {
        if (t < globalMinT) globalMinT = t;
        if (t > globalMaxT) globalMaxT = t;
        if (USE_ABSOLUTE_TIME) {
          const absMs = baseMs + t * 1000;
          if (absMs < earliestAbsoluteMs) {
            earliestAbsoluteMs = absMs;
            globalBaseTsMs = baseMs;
          }
        }
      }
    }
  }
  if (globalMinT === Infinity) globalMinT = 0;
  if (globalMaxT === -Infinity) globalMaxT = 1;
  if (USE_ABSOLUTE_TIME && globalBaseTsMs === null) globalBaseTsMs = 0;
  rangeMinT = globalMinT;
  rangeMaxT = globalMaxT;
}

function computeGlobalBlockSizeRange() {
  globalMinBlockSize = Infinity;
  globalMaxBlockSize = -Infinity;
  for (const key of Object.keys(PLOTS_DATA)) {
    const item = PLOTS_DATA[key];
    for (const s of (item.block_size_series || [])) {
      for (const p of (s.points || [])) {
        const size = p[1];
        if (typeof size !== 'number') continue;
        if (size < globalMinBlockSize) globalMinBlockSize = size;
        if (size > globalMaxBlockSize) globalMaxBlockSize = size;
      }
    }
  }
  if (globalMinBlockSize === Infinity) globalMinBlockSize = 0;
  if (globalMaxBlockSize === -Infinity) globalMaxBlockSize = 0;
}

function hasBlocksAfterSizeFilter(item) {
  if (!item) return false;
  if (!minBlockSizeBytes || minBlockSizeBytes <= 0) {
    return (item.block_size_series || []).some(s => (s.points || []).length > 0);
  }
  return (item.block_size_series || []).some(s =>
    (s.points || []).some(p => typeof p[1] === 'number' && p[1] >= minBlockSizeBytes),
  );
}

function getVisibleUnionKeys() {
  return UNION_KEYS.filter(k => hasBlocksAfterSizeFilter(PLOTS_DATA[k]));
}

function formatDuration(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function secsToTimestamp(secs) {
  if (!USE_ABSOLUTE_TIME || globalBaseTsMs === null) return formatDuration(secs);
  const d = new Date(globalBaseTsMs + secs * 1000);
  // Format as HH:MM:SS
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

function updateTimelineLabels() {
  const leftVal = USE_ABSOLUTE_TIME ? secsToTimestamp(rangeMinT) : formatDuration(rangeMinT);
  const rightVal = USE_ABSOLUTE_TIME ? secsToTimestamp(rangeMaxT) : formatDuration(rangeMaxT);
  document.getElementById('timeline-label-left').textContent = leftVal;
  document.getElementById('timeline-label-right').textContent = rightVal;
  document.getElementById('timeline-duration').textContent = formatDuration(rangeMaxT - rangeMinT);
}

function updateTimelineHandles() {
  const track = document.getElementById('timeline-track');
  const handleL = document.getElementById('timeline-handle-left');
  const handleR = document.getElementById('timeline-handle-right');
  const selection = document.getElementById('timeline-selection');
  
  const trackWidth = track.offsetWidth;
  const range = globalMaxT - globalMinT;
  if (range <= 0) return;
  
  const leftPct = (rangeMinT - globalMinT) / range;
  const rightPct = (rangeMaxT - globalMinT) / range;
  
  const leftPx = leftPct * trackWidth;
  const rightPx = rightPct * trackWidth;
  
  handleL.style.left = `${leftPx - 6}px`;  // center the 12px handle
  handleR.style.left = `${rightPx - 6}px`;
  
  selection.style.left = `${leftPx}px`;
  selection.style.width = `${rightPx - leftPx}px`;
  
  updateTimelineLabels();
}

function setupTimelineDrag() {
  const track = document.getElementById('timeline-track');
  const handleL = document.getElementById('timeline-handle-left');
  const handleR = document.getElementById('timeline-handle-right');
  
  let dragging = null;  // 'left' or 'right'
  
  function onMove(e) {
    if (!dragging) return;
    const rect = track.getBoundingClientRect();
    const x = (e.clientX || e.touches[0].clientX) - rect.left;
    const pct = Math.max(0, Math.min(1, x / rect.width));
    const t = globalMinT + pct * (globalMaxT - globalMinT);
    
    if (dragging === 'left') {
      rangeMinT = Math.min(t, rangeMaxT - 1);
    } else {
      rangeMaxT = Math.max(t, rangeMinT + 1);
    }
    updateTimelineHandles();
  }
  
  function onEnd() {
    if (dragging) {
      dragging = null;
      rebuildAll();
    }
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onEnd);
    document.removeEventListener('touchmove', onMove);
    document.removeEventListener('touchend', onEnd);
  }
  
  function startDrag(handle, e) {
    e.preventDefault();
    dragging = handle;
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onEnd);
    document.addEventListener('touchmove', onMove);
    document.addEventListener('touchend', onEnd);
  }
  
  handleL.addEventListener('mousedown', e => startDrag('left', e));
  handleL.addEventListener('touchstart', e => startDrag('left', e));
  handleR.addEventListener('mousedown', e => startDrag('right', e));
  handleR.addEventListener('touchstart', e => startDrag('right', e));
  
  // Click on track to move nearest handle
  track.addEventListener('click', e => {
    if (e.target === handleL || e.target === handleR) return;
    const rect = track.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const pct = x / rect.width;
    const t = globalMinT + pct * (globalMaxT - globalMinT);
    
    // Move nearest handle
    const distL = Math.abs(t - rangeMinT);
    const distR = Math.abs(t - rangeMaxT);
    if (distL < distR) {
      rangeMinT = Math.min(t, rangeMaxT - 1);
    } else {
      rangeMaxT = Math.max(t, rangeMinT + 1);
    }
    updateTimelineHandles();
    rebuildAll();
  });
}

function filterByBlockSize(points, blockSizeById) {
  if (!minBlockSizeBytes || minBlockSizeBytes <= 0) return points;
  if (!points || points.length === 0) return [];
  const out = [];
  for (const p of points) {
    const blockId = p[2];
    const size = blockSizeById ? blockSizeById[blockId] : null;
    if (typeof size === 'number' && size >= minBlockSizeBytes) {
      out.push(p);
    }
  }
  return out;
}

function aggregatePoints(points, windowSec, blockSizeById) {
  if (!points || points.length === 0) return [];
  const sizeFiltered = filterByBlockSize(points, blockSizeById);
  if (sizeFiltered.length === 0) return [];
  const sorted = sizeFiltered.slice().sort((a,b) => a[0] - b[0]);
  
  // Apply timeline range filter first
  const rangeFiltered = sorted.filter(([t,_]) => t >= rangeMinT && t <= rangeMaxT);
  if (rangeFiltered.length === 0) return [];
  
  let minT = rangeFiltered[0][0];
  let maxT = rangeFiltered[rangeFiltered.length - 1][0];
  let allowedMin = trimEdges ? (minT + 300) : minT;  // +5 min
  let allowedMax = trimEdges ? (maxT - 120)  : maxT;  // -2 min
  if (allowedMax < allowedMin) return [];

  if (!windowSec || windowSec <= 0) {
    return rangeFiltered.filter(([t,_]) => t >= allowedMin && t <= allowedMax);
  }

  const buckets = new Map();
  for (const [t, v] of rangeFiltered) {
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

// Compute averages for a (type, called_from) pair within current time range
function computeAveragesForPair(pairData) {
  const blockSizeById = pairData.block_size_by_id || {};
  function avgFromPoints(pointsKey) {
    const pts = pairData[pointsKey] || [];
    const sizeFiltered = filterByBlockSize(pts, blockSizeById);
    // Filter by timeline range
    const filtered = sizeFiltered.filter(([t,_]) => t >= rangeMinT && t <= rangeMaxT);
    if (filtered.length === 0) return 0;
    const sum = filtered.reduce((acc, [_,v]) => acc + v, 0);
    return sum / filtered.length;
  }
  
  function countFromPoints(pointsKey) {
    const pts = pairData[pointsKey] || [];
    const sizeFiltered = filterByBlockSize(pts, blockSizeById);
    return sizeFiltered.filter(([t,_]) => t >= rangeMinT && t <= rangeMaxT).length;
  }
  
  return {
    num_blocks: countFromPoints('block_size_points'),
    avg_block_size_bytes: avgFromPoints('block_size_points'),
    avg_compression_percent: avgFromPoints('compression_percent_points'),
    avg_broadcast_time_avg_s: avgFromPoints('broadcast_time_avg_points'),
    avg_broadcast_time_full_s: avgFromPoints('broadcast_time_full_points'),
    avg_broadcast_time_66p_s: avgFromPoints('broadcast_time_66p_points'),
    avg_compression_time_s: avgFromPoints('compression_time_points'),
    avg_decompression_time_s: avgFromPoints('decompression_time_points'),
  };
}

function rebuildAveragesTable() {
  const container = document.getElementById('averages-table-container');
  
  let html = '<table><thead><tr><th>Parameter</th>';
  // Header row 1: (type, called_from) groups
  const visibleKeys = getVisibleUnionKeys();
  if (visibleKeys.length === 0) {
    container.innerHTML = '<div style="color:#6b7280;">No blocks match the current size filter.</div>';
    return;
  }
  for (const k of visibleKeys) {
    const label = UNION_MAP[k].label;
    html += `<th class="group-start" colspan="${EXP_SHORT_NAMES.length}">${label}</th>`;
  }
  html += '</tr><tr><th></th>';
  // Header row 2: experiment names
  for (const _ of visibleKeys) {
    for (let i = 0; i < EXP_SHORT_NAMES.length; i++) {
      const cls = i === 0 ? ' class="group-start"' : '';
      html += `<th${cls}>${EXP_SHORT_NAMES[i]}</th>`;
    }
  }
  html += '</tr></thead><tbody>';
  
  // Compute all averages first
  const allAvgs = {};  // allAvgs[unionKey][expIdx] = avgMap
  for (const k of visibleKeys) {
    allAvgs[k] = [];
    const item = PLOTS_DATA[k];
    if (!item) {
      for (let i = 0; i < EXP_SHORT_NAMES.length; i++) allAvgs[k].push(null);
      continue;
    }
    // For each experiment, find its data in the series
    for (let expIdx = 0; expIdx < EXP_SHORT_NAMES.length; expIdx++) {
      // Build pair data from the series for this experiment
      const pairData = {
        block_size_points: item.block_size_series[expIdx]?.points || [],
        compression_percent_points: item.compression_percent_series[expIdx]?.points || [],
        broadcast_time_avg_points: item.broadcast_time_avg_series[expIdx]?.points || [],
        broadcast_time_full_points: item.broadcast_time_full_series[expIdx]?.points || [],
        broadcast_time_66p_points: item.broadcast_time_66p_series[expIdx]?.points || [],
        compression_time_points: item.compression_time_series[expIdx]?.points || [],
        decompression_time_points: item.decompression_time_series[expIdx]?.points || [],
        block_size_by_id: item.block_size_by_id_series[expIdx] || {},
      };
      const avgMap = computeAveragesForPair(pairData);
      allAvgs[k].push(avgMap);
    }
  }
  
  // Rows
  for (const [keyName, rowLabel] of ROWS_SPEC) {
    html += `<tr><td>${rowLabel}</td>`;
    
    for (const k of visibleKeys) {
      // Gather values for this pair across experiments
      const groupVals = allAvgs[k].map(avg => avg ? avg[keyName] : null);
      
      // Determine best index
      let bestIdx = null;
      if (keyName === 'avg_compression_percent') {
        let bestVal = null;
        for (let i = 0; i < groupVals.length; i++) {
          if (groupVals[i] != null && (bestVal === null || groupVals[i] > bestVal)) {
            bestVal = groupVals[i];
            bestIdx = i;
          }
        }
      } else if (['avg_broadcast_time_avg_s', 'avg_broadcast_time_full_s', 'avg_broadcast_time_66p_s'].includes(keyName)) {
        let bestVal = null;
        for (let i = 0; i < groupVals.length; i++) {
          if (groupVals[i] != null && groupVals[i] > 0 && (bestVal === null || groupVals[i] < bestVal)) {
            bestVal = groupVals[i];
            bestIdx = i;
          }
        }
      }
      
      // Emit cells
      for (let i = 0; i < EXP_SHORT_NAMES.length; i++) {
        let baseCls = i === 0 ? 'group-start' : '';
        const val = groupVals[i];
        
        if (val == null || (keyName === 'num_blocks' && val === 0)) {
          const clsAttr = baseCls ? ` class="${baseCls}"` : '';
          html += `<td${clsAttr}>-</td>`;
          continue;
        }
        
        let extraCls = '';
        if (bestIdx !== null) {
          if (i === bestIdx) extraCls = 'best-cell';
          else extraCls = 'other-cell';
        }
        const clsCombo = [baseCls, extraCls].filter(c => c).join(' ');
        const clsAttr = clsCombo ? ` class="${clsCombo}"` : '';
        
        let cell;
        if (keyName === 'num_blocks') {
          cell = Math.round(val);
        } else if (keyName === 'avg_block_size_bytes') {
          cell = Math.round(val / 1024) + ' KB';
        } else if (keyName.includes('percent')) {
          cell = (val * 100).toFixed(2) + '%';
        } else if (['avg_broadcast_time_avg_s', 'avg_broadcast_time_full_s', 'avg_broadcast_time_66p_s'].includes(keyName)) {
          cell = val.toFixed(2) + ' s';
        } else if (['avg_compression_time_s', 'avg_decompression_time_s'].includes(keyName)) {
          cell = (val * 1000).toFixed(2) + ' ms';
        } else {
          cell = val.toFixed(3);
        }
        html += `<td${clsAttr}>${cell}</td>`;
      }
    }
    html += '</tr>';
  }
  
  html += '</tbody></table>';
  container.innerHTML = html;
}

function rebuildAll() {
  rebuildAveragesTable();
  updateVisibleTabs();
  const active = document.querySelector('.tab-btn.active');
  if (active) {
    renderTab(active.dataset.tab);
  }
}

// Convert relative seconds to Date given base timestamp
function secsToDate(baseTsStr, secs) {
  if (!baseTsStr) return new Date(secs * 1000);  // fallback: treat as unix epoch
  const baseMs = new Date(baseTsStr).getTime();
  return new Date(baseMs + secs * 1000);
}

function plotSeriesMulti(divId, title, xLabel, yLabel, seriesList, blockSizeByIdSeries, yTickFormat = null, valueScale = 1.0) {
  const traces = [];
  const series = seriesList || [];
  for (let idx = 0; idx < series.length; idx++) {
    const s = series[idx];
    const blockSizeById = (blockSizeByIdSeries || [])[idx] || {};
    const ptsRaw = Array.isArray(s.points) ? s.points : [];
    const scaled = (valueScale === 1.0) ? ptsRaw : ptsRaw.map(p => [p[0], p[1] * valueScale, p[2]]);
    const pts = aggregatePoints(scaled, averagingWindowSec, blockSizeById);
    // Convert seconds to actual timestamps using base_ts if single experiment
    const x = USE_ABSOLUTE_TIME
      ? pts.map(p => secsToDate(s.base_ts, p[0]))
      : pts.map(p => p[0]);
    const y = pts.map(p => p[1]);
    traces.push({ x, y, type: 'scatter', mode: 'lines', name: s.name, line: { width: 2 } });
  }
  const layout = {
    title: title,
    margin: {l: 50, r: 20, t: 40, b: 60},
    xaxis: USE_ABSOLUTE_TIME ? { title: xLabel, type: 'date' } : { title: xLabel },
    yaxis: { title: yLabel, rangemode: 'tozero' },
  };
  if (yTickFormat) { layout.yaxis.tickformat = yTickFormat; }
  Plotly.newPlot(divId, traces, layout, {displayModeBar: false, responsive: true});
}

function renderTab(key) {
  const item = PLOTS_DATA[key];
  if (!item) return;
  if (!hasBlocksAfterSizeFilter(item)) return;
  const blockSizeByIdSeries = item.block_size_by_id_series || [];
  plotSeriesMulti(`${key}-block_size`, `${item.label} - Block size`, "time", "size (bytes)", item.block_size_series, blockSizeByIdSeries);
  plotSeriesMulti(`${key}-compression_percent`, `${item.label} - Compression %`, "time", "percent", item.compression_percent_series, blockSizeByIdSeries, '.2f', 100.0);
  plotSeriesMulti(`${key}-broadcast_avg`, `${item.label} - Broadcast (avg)`, "time", "seconds", item.broadcast_time_avg_series, blockSizeByIdSeries, '.2f');
  plotSeriesMulti(`${key}-broadcast_66p`, `${item.label} - Broadcast (66p)`, "time", "seconds", item.broadcast_time_66p_series, blockSizeByIdSeries, '.2f');
  plotSeriesMulti(`${key}-broadcast_full`, `${item.label} - Broadcast (full)`, "time", "seconds", item.broadcast_time_full_series, blockSizeByIdSeries, '.2f');
  plotSeriesMulti(`${key}-compression_time`, `${item.label} - Compression time`, "time", "milliseconds", item.compression_time_series, blockSizeByIdSeries, '.2f', 1000.0);
  plotSeriesMulti(`${key}-decompression_time`, `${item.label} - Decompression time`, "time", "milliseconds", item.decompression_time_series, blockSizeByIdSeries, '.2f', 1000.0);
}

function updateVisibleTabs() {
  const visibleKeys = new Set(getVisibleUnionKeys());
  let firstVisible = null;
  document.querySelectorAll('.tab-btn').forEach(btn => {
    const isVisible = visibleKeys.has(btn.dataset.tab);
    btn.style.display = isVisible ? '' : 'none';
    if (isVisible && !firstVisible) firstVisible = btn.dataset.tab;
  });
  document.querySelectorAll('.tab-content').forEach(div => {
    const key = div.id.replace('tab-', '');
    const isVisible = visibleKeys.has(key);
    div.style.display = isVisible ? '' : 'none';
  });
  const active = document.querySelector('.tab-btn.active');
  if (!active || !visibleKeys.has(active.dataset.tab)) {
    if (firstVisible) {
      setActiveTab(firstVisible);
      renderTab(firstVisible);
    }
  }
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
const blockSizeRange = document.getElementById('block-size-range');
const blockSizeNumber = document.getElementById('block-size-number');
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
function applyBlockSizeChange(val) {
  const maxKb = Math.max(0, Number(blockSizeRange.max) || 0);
  const v = Math.max(0, Math.min(maxKb, Number(val) || 0));
  minBlockSizeBytes = Math.round(v * 1024);
  blockSizeRange.value = String(v);
  blockSizeNumber.value = String(v);
  rebuildAll();
}
blockSizeRange.addEventListener('input', e => applyBlockSizeChange(e.target.value));
blockSizeNumber.addEventListener('change', e => applyBlockSizeChange(e.target.value));
trimCheckbox.addEventListener('change', e => {
  trimEdges = !!e.target.checked;
  const active = document.querySelector('.tab-btn.active');
  if (active) {
    const key = active.dataset.tab;
    renderTab(key);
  }
});

// Initialize timeline and block size slider
computeGlobalTimeRange();
computeGlobalBlockSizeRange();
const maxKb = Math.ceil(globalMaxBlockSize / 1024);
blockSizeRange.max = String(maxKb);
blockSizeNumber.max = String(maxKb);
blockSizeRange.value = "0";
blockSizeNumber.value = "0";
setupTimelineDrag();
updateTimelineHandles();

// Build initial averages table
rebuildAveragesTable();

// Activate the first tab by default
const firstBtn = document.querySelector('.tab-btn');
if (firstBtn) {
  const key = firstBtn.dataset.tab;
  setActiveTab(key);
  renderTab(key);
}

// Handle window resize for timeline
window.addEventListener('resize', () => {
  updateTimelineHandles();
});
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
        name = json_path.stem
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
    
    # Open the HTML file with the default application
    try:
        subprocess.run(["open", str(out_path)], check=True)
        print(f"Opened {out_path} in default browser")
    except subprocess.CalledProcessError:
        print(f"Failed to open {out_path}")
    except FileNotFoundError:
        # 'open' command not available (not on macOS)
        print("Note: 'open' command not available on this system")


if __name__ == "__main__":
    main()


