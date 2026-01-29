const ROWS_SPEC = [
  ["num_blocks", "Blocks"],
  ["avg_block_size_bytes", "Avg block size (KB)"],
  ["avg_compression_percent", "Avg compression percent"],
  ["avg_broadcast_time_avg_s", "Avg broadcast time (avg)"],
  ["avg_broadcast_time_66p_s", "Avg broadcast time (66p)"],
  ["avg_broadcast_time_full_s", "Avg broadcast time (full)"],
  ["avg_compression_time_s", "Avg compression time (ms)"],
  ["avg_decompression_time_s", "Avg decompression time (ms)"],
];

let PLOTS_DATA = {};
let UNION_KEYS = [];
let UNION_MAP = {};
let EXP_SHORT_NAMES = [];
let USE_ABSOLUTE_TIME = false;

let averagingWindowSec = 10; // default 10 seconds
let trimEdges = true; // default trim enabled
let minBlockSizeBytes = 0; // 0 = no filter

let globalMinT = Infinity;
let globalMaxT = -Infinity;
let rangeMinT = 0;  // current left bound
let rangeMaxT = 0;  // current right bound
let globalBaseTsMs = null;  // base timestamp in milliseconds for converting to actual dates

let globalMinBlockSize = Infinity;
let globalMaxBlockSize = -Infinity;

function setStatus(message, state = "") {
  const overlay = document.getElementById("status-overlay");
  const card = document.getElementById("status-card");
  const text = document.getElementById("status-text");
  if (!overlay || !card || !text) return;
  if (!message) {
    overlay.classList.remove("visible");
    return;
  }
  text.textContent = message;
  card.className = "status-card";
  if (state) card.classList.add(state);
  overlay.classList.add("visible");
}

function indexMap(fields, fallback) {
  const list = (fields && fields.length) ? fields : fallback;
  const map = {};
  for (let i = 0; i < list.length; i++) {
    map[list[i]] = i;
  }
  return map;
}

function percentile(values, p) {
  if (!values || values.length === 0) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  if (sorted.length === 1) return sorted[0];
  const pos = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  if (lower === upper) return sorted[lower];
  const weight = pos - lower;
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

function formatLocalIso(ms) {
  const d = new Date(ms);
  const pad = (val, len = 2) => String(val).padStart(len, "0");
  const yyyy = d.getFullYear();
  const mm = pad(d.getMonth() + 1);
  const dd = pad(d.getDate());
  const hh = pad(d.getHours());
  const mi = pad(d.getMinutes());
  const ss = pad(d.getSeconds());
  const msPart = pad(d.getMilliseconds(), 3);
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}.${msPart}`;
}

function computeStatsFromCompressed(payload, fallbackName) {
  const recordFields = payload.record_fields || [
    "node_idx",
    "start_us",
    "duration_us",
    "stage_idx",
    "type_idx",
    "called_from_idx",
    "compression_idx",
    "size_idx",
  ];
  const blockFields = payload.block_fields || ["block_id", "size_map", "records"];
  const sizeFields = payload.size_fields || ["original_size", "compressed_size"];

  const recIdx = indexMap(recordFields, recordFields);
  const blockIdx = indexMap(blockFields, blockFields);
  const sizeIdx = indexMap(sizeFields, sizeFields);

  const maps = payload.maps || {};
  const stageMap = maps.stage || [];
  const typeMap = maps.type || [];
  const calledFromMap = maps.called_from || [];

  const ts0 = payload.ts0 ? Date.parse(payload.ts0) : null;
  if (!Array.isArray(payload.blocks) || ts0 === null || Number.isNaN(ts0)) {
    return {
      experiment_name: payload.experiment_name || fallbackName || "",
      earliest_ts_global: null,
      type_called_from_stats: {},
    };
  }

  const grouped = new Map();
  let earliestSec = Infinity;

  for (const blockEntry of payload.blocks) {
    const blockId = blockEntry[blockIdx.block_id];
    const sizeMap = blockEntry[blockIdx.size_map] || [];
    const records = blockEntry[blockIdx.records] || [];

    for (const rec of records) {
      const startUs = rec[recIdx.start_us];
      const durationUs = rec[recIdx.duration_us];
      if (startUs === undefined || durationUs === undefined) continue;

      const startMs = ts0 + (startUs / 1000.0);
      const endMs = startMs + (durationUs / 1000.0);
      const startSec = startMs / 1000.0;
      const endSec = endMs / 1000.0;
      const durationSec = durationUs / 1_000_000.0;

      if (startSec < earliestSec) earliestSec = startSec;
      if (endSec < earliestSec) earliestSec = endSec;

      const stage = stageMap[rec[recIdx.stage_idx]];
      const type = typeMap[rec[recIdx.type_idx]];
      const calledFrom = calledFromMap[rec[recIdx.called_from_idx]];

      const sizeIndex = rec[recIdx.size_idx];
      const sizePair = (sizeMap && sizeMap[sizeIndex]) ? sizeMap[sizeIndex] : [null, null];
      const originalSize = sizePair[sizeIdx.original_size];
      const compressedSize = sizePair[sizeIdx.compressed_size];

      const key = `${type || "None"}__${calledFrom || "None"}`;
      let group = grouped.get(key);
      if (!group) {
        group = { type, called_from: calledFrom ?? null, blocks: new Map() };
        grouped.set(key, group);
      }
      let blockRecs = group.blocks.get(blockId);
      if (!blockRecs) {
        blockRecs = [];
        group.blocks.set(blockId, blockRecs);
      }
      blockRecs.push({
        start_sec: startSec,
        end_sec: endSec,
        duration_sec: durationSec,
        stage,
        original_size: originalSize,
        compressed_size: compressedSize,
      });
    }
  }

  if (earliestSec === Infinity) {
    return {
      experiment_name: payload.experiment_name || fallbackName || "",
      earliest_ts_global: null,
      type_called_from_stats: {},
    };
  }

  function shiftPoints(points) {
    return points
      .map(([t, v, blockId]) => [t - earliestSec, v, blockId])
      .sort((a, b) => a[0] - b[0]);
  }

  const statsByKey = {};
  for (const [key, group] of grouped.entries()) {
    const byBlock = group.blocks;
    const numBlocks = byBlock.size;

    const blockSizePointsDt = [];
    const compressionPercentPointsDt = [];
    const broadcastTimeAvgPointsDt = [];
    const broadcastTimeFullPointsDt = [];
    const broadcastTime66pPointsDt = [];
    const compressionTimePointsDt = [];
    const decompressionTimePointsDt = [];
    const blockSizeById = {};

    for (const [blockId, recs] of byBlock.entries()) {
      recs.sort((a, b) => a.start_sec - b.start_sec);

      for (const rec of recs) {
        if (rec.stage === "compress") {
          compressionTimePointsDt.push([rec.end_sec, rec.duration_sec, blockId]);
        } else if (rec.stage === "decompress") {
          decompressionTimePointsDt.push([rec.end_sec, rec.duration_sec, blockId]);
        }
      }

      const compressTs = recs.filter(r => r.stage === "compress").map(r => r.start_sec);
      const decompressTs = recs.filter(r => r.stage === "decompress").map(r => r.end_sec);
      let tsBlock = null;
      if (compressTs.length && decompressTs.length) {
        tsBlock = Math.min(...compressTs);
        const earliestCompress = tsBlock;
        const latestDecompress = Math.max(...decompressTs);

        broadcastTimeFullPointsDt.push([tsBlock, latestDecompress - earliestCompress, blockId]);

        const avgDecomp = decompressTs.reduce((a, b) => a + b, 0) / decompressTs.length;
        broadcastTimeAvgPointsDt.push([tsBlock, avgDecomp - earliestCompress, blockId]);

        const decompSecs = decompressTs.map(t => t - earliestCompress);
        broadcastTime66pPointsDt.push([tsBlock, percentile(decompSecs, 66), blockId]);
      } else {
        continue;
      }

      const originalSizes = new Set();
      const compressedSizes = new Set();
      for (const rec of recs) {
        const orig = rec.original_size;
        const comp = rec.compressed_size;
        if (orig != null && orig > 0) originalSizes.add(orig);
        if (comp != null && comp > 0) compressedSizes.add(comp);
      }
      if (originalSizes.size === 0 || compressedSizes.size === 0) continue;

      const originalSize = originalSizes.values().next().value;
      const compressedSize = compressedSizes.values().next().value;
      if (tsBlock === null) {
        continue;
      }
      blockSizePointsDt.push([tsBlock, originalSize, blockId]);
      blockSizeById[blockId] = originalSize;
      const compressionPercent = (originalSize - compressedSize) / originalSize;
      compressionPercentPointsDt.push([tsBlock, compressionPercent, blockId]);
    }

    statsByKey[key] = {
      type: group.type,
      called_from: group.called_from,
      num_blocks: numBlocks,
      block_size_points: shiftPoints(blockSizePointsDt),
      compression_percent_points: shiftPoints(compressionPercentPointsDt),
      broadcast_time_avg_points: shiftPoints(broadcastTimeAvgPointsDt),
      broadcast_time_full_points: shiftPoints(broadcastTimeFullPointsDt),
      broadcast_time_66p_points: shiftPoints(broadcastTime66pPointsDt),
      compression_time_points: shiftPoints(compressionTimePointsDt),
      decompression_time_points: shiftPoints(decompressionTimePointsDt),
      block_size_by_id: blockSizeById,
    };
  }

  return {
    experiment_name: payload.experiment_name || fallbackName || "",
    earliest_ts_global: formatLocalIso(earliestSec * 1000),
    type_called_from_stats: statsByKey,
  };
}

function buildPlotsData(experiments) {
  const unionMap = {};
  for (const exp of experiments) {
    const tos = exp.stats.type_called_from_stats || {};
    for (const [k, pair] of Object.entries(tos)) {
      const t = pair.type;
      const cf = pair.called_from;
      const label = (cf === null || cf === "None") ? `${t}` : `${t} (${cf})`;
      unionMap[k] = { type: t, called_from: cf, label };
    }
  }

  const unionKeys = Object.keys(unionMap).sort((a, b) => {
    const ta = String(unionMap[a].type || "");
    const tb = String(unionMap[b].type || "");
    if (ta !== tb) return ta.localeCompare(tb);
    const ca = String(unionMap[a].called_from || "");
    const cb = String(unionMap[b].called_from || "");
    return ca.localeCompare(cb);
  });

  const expShortNames = experiments.map(exp => exp.name);
  const plotsData = {};

  for (const k of unionKeys) {
    const meta = unionMap[k];
    const entry = {
      label: meta.label,
      block_size_by_id_series: [],
      block_size_series: [],
      compression_percent_series: [],
      broadcast_time_avg_series: [],
      broadcast_time_full_series: [],
      broadcast_time_66p_series: [],
      compression_time_series: [],
      decompression_time_series: [],
    };
    for (const exp of experiments) {
      const expName = exp.name;
      const baseTs = exp.stats.earliest_ts_global || null;
      const tos = exp.stats.type_called_from_stats || {};
      const pair = tos[k];
      if (!pair) {
        entry.block_size_by_id_series.push({});
        entry.block_size_series.push({ name: expName, base_ts: baseTs, points: [] });
        entry.compression_percent_series.push({ name: expName, base_ts: baseTs, points: [] });
        entry.broadcast_time_avg_series.push({ name: expName, base_ts: baseTs, points: [] });
        entry.broadcast_time_full_series.push({ name: expName, base_ts: baseTs, points: [] });
        entry.broadcast_time_66p_series.push({ name: expName, base_ts: baseTs, points: [] });
        entry.compression_time_series.push({ name: expName, base_ts: baseTs, points: [] });
        entry.decompression_time_series.push({ name: expName, base_ts: baseTs, points: [] });
        continue;
      }
      entry.block_size_by_id_series.push(pair.block_size_by_id || {});
      entry.block_size_series.push({ name: expName, base_ts: baseTs, points: pair.block_size_points || [] });
      entry.compression_percent_series.push({ name: expName, base_ts: baseTs, points: pair.compression_percent_points || [] });
      entry.broadcast_time_avg_series.push({ name: expName, base_ts: baseTs, points: pair.broadcast_time_avg_points || [] });
      entry.broadcast_time_full_series.push({ name: expName, base_ts: baseTs, points: pair.broadcast_time_full_points || [] });
      entry.broadcast_time_66p_series.push({ name: expName, base_ts: baseTs, points: pair.broadcast_time_66p_points || [] });
      entry.compression_time_series.push({ name: expName, base_ts: baseTs, points: pair.compression_time_points || [] });
      entry.decompression_time_series.push({ name: expName, base_ts: baseTs, points: pair.decompression_time_points || [] });
    }
    plotsData[k] = entry;
  }

  return { unionKeys, unionMap, expShortNames, plotsData };
}

function setTitle(experimentNames) {
  const title = "Stats Report - " + experimentNames.join(", ");
  document.title = title;
  const el = document.querySelector("h1");
  if (el) el.textContent = title;
}

function buildTabsAndContents() {
  const tabs = document.getElementById("tabs");
  if (!tabs) return;
  tabs.innerHTML = "";
  document.querySelectorAll(".tab-content").forEach(el => el.remove());

  const fragment = document.createDocumentFragment();

  for (const k of UNION_KEYS) {
    const label = UNION_MAP[k].label;
    const btn = document.createElement("button");
    btn.className = "tab-btn";
    btn.dataset.tab = k;
    btn.textContent = label;
    tabs.appendChild(btn);

    const div = document.createElement("div");
    div.className = "tab-content";
    div.id = `tab-${k}`;
    div.innerHTML = `
      <h3 style="margin:8px 0 16px 0">${label}</h3>
      <div id="${k}-broadcast_full" class="plot"></div>
      <div id="${k}-broadcast_66p" class="plot"></div>
      <div id="${k}-broadcast_avg" class="plot"></div>
      <div id="${k}-block_size" class="plot"></div>
      <div id="${k}-compression_percent" class="plot"></div>
      <div id="${k}-compression_time" class="plot"></div>
      <div id="${k}-decompression_time" class="plot"></div>
    `;
    fragment.appendChild(div);
  }

  tabs.parentNode.insertBefore(fragment, tabs.nextSibling);
}

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
      for (const [t] of (s.points || [])) {
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
        if (typeof size !== "number") continue;
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
  return (item.block_size_series || []).some(
    s => (s.points || []).some(p => typeof p[1] === "number" && p[1] >= minBlockSizeBytes),
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
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

function updateTimelineLabels() {
  const leftVal = USE_ABSOLUTE_TIME ? secsToTimestamp(rangeMinT) : formatDuration(rangeMinT);
  const rightVal = USE_ABSOLUTE_TIME ? secsToTimestamp(rangeMaxT) : formatDuration(rangeMaxT);
  document.getElementById("timeline-label-left").textContent = leftVal;
  document.getElementById("timeline-label-right").textContent = rightVal;
  document.getElementById("timeline-duration").textContent = formatDuration(rangeMaxT - rangeMinT);
}

function updateTimelineHandles() {
  const track = document.getElementById("timeline-track");
  const handleL = document.getElementById("timeline-handle-left");
  const handleR = document.getElementById("timeline-handle-right");
  const selection = document.getElementById("timeline-selection");

  const trackWidth = track.offsetWidth;
  const range = globalMaxT - globalMinT;
  if (range <= 0) return;

  const leftPct = (rangeMinT - globalMinT) / range;
  const rightPct = (rangeMaxT - globalMinT) / range;

  const leftPx = leftPct * trackWidth;
  const rightPx = rightPct * trackWidth;

  handleL.style.left = `${leftPx - 6}px`;
  handleR.style.left = `${rightPx - 6}px`;

  selection.style.left = `${leftPx}px`;
  selection.style.width = `${rightPx - leftPx}px`;

  updateTimelineLabels();
}

function setupTimelineDrag() {
  const track = document.getElementById("timeline-track");
  const handleL = document.getElementById("timeline-handle-left");
  const handleR = document.getElementById("timeline-handle-right");

  let dragging = null;

  function onMove(e) {
    if (!dragging) return;
    const rect = track.getBoundingClientRect();
    const x = (e.clientX || e.touches[0].clientX) - rect.left;
    const pct = Math.max(0, Math.min(1, x / rect.width));
    const t = globalMinT + pct * (globalMaxT - globalMinT);

    if (dragging === "left") {
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
    document.removeEventListener("mousemove", onMove);
    document.removeEventListener("mouseup", onEnd);
    document.removeEventListener("touchmove", onMove);
    document.removeEventListener("touchend", onEnd);
  }

  function startDrag(handle, e) {
    e.preventDefault();
    dragging = handle;
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onEnd);
    document.addEventListener("touchmove", onMove);
    document.addEventListener("touchend", onEnd);
  }

  handleL.addEventListener("mousedown", e => startDrag("left", e));
  handleL.addEventListener("touchstart", e => startDrag("left", e));
  handleR.addEventListener("mousedown", e => startDrag("right", e));
  handleR.addEventListener("touchstart", e => startDrag("right", e));

  track.addEventListener("click", e => {
    if (e.target === handleL || e.target === handleR) return;
    const rect = track.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const pct = x / rect.width;
    const t = globalMinT + pct * (globalMaxT - globalMinT);

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
    if (typeof size === "number" && size >= minBlockSizeBytes) {
      out.push(p);
    }
  }
  return out;
}

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
      for (const [t] of (s.points || [])) {
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
        if (typeof size !== "number") continue;
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
  return (item.block_size_series || []).some(
    s => (s.points || []).some(p => typeof p[1] === "number" && p[1] >= minBlockSizeBytes),
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
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

function updateTimelineLabels() {
  const leftVal = USE_ABSOLUTE_TIME ? secsToTimestamp(rangeMinT) : formatDuration(rangeMinT);
  const rightVal = USE_ABSOLUTE_TIME ? secsToTimestamp(rangeMaxT) : formatDuration(rangeMaxT);
  document.getElementById("timeline-label-left").textContent = leftVal;
  document.getElementById("timeline-label-right").textContent = rightVal;
  document.getElementById("timeline-duration").textContent = formatDuration(rangeMaxT - rangeMinT);
}

function updateTimelineHandles() {
  const track = document.getElementById("timeline-track");
  const handleL = document.getElementById("timeline-handle-left");
  const handleR = document.getElementById("timeline-handle-right");
  const selection = document.getElementById("timeline-selection");

  const trackWidth = track.offsetWidth;
  const range = globalMaxT - globalMinT;
  if (range <= 0) return;

  const leftPct = (rangeMinT - globalMinT) / range;
  const rightPct = (rangeMaxT - globalMinT) / range;

  const leftPx = leftPct * trackWidth;
  const rightPx = rightPct * trackWidth;

  handleL.style.left = `${leftPx - 6}px`;
  handleR.style.left = `${rightPx - 6}px`;

  selection.style.left = `${leftPx}px`;
  selection.style.width = `${rightPx - leftPx}px`;

  updateTimelineLabels();
}

function setupTimelineDrag() {
  const track = document.getElementById("timeline-track");
  const handleL = document.getElementById("timeline-handle-left");
  const handleR = document.getElementById("timeline-handle-right");

  let dragging = null;

  function onMove(e) {
    if (!dragging) return;
    const rect = track.getBoundingClientRect();
    const x = (e.clientX || e.touches[0].clientX) - rect.left;
    const pct = Math.max(0, Math.min(1, x / rect.width));
    const t = globalMinT + pct * (globalMaxT - globalMinT);

    if (dragging === "left") {
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
    document.removeEventListener("mousemove", onMove);
    document.removeEventListener("mouseup", onEnd);
    document.removeEventListener("touchmove", onMove);
    document.removeEventListener("touchend", onEnd);
  }

  function startDrag(handle, e) {
    e.preventDefault();
    dragging = handle;
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onEnd);
    document.addEventListener("touchmove", onMove);
    document.addEventListener("touchend", onEnd);
  }

  handleL.addEventListener("mousedown", e => startDrag("left", e));
  handleL.addEventListener("touchstart", e => startDrag("left", e));
  handleR.addEventListener("mousedown", e => startDrag("right", e));
  handleR.addEventListener("touchstart", e => startDrag("right", e));

  track.addEventListener("click", e => {
    if (e.target === handleL || e.target === handleR) return;
    const rect = track.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const pct = x / rect.width;
    const t = globalMinT + pct * (globalMaxT - globalMinT);

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
    if (typeof size === "number" && size >= minBlockSizeBytes) {
      out.push(p);
    }
  }
  return out;
}

function aggregatePoints(points, windowSec, blockSizeById) {
  if (!points || points.length === 0) return [];
  const sizeFiltered = filterByBlockSize(points, blockSizeById);
  if (sizeFiltered.length === 0) return [];
  const sorted = sizeFiltered.slice().sort((a, b) => a[0] - b[0]);

  const rangeFiltered = sorted.filter(([t]) => t >= rangeMinT && t <= rangeMaxT);
  if (rangeFiltered.length === 0) return [];

  let minT = rangeFiltered[0][0];
  let maxT = rangeFiltered[rangeFiltered.length - 1][0];
  let allowedMin = trimEdges ? (minT + 300) : minT;
  let allowedMax = trimEdges ? (maxT - 120) : maxT;
  if (allowedMax < allowedMin) return [];

  if (!windowSec || windowSec <= 0) {
    return rangeFiltered.filter(([t]) => t >= allowedMin && t <= allowedMax);
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
  const keys = Array.from(buckets.keys()).sort((a, b) => a - b);
  for (const k of keys) {
    const { sum: s, count: c, sumT: st } = buckets.get(k);
    const tAvg = st / c;
    out.push([tAvg, s / c]);
  }
  return out;
}

function computeAveragesForPair(pairData) {
  const blockSizeById = pairData.block_size_by_id || {};
  function avgFromPoints(pointsKey) {
    const pts = pairData[pointsKey] || [];
    const sizeFiltered = filterByBlockSize(pts, blockSizeById);
    const filtered = sizeFiltered.filter(([t]) => t >= rangeMinT && t <= rangeMaxT);
    if (filtered.length === 0) return 0;
    const sum = filtered.reduce((acc, [_, v]) => acc + v, 0);
    return sum / filtered.length;
  }

  function countFromPoints(pointsKey) {
    const pts = pairData[pointsKey] || [];
    const sizeFiltered = filterByBlockSize(pts, blockSizeById);
    return sizeFiltered.filter(([t]) => t >= rangeMinT && t <= rangeMaxT).length;
  }

  return {
    num_blocks: countFromPoints("block_size_points"),
    avg_block_size_bytes: avgFromPoints("block_size_points"),
    avg_compression_percent: avgFromPoints("compression_percent_points"),
    avg_broadcast_time_avg_s: avgFromPoints("broadcast_time_avg_points"),
    avg_broadcast_time_full_s: avgFromPoints("broadcast_time_full_points"),
    avg_broadcast_time_66p_s: avgFromPoints("broadcast_time_66p_points"),
    avg_compression_time_s: avgFromPoints("compression_time_points"),
    avg_decompression_time_s: avgFromPoints("decompression_time_points"),
  };
}

function rebuildAveragesTable() {
  const container = document.getElementById("averages-table-container");

  let html = '<table><thead><tr><th>Parameter</th>';
  const visibleKeys = getVisibleUnionKeys();
  if (visibleKeys.length === 0) {
    container.innerHTML = '<div style="color:#6b7280;">No blocks match the current size filter.</div>';
    return;
  }
  for (const k of visibleKeys) {
    const label = UNION_MAP[k].label;
    html += `<th class="group-start" colspan="${EXP_SHORT_NAMES.length}">${label}</th>`;
  }
  html += "</tr><tr><th></th>";
  for (const _ of visibleKeys) {
    for (let i = 0; i < EXP_SHORT_NAMES.length; i++) {
      const cls = i === 0 ? ' class="group-start"' : "";
      html += `<th${cls}>${EXP_SHORT_NAMES[i]}</th>`;
    }
  }
  html += "</tr></thead><tbody>";

  const allAvgs = {};
  for (const k of visibleKeys) {
    allAvgs[k] = [];
    const item = PLOTS_DATA[k];
    if (!item) {
      for (let i = 0; i < EXP_SHORT_NAMES.length; i++) allAvgs[k].push(null);
      continue;
    }
    for (let expIdx = 0; expIdx < EXP_SHORT_NAMES.length; expIdx++) {
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

  for (const [keyName, rowLabel] of ROWS_SPEC) {
    html += `<tr><td>${rowLabel}</td>`;

    for (const k of visibleKeys) {
      const groupVals = allAvgs[k].map(avg => avg ? avg[keyName] : null);

      let bestIdx = null;
      if (keyName === "avg_compression_percent") {
        let bestVal = null;
        for (let i = 0; i < groupVals.length; i++) {
          if (groupVals[i] != null && (bestVal === null || groupVals[i] > bestVal)) {
            bestVal = groupVals[i];
            bestIdx = i;
          }
        }
      } else if (["avg_broadcast_time_avg_s", "avg_broadcast_time_full_s", "avg_broadcast_time_66p_s"].includes(keyName)) {
        let bestVal = null;
        for (let i = 0; i < groupVals.length; i++) {
          if (groupVals[i] != null && groupVals[i] > 0 && (bestVal === null || groupVals[i] < bestVal)) {
            bestVal = groupVals[i];
            bestIdx = i;
          }
        }
      }

      for (let i = 0; i < EXP_SHORT_NAMES.length; i++) {
        let baseCls = i === 0 ? "group-start" : "";
        const val = groupVals[i];

        if (val == null || (keyName === "num_blocks" && val === 0)) {
          const clsAttr = baseCls ? ` class="${baseCls}"` : "";
          html += `<td${clsAttr}>-</td>`;
          continue;
        }

        let extraCls = "";
        if (bestIdx !== null) {
          if (i === bestIdx) extraCls = "best-cell";
          else extraCls = "other-cell";
        }
        const clsCombo = [baseCls, extraCls].filter(c => c).join(" ");
        const clsAttr = clsCombo ? ` class="${clsCombo}"` : "";

        let cell;
        if (keyName === "num_blocks") {
          cell = Math.round(val);
        } else if (keyName === "avg_block_size_bytes") {
          cell = Math.round(val / 1024) + " KB";
        } else if (keyName.includes("percent")) {
          cell = (val * 100).toFixed(2) + "%";
        } else if (["avg_broadcast_time_avg_s", "avg_broadcast_time_full_s", "avg_broadcast_time_66p_s"].includes(keyName)) {
          cell = val.toFixed(2) + " s";
        } else if (["avg_compression_time_s", "avg_decompression_time_s"].includes(keyName)) {
          cell = (val * 1000).toFixed(2) + " ms";
        } else {
          cell = val.toFixed(3);
        }
        html += `<td${clsAttr}>${cell}</td>`;
      }
    }
    html += "</tr>";
  }

  html += "</tbody></table>";
  container.innerHTML = html;
}

function rebuildAll() {
  rebuildAveragesTable();
  updateVisibleTabs();
  const active = document.querySelector(".tab-btn.active");
  if (active) {
    renderTab(active.dataset.tab);
  }
}

function secsToDate(baseTsStr, secs) {
  if (!baseTsStr) return new Date(secs * 1000);
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
    const x = USE_ABSOLUTE_TIME
      ? pts.map(p => secsToDate(s.base_ts, p[0]))
      : pts.map(p => p[0]);
    const y = pts.map(p => p[1]);
    traces.push({ x, y, type: "scatter", mode: "lines", name: s.name, line: { width: 2 } });
  }
  const layout = {
    title: title,
    margin: { l: 50, r: 20, t: 40, b: 60 },
    xaxis: USE_ABSOLUTE_TIME ? { title: xLabel, type: "date" } : { title: xLabel },
    yaxis: { title: yLabel, rangemode: "tozero" },
  };
  if (yTickFormat) { layout.yaxis.tickformat = yTickFormat; }
  Plotly.newPlot(divId, traces, layout, { displayModeBar: false, responsive: true });
}

function renderTab(key) {
  const item = PLOTS_DATA[key];
  if (!item) return;
  if (!hasBlocksAfterSizeFilter(item)) return;
  const blockSizeByIdSeries = item.block_size_by_id_series || [];
  plotSeriesMulti(`${key}-broadcast_full`, `${item.label} - Broadcast time (full)`, "time", "seconds", item.broadcast_time_full_series, blockSizeByIdSeries, ".2f");
  plotSeriesMulti(`${key}-broadcast_66p`, `${item.label} - Broadcast time (66p)`, "time", "seconds", item.broadcast_time_66p_series, blockSizeByIdSeries, ".2f");
  plotSeriesMulti(`${key}-broadcast_avg`, `${item.label} - Broadcast time (avg)`, "time", "seconds", item.broadcast_time_avg_series, blockSizeByIdSeries, ".2f");
  plotSeriesMulti(`${key}-block_size`, `${item.label} - Block size`, "time", "size (bytes)", item.block_size_series, blockSizeByIdSeries);
  plotSeriesMulti(`${key}-compression_percent`, `${item.label} - Compression %`, "time", "percent", item.compression_percent_series, blockSizeByIdSeries, ".2f", 100.0);
  plotSeriesMulti(`${key}-compression_time`, `${item.label} - Compression time`, "time", "milliseconds", item.compression_time_series, blockSizeByIdSeries, ".2f", 1000.0);
  plotSeriesMulti(`${key}-decompression_time`, `${item.label} - Decompression time`, "time", "milliseconds", item.decompression_time_series, blockSizeByIdSeries, ".2f", 1000.0);
}

function updateVisibleTabs() {
  const visibleKeys = new Set(getVisibleUnionKeys());
  let firstVisible = null;
  document.querySelectorAll(".tab-btn").forEach(btn => {
    const isVisible = visibleKeys.has(btn.dataset.tab);
    btn.style.display = isVisible ? "" : "none";
    if (isVisible && !firstVisible) firstVisible = btn.dataset.tab;
  });
  document.querySelectorAll(".tab-content").forEach(div => {
    const key = div.id.replace("tab-", "");
    const isVisible = visibleKeys.has(key);
    div.style.display = isVisible ? "" : "none";
  });
  const active = document.querySelector(".tab-btn.active");
  if (!active || !visibleKeys.has(active.dataset.tab)) {
    if (firstVisible) {
      setActiveTab(firstVisible);
      renderTab(firstVisible);
    }
  }
}

function setActiveTab(key) {
  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.tab === key);
  });
  document.querySelectorAll(".tab-content").forEach(div => {
    div.classList.toggle("active", div.id === "tab-" + key);
  });
}

function setupControls() {
  const smoothRange = document.getElementById("smooth-range");
  const smoothNumber = document.getElementById("smooth-number");
  const blockSizeRange = document.getElementById("block-size-range");
  const blockSizeNumber = document.getElementById("block-size-number");
  const trimCheckbox = document.getElementById("trim-edges");
  function applyAveragingChange(val) {
    const v = Math.max(0, Math.min(600, Number(val) || 0));
    averagingWindowSec = v;
    smoothRange.value = String(v);
    smoothNumber.value = String(v);
    const active = document.querySelector(".tab-btn.active");
    if (active) {
      const key = active.dataset.tab;
      renderTab(key);
    }
  }
  smoothRange.addEventListener("input", e => applyAveragingChange(e.target.value));
  smoothNumber.addEventListener("change", e => applyAveragingChange(e.target.value));
  function applyBlockSizeChange(val) {
    const maxKb = Math.max(0, Number(blockSizeRange.max) || 0);
    const v = Math.max(0, Math.min(maxKb, Number(val) || 0));
    minBlockSizeBytes = Math.round(v * 1024);
    blockSizeRange.value = String(v);
    blockSizeNumber.value = String(v);
    rebuildAll();
  }
  blockSizeRange.addEventListener("input", e => applyBlockSizeChange(e.target.value));
  blockSizeNumber.addEventListener("change", e => applyBlockSizeChange(e.target.value));
  trimCheckbox.addEventListener("change", e => {
    trimEdges = !!e.target.checked;
    const active = document.querySelector(".tab-btn.active");
    if (active) {
      const key = active.dataset.tab;
      renderTab(key);
    }
  });
}

function setupTabs() {
  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const key = btn.dataset.tab;
      setActiveTab(key);
      renderTab(key);
    });
  });
}

function initializeUI() {
  buildTabsAndContents();
  setupTabs();
  setupControls();

  USE_ABSOLUTE_TIME = EXP_SHORT_NAMES.length === 1;

  computeGlobalTimeRange();
  computeGlobalBlockSizeRange();
  const maxKb = Math.ceil(globalMaxBlockSize / 1024);
  const blockSizeRange = document.getElementById("block-size-range");
  const blockSizeNumber = document.getElementById("block-size-number");
  blockSizeRange.max = String(maxKb);
  blockSizeNumber.max = String(maxKb);
  blockSizeRange.value = "0";
  blockSizeNumber.value = "0";
  setupTimelineDrag();
  updateTimelineHandles();

  rebuildAveragesTable();

  const firstBtn = document.querySelector(".tab-btn");
  if (firstBtn) {
    const key = firstBtn.dataset.tab;
    setActiveTab(key);
    renderTab(key);
  }

  window.addEventListener("resize", () => {
    updateTimelineHandles();
  });
}

function getExperimentNamesFromQuery() {
  const params = new URLSearchParams(window.location.search);
  const listParam = params.get("experiments");
  const singleParam = params.get("experiment") || params.get("exp");
  if (listParam) {
    return listParam.split(",").map(s => s.trim()).filter(Boolean);
  }
  if (singleParam) return [singleParam.trim()];
  return [];
}

function getTimeRangeFromQuery() {
  const params = new URLSearchParams(window.location.search);
  const start = params.get("start");
  const end = params.get("end");
  if (start && end) {
    return { start, end };
  }
  return null;
}

function getDisplayNameFromQuery() {
  const params = new URLSearchParams(window.location.search);
  const name = params.get("name");
  if (!name) return "";
  return name.trim();
}

function trimLabel(value, maxLen = 15) {
  if (!value) return "";
  const trimmed = String(value).trim();
  return trimmed.length > maxLen ? trimmed.slice(0, maxLen) : trimmed;
}

function loadExperimentScript(name) {
  return new Promise((resolve, reject) => {
    if (window.__compressed_records && window.__compressed_records[name]) {
      resolve();
      return;
    }
    const script = document.createElement("script");
    script.src = `logs/${name}/records.js`;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`Failed to load logs/${name}/records.js`));
    document.head.appendChild(script);
  });
}

async function fetchServerPayload(range) {
  const url = `/get_benchmark_data?start=${encodeURIComponent(range.start)}&end=${encodeURIComponent(range.end)}`;
  const startMs = Date.parse(range.start);
  const endMs = Date.parse(range.end);
  const durationMs = Number.isFinite(startMs) && Number.isFinite(endMs) ? Math.max(0, endMs - startMs) : 0;
  const expectedPrepareMs = durationMs ? (durationMs / 3600000) * 15000 : 0;
  let prepareTimer = null;
  const prepareStart = performance.now();

  if (expectedPrepareMs > 0) {
    setStatus("Preparing... 0%", "loading");
    prepareTimer = setInterval(() => {
      const elapsed = performance.now() - prepareStart;
      const pct = Math.min(99, Math.floor((elapsed / expectedPrepareMs) * 100));
      setStatus(`Preparing... ${pct}%`, "loading");
    }, 500);
  } else {
    setStatus(`Preparing ${range.start} .. ${range.end}`, "loading");
  }

  const res = await fetch(url);
  if (prepareTimer) clearInterval(prepareTimer);
  if (!res.ok) {
    let detail = `Request failed: ${res.status} ${res.statusText}`;
    try {
      const err = await res.json();
      if (err && err.error) detail = err.error;
    } catch {
      // ignore JSON parse errors
    }
    throw new Error(detail);
  }
  const total = Number(res.headers.get("Content-Length")) || 0;
  const encoding = (res.headers.get("Content-Encoding") || "").toLowerCase();
  const uncompressed = Number(res.headers.get("X-Uncompressed-Length")) || 0;
  const canUsePercent = encoding === "gzip" ? uncompressed > 0 : total > 0;
  setStatus("Downloading...", "loading");

  if (!res.body) {
    const payload = await res.json();
    setStatus("", "");
    return payload;
  }

  const reader = res.body.getReader();
  const chunks = [];
  let received = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    received += value.length;
    if (canUsePercent) {
      const denom = encoding === "gzip" ? uncompressed : total;
      const pct = Math.min(100, Math.floor((received / denom) * 100));
      setStatus(`Downloading... ${pct}%`, "loading");
    } else {
      const mb = (received / (1024 * 1024)).toFixed(1);
      setStatus(`Downloading... ${mb} MB`, "loading");
    }
  }
  const merged = new Uint8Array(received);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  const text = new TextDecoder("utf-8").decode(merged);
  const payload = JSON.parse(text);
  setStatus("", "");
  return payload;
}

async function loadAndInit() {
  const range = getTimeRangeFromQuery();
  if (range) {
    const displayName = getDisplayNameFromQuery();
    let payload;
    try {
      payload = await fetchServerPayload(range);
    } catch (err) {
      setStatus(err && err.message ? err.message : "Failed to load server data", "error");
      return;
    }
    const expNameRaw = displayName || payload.experiment_name || `devnet ${range.start}..${range.end}`;
    const expName = trimLabel(expNameRaw, 15);
    const stats = computeStatsFromCompressed(payload, expName);
    const experiments = [{ name: expName, stats }];

    setTitle([expNameRaw]);
    const data = buildPlotsData(experiments);
    PLOTS_DATA = data.plotsData;
    UNION_KEYS = data.unionKeys;
    UNION_MAP = data.unionMap;
    EXP_SHORT_NAMES = data.expShortNames;

    initializeUI();
    return;
  }

  const names = getExperimentNamesFromQuery();
  if (!names.length) {
    setStatus("Provide start/end or experiment query parameters.", "error");
    return;
  }

  setStatus(`Loading local logs: ${names.join(", ")}`, "loading");
  try {
    await Promise.all(names.map(loadExperimentScript));
  } catch (err) {
    setStatus("Failed to load local records.js files.", "error");
    return;
  }

  setTitle(names);
  const payloadMap = window.__compressed_records || {};
  const experiments = names
    .map(name => {
      const payload = payloadMap[name];
      if (!payload) return null;
      const displayName = trimLabel(name, 15);
      const stats = computeStatsFromCompressed(payload, displayName);
      return { name: displayName, stats };
    })
    .filter(Boolean);
  if (!experiments.length) {
    setStatus("No local data found for requested experiments.", "error");
    return;
  }

  const data = buildPlotsData(experiments);
  PLOTS_DATA = data.plotsData;
  UNION_KEYS = data.unionKeys;
  UNION_MAP = data.unionMap;
  EXP_SHORT_NAMES = data.expShortNames;

  setStatus("", "");
  initializeUI();
}

window.addEventListener("DOMContentLoaded", () => {
  loadAndInit();
});
