@fused.udf
def udf():
    html = r'''<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Viewport → rowgroups → ERA5 rows → H3@5 map</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">

  <!-- Mapbox -->
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <!-- h3 (minimal - only for viewport polygon) -->
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>

  <!-- deck.gl -->
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/mapbox@9.1.3/dist.min.js"></script>

  <style>
    :root {
      --bg: #0f172a;
      --panel: rgba(15,23,42,.8);
      --txt: #e2e8f0;
      --accent: #38bdf8;
    }

    html, body {
      margin: 0;
      height: 100%;
      overflow: hidden;
      background: #000;
      color: #fff;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    body {
      display: flex;
      flex-direction: column;
    }

    #topbar {
      background: var(--panel);
      padding: 8px 10px;
      display: flex;
      gap: 1rem;
      align-items: center;
      z-index: 10;
      flex: 0 0 auto;
    }
    #path {
      width: 460px;
      background: rgba(15,23,42,.4);
      border: 1px solid rgba(148,163,184,.4);
      color: #e2e8f0;
      font-size: 12px;
      padding: 4px 6px;
      border-radius: 4px;
    }
    #status {
      font-size: 12px;
      opacity: .7;
      max-width: 38vw;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    #main {
      flex: 1 1 auto;
      display: flex;
      min-height: 0;
    }

    #map-wrap {
      position: relative;
      flex: 1;
    }
    #map {
      width: 100%;
      height: 100%;
      position: relative;
    }
    #map canvas.mapboxgl-canvas {
      position: absolute;
      top: 0;
      left: 0;
    }
    #map .deckgl-overlay,
    #map canvas.deckgl-canvas {
      position: absolute !important;
      top: 0 !important;
      left: 0 !important;
      width: 100% !important;
      height: 100% !important;
    }

    #overlay-pill {
      position: absolute;
      top: 10px;
      right: 10px;
      background: rgba(15,23,42,.85);
      border: 1px solid rgba(148,163,184,.2);
      border-radius: 6px;
      padding: 6px 8px;
      font-size: 11px;
      line-height: 1.35;
      pointer-events: none;
      min-width: 160px;
    }
    #tooltip {
      position: absolute;
      pointer-events: none;
      background: rgba(15,23,42,.96);
      color: #fff;
      padding: 4px 6px;
      border-radius: 4px;
      font-size: 11px;
      display: none;
      z-index: 90;
    }
    #sql-panel {
      flex: 0 0 auto;
      padding: 12px 16px;
      background: rgba(15,23,42,.92);
      border-top: 1px solid rgba(148,163,184,.18);
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    #sql-panel label {
      font-size: 11px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      opacity: 0.72;
    }
    #sql-input {
      width: 100%;
      min-height: 80px;
      max-height: 160px;
      padding: 8px;
      border-radius: 6px;
      border: 1px solid rgba(148,163,184,.25);
      background: rgba(30,41,59,.45);
      color: inherit;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 12px;
      resize: vertical;
    }
    #sql-status {
      font-size: 11px;
      opacity: 0.65;
      min-height: 14px;
    }
  </style>
</head>
<body>
  <div id="topbar">
    <div>
      <div style="font-size:11px;opacity:.7;">ERA5 parquet path</div>
      <input id="path" type="text" value="s3://fused-asset/data/era5/t2m_daily_mean_v2_10000/month=1950-05/0.parquet">
    </div>
    <div id="status">Init…</div>
  </div>
  <div id="main">
    <div id="map-wrap">
      <div id="map"></div>
      <div id="tooltip"></div>
      <div id="overlay-pill">
        <div><b>Viewport → H3@2 → RGs</b></div>
        <div id="summary-h3">hex2: –</div>
        <div id="summary-rg">rowgroups: –</div>
      </div>
    </div>
  </div>
  <div id="sql-panel">
    <label for="sql-input">DuckDB query (operates on `data` table with H3 functions available)</label>
    <textarea id="sql-input" spellcheck="false"></textarea>
    <div id="sql-status"></div>
  </div>

  <!-- FastTortoise -->
  <script src="https://fused-magic.s3.amazonaws.com/fasttortoise/v0.1.0/FastTortoiseWasm.js"></script>
  <script type="module">
    import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.31.0/+esm';

    // ================= CONFIG =================
    const MAPBOX_TOKEN = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA";
    const INDEX_URL_PARQUET = "https://unstable.udf.ai/fsh_2wpue89IZCvHI8owwStJDF/run?dataset=t2m_daily_mean_v2_10000&dtype_out_raster=png&dtype_out_vector=parquet";
    const META_URL = "https://unstable.fused.io/server/v1/file-metadata";
    const FILTER_RES = 2;
    const DISPLAY_RES = 7;
    const DEFAULT_CENTER = [-74.0060, 40.7128];
    const DEFAULT_ZOOM = 9;
    const DEFAULT_SQL = `
      SELECT h3_cell_to_parent(hex, 5) as hex,
             AVG(daily_mean) as avg,
             COUNT(*) as count
      FROM (select hexk.unnest as hex, daily_mean from data, UNNEST(h3_grid_disk(h3_cell_to_parent(hex,5), 1)) AS hexk)
      GROUP BY hex
    `;
    const SQL_DEBOUNCE_MS = 450;
    const CHANNEL = 'hexmap-bus';
    const DATASET = 'era5';
    const TIMESERIES_DEBOUNCE_MS = 0;
    const HOVER_TIMESERIES_DEBOUNCE_MS = 0;

    // ================= STATE =================
    let wasmModule, db, conn;
    let map;
    let indexByHex2Dec = new Map();
    let fileCounter = 0;
    let fetchInProgress = false;

    let overlay = null;
    let overlayReady = false;
    let tooltipAttached = false;
    let sqlInput = null;
    let sqlStatus = null;
    let sqlTypingTimer = null;
    let currentSQL = DEFAULT_SQL;
    let tableReady = false;
    let lastAggregatedData = [];
    const componentId = 'hexmap-' + Math.random().toString(36).slice(2);
    let bc = null;
    try {
      if ('BroadcastChannel' in window) {
        bc = new BroadcastChannel(CHANNEL);
      }
    } catch (_) {}
    let chartBroadcastTimer = null;
    let pendingTimeseriesRequest = false;
    let lastTimeseriesPayload = null;
    let hoverTimeseriesTimer = null;
    let currentHoverHex = null;
    let hoverTimeseriesCache = new Map();

    function busSend(obj) {
      const payload = JSON.stringify(obj);
      try { if (bc) bc.postMessage(obj); } catch (err) { console.warn('BroadcastChannel send error', err); }
      try { window.parent.postMessage(payload, '*'); } catch (_) {}
      try { if (window.top && window.top !== window.parent) window.top.postMessage(payload, '*'); } catch (_) {}
    }

    function sanitizeHex(hex) {
      if (hex == null) return null;
      const str = String(hex).trim().toUpperCase().replace(/[^0-9A-F]/g, '');
      return str || null;
    }

    function tryParseMessage(msg) {
      if (msg && typeof msg === 'object') return msg;
      if (typeof msg === 'string') {
        try { return JSON.parse(msg); } catch (_) { return null; }
      }
      return null;
    }

    function datasetMatches(msg) {
      if (!msg?.dataset) return true;
      if (msg.dataset === 'all') return true;
      return msg.dataset === DATASET;
    }

    function handleIncomingMessage(raw) {
      const msg = tryParseMessage(raw);
      if (!msg || !datasetMatches(msg)) return;
      if (msg.fromComponent && msg.fromComponent === componentId) return;
      if (msg.type === 'hexmap_request_timeseries') {
        if (lastTimeseriesPayload && !fetchInProgress) {
          busSend(lastTimeseriesPayload);
          if (tableReady) {
            scheduleChartBroadcast(0);
          }
        } else {
          pendingTimeseriesRequest = true;
          scheduleChartBroadcast(0);
        }
      }
    }

    if (bc) {
      bc.onmessage = ev => handleIncomingMessage(ev.data);
    }
    window.addEventListener('message', ev => handleIncomingMessage(ev.data));

    function currentBoundsMeta() {
      if (!map || typeof map.getBounds !== 'function') return null;
      const b = map.getBounds();
      if (!b) return null;
      const west = b.getWest();
      const south = b.getSouth();
      const east = b.getEast();
      const north = b.getNorth();
      if (![west, south, east, north].every(Number.isFinite)) return null;
      return { west, south, east, north, zoom: map.getZoom?.() }; 
    }

    function broadcastTimeseries(rows, meta = {}) {
      pendingTimeseriesRequest = false;
      const payload = {
        type: 'hexmap_timeseries',
        dataset: DATASET,
        fromComponent: componentId,
        timestamp: Date.now(),
        data: rows,
        meta
      };
      lastTimeseriesPayload = payload;
      busSend(payload);
    }

    function broadcastTimeseriesEmpty(reason) {
      const bounds = currentBoundsMeta();
      const payload = {
        type: 'hexmap_timeseries',
        dataset: DATASET,
        fromComponent: componentId,
        timestamp: Date.now(),
        data: [],
        meta: { reason, bounds }
      };
      lastTimeseriesPayload = payload;
      busSend(payload);
    }

    function broadcastHoverTimeseries(hexStr, rows, reason) {
      const normalizedHex = sanitizeHex(hexStr);
      if (normalizedHex === null && !rows.length && reason === 'hover_off') {
        // avoid spamming identical empty off events
      }
      const payload = {
        type: 'hexmap_hover_timeseries',
        dataset: DATASET,
        fromComponent: componentId,
        timestamp: Date.now(),
        data: Array.isArray(rows) ? rows : [],
        meta: { hex: normalizedHex, reason }
      };
      console.log('🟡 Broadcasting hover timeseries:', { hex: normalizedHex, rowCount: rows.length, reason });
      busSend(payload);
    }

     function clearHoverTimeseries(sendMessage = true) {
      if (hoverTimeseriesTimer) {
        clearTimeout(hoverTimeseriesTimer);
        hoverTimeseriesTimer = null;
      }
      if (currentHoverHex === null) {
        if (sendMessage) {
          broadcastHoverTimeseries(null, [], 'hover_off');
        }
        return;
      }
      const prevHex = currentHoverHex;
      currentHoverHex = null;
      if (sendMessage) {
        broadcastHoverTimeseries(prevHex, [], 'hover_off');
      }
    }

    function handleHexHover(nextHex) {
      const sanitized = sanitizeHex(nextHex);
      if (!sanitized) {
        clearHoverTimeseries(true);
        return;
      }
      if (sanitized === currentHoverHex) return;
      currentHoverHex = sanitized;
      if (hoverTimeseriesTimer) {
        clearTimeout(hoverTimeseriesTimer);
      }
      hoverTimeseriesTimer = setTimeout(() => {
        fetchHoverTimeseries(currentHoverHex).catch(err => console.error('hover timeseries error', err));
      }, HOVER_TIMESERIES_DEBOUNCE_MS);
    }

    async function fetchHoverTimeseries(hexStr) {
      if (!hexStr) return;
      const sanitized = sanitizeHex(hexStr);
      if (!sanitized) return;

      const cached = hoverTimeseriesCache.get(sanitized);
      if (cached) {
        if (currentHoverHex === sanitized) {
          broadcastHoverTimeseries(sanitized, cached, cached.length ? 'ready' : 'no_data');
        }
        return;
      }

      if (!conn || !tableReady) {
        broadcastHoverTimeseries(sanitized, [], 'no_data');
        return;
      }

      broadcastHoverTimeseries(sanitized, [], 'no_data');
    }

    async function precomputeHoverTimeseriesForViewport() {
      if (!conn || !tableReady) {
        return;
      }

      try {
        const result = await conn.query(`
          WITH normalized AS (
            SELECT 
              datestr,
              daily_mean,
              CASE
                WHEN typeof(hex) IN ('BIGINT','UBIGINT','HUGEINT') THEN CAST(hex AS UBIGINT)
                WHEN typeof(hex) IN ('VARCHAR','TEXT') THEN h3_string_to_h3(CAST(hex AS VARCHAR))
                WHEN typeof(hex) = 'BLOB' THEN h3_string_to_h3(CAST(hex AS VARCHAR))
                ELSE CAST(hex AS UBIGINT)
              END AS hex_id
            FROM data
            WHERE datestr IS NOT NULL
              AND daily_mean IS NOT NULL
          ),
          parents AS (
            SELECT 
              datestr,
              daily_mean,
              h3_cell_to_parent(hex_id, 5) AS parent_hex
            FROM normalized
          ),
          exploded AS (
            SELECT 
              datestr,
              daily_mean,
              disks.unnest AS neighbor_hex
            FROM parents,
              UNNEST(h3_grid_disk(parent_hex, 1)) AS disks
          )
          SELECT 
            h3_h3_to_string(neighbor_hex) AS hex,
            datestr,
            MAX(daily_mean) AS max_temp
          FROM exploded
          GROUP BY hex, datestr
        `);

        const newCache = new Map();
        for (const row of result.toArray()) {
          const hexStr = sanitizeHex(row.hex);
          if (!hexStr) continue;
          const dateVal = row.datestr != null ? row.datestr : null;
          const maxVal = Number(row.max_temp);
          if (dateVal == null || !Number.isFinite(maxVal)) continue;
          const celsius = maxVal - 273.15;
          const entry = { date: dateVal, max: Number(celsius.toFixed(3)) };
          if (!newCache.has(hexStr)) newCache.set(hexStr, []);
          newCache.get(hexStr).push(entry);
        }

        for (const entries of newCache.values()) {
          entries.sort((a, b) => {
            const aNum = Number(a.date);
            const bNum = Number(b.date);
            if (Number.isFinite(aNum) && Number.isFinite(bNum)) {
              return aNum - bNum;
            }
            return String(a.date).localeCompare(String(b.date));
          });
        }
        const merged = new Map(hoverTimeseriesCache);
        for (const [hexStr, entries] of newCache.entries()) {
          merged.set(hexStr, entries);
        }
        hoverTimeseriesCache = merged;
      } catch (err) {
        console.error('precomputeHoverTimeseries error', err);
      }
    }

    function scheduleChartBroadcast(delay = TIMESERIES_DEBOUNCE_MS) {
      if (!conn || fetchInProgress || !tableReady) {
        pendingTimeseriesRequest = true;
        return;
      }
      pendingTimeseriesRequest = false;
      if (chartBroadcastTimer) clearTimeout(chartBroadcastTimer);
      chartBroadcastTimer = setTimeout(() => {
        chartBroadcastTimer = null;
        broadcastChartData().catch(err => console.error('broadcastChartData error', err));
      }, Math.max(0, delay));
    }

    async function broadcastChartData() {
      if (!conn || !tableReady) return;
      const bounds = currentBoundsMeta();
      if (!bounds) return;
      const { west, south, east, north } = bounds;
      try {
        const result = await conn.query(`
          WITH normalized AS (
            SELECT 
              *,
              CASE
                WHEN typeof(hex) IN ('VARCHAR','TEXT') THEN h3_string_to_h3(CAST(hex AS VARCHAR))
                WHEN typeof(hex) IN ('BLOB') THEN h3_string_to_h3(CAST(hex AS VARCHAR))
                ELSE CAST(hex AS UBIGINT)
              END AS hex_id
            FROM data
          ),
          cell_coords AS (
            SELECT 
              *,
              h3_cell_to_lat(hex_id) as lat,
              h3_cell_to_lng(hex_id) as lng
            FROM normalized
          )
          SELECT 
            datestr,
            AVG(daily_mean) as avg_temp,
            MIN(daily_mean) as min_temp,
            MAX(daily_mean) as max_temp,
            COUNT(*) as samples
          FROM cell_coords
          WHERE daily_mean IS NOT NULL 
            AND datestr IS NOT NULL
            AND lat BETWEEN ${south} AND ${north}
            AND lng BETWEEN ${west} AND ${east}
          GROUP BY datestr
          ORDER BY datestr
        `);
        const rows = result.toArray().map(r => {
          const dateVal = r.datestr != null ? Number(r.datestr) : null;
          const avgVal = Number(r.avg_temp);
          const minVal = Number(r.min_temp);
          const maxVal = Number(r.max_temp);
          const sampleVal = Number(r.samples ?? 0);
          if (dateVal == null || !Number.isFinite(avgVal)) return null;
          const avgC = avgVal - 273.15;
          const minC = Number.isFinite(minVal) ? minVal - 273.15 : null;
          const maxC = Number.isFinite(maxVal) ? maxVal - 273.15 : null;
          const avgOut = Number(avgC.toFixed(3));
          const minOut = minC == null ? null : Number(minC.toFixed(3));
          const maxOut = maxC == null ? null : Number(maxC.toFixed(3));
          return {
            date: dateVal,
            avg: avgOut,
            min: minOut,
            max: maxOut,
            samples: Number.isFinite(sampleVal) ? sampleVal : null
          };
        }).filter(Boolean);
        broadcastTimeseries(rows, { bounds, sql: currentSQL, rowCount: rows.length });
      } catch (err) {
        console.error('broadcastChartData error', err);
      }
    }
    
    // Cache for rowgroup data
    let rowgroupCache = new Map(); // key: rowgroup_id, value: { fileName, rows }
    let loadedRowgroups = new Set(); // track which rowgroups are in the current 'data' table

    async function init() {
      const status = document.getElementById('status');
      status.textContent = 'Loading FastTortoise…';
      
      wasmModule = await FastTortoiseModule({ print: console.log, printErr: console.error });

      status.textContent = 'Loading DuckDB…';
      const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
      const workerBlob = await (await fetch(bundle.mainWorker)).blob();
      const worker = new Worker(URL.createObjectURL(workerBlob));
      db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
      await db.instantiate(bundle.mainModule);
      conn = await db.connect();
      
              status.textContent = 'Installing H3 extension…';
      try {
        await conn.query("INSTALL h3 FROM community");
        await conn.query('LOAD h3');
        
        // Test H3 functions are available
        const testResult = await conn.query("SELECT h3_h3_to_string(CAST(599686042433355775 AS UBIGINT)) as test");
        console.log('H3 extension test:', testResult.toArray());
        
        status.textContent = 'H3 extension loaded';
      } catch (err) {
        console.error('H3 extension error:', err);
        status.textContent = 'Warning: H3 extension failed to load';
      }

      setupMap();
      await loadIndex();

      sqlInput = document.getElementById('sql-input');
      sqlStatus = document.getElementById('sql-status');
      if (sqlInput) {
        sqlInput.value = DEFAULT_SQL.trim();
        sqlInput.addEventListener('input', onSqlInput);
      }
      setSqlStatus('Awaiting data…');

      status.textContent = 'Ready — pan or zoom the map to load data.';
    }

    function setupMap() {
      mapboxgl.accessToken = MAPBOX_TOKEN;
      map = new mapboxgl.Map({
        container: 'map',
        style: 'mapbox://styles/mapbox/dark-v10',
        center: DEFAULT_CENTER,
        zoom: DEFAULT_ZOOM
      });

      map.on('load', () => {
        overlayReady = true;
        // Auto-fetch on initial load
        fetchViewportData();
      });
      
      // Auto-fetch when map movement ends
      map.on('moveend', () => {
        fetchViewportData();
      });
      
      map.on('move', () => {
        scheduleChartBroadcast(0);
      });
    }

    async function loadIndex() {
      const status = document.getElementById('status');
      status.textContent = 'Loading hex2→rowgroup index…';
      const res = await fetch(INDEX_URL_PARQUET);
      const ab = await res.arrayBuffer();
      const u8 = new Uint8Array(ab);
      await db.registerFileBuffer('hex2_index.parquet', u8);
      const qres = await conn.query(`SELECT * FROM read_parquet('hex2_index.parquet')`);
      const arr = qres.toArray();

      const mapX = new Map();
      for (const r of arr) {
        let hex2 = r.hex2;
        let rg = r.rowgroup_id;
        if (typeof hex2 === 'bigint') hex2 = hex2.toString();
        if (typeof rg === 'bigint') rg = Number(rg);
        if (!hex2 || rg == null) continue;
        const key = String(hex2);
        if (!mapX.has(key)) mapX.set(key, new Set());
        mapX.get(key).add(rg);
      }
      indexByHex2Dec = mapX;
      status.textContent = `Index ready (${indexByHex2Dec.size} cells)`;
    }

    function bboxToHex2Decimal() {
      const b = map.getBounds();
      const poly = [[
        [b.getWest(), b.getSouth()],
        [b.getWest(), b.getNorth()],
        [b.getEast(), b.getNorth()],
        [b.getEast(), b.getSouth()],
        [b.getWest(), b.getSouth()]
      ]];
      
      // Step 1: Get viewport at resolution 5
      const res5Cells = h3.polygonToCells(poly, 5, true);
      
      // Step 2: Expand each res-5 cell by 1 k-ring (includes center + neighbors)
      const expandedRes5 = new Set();
      for (const cell of res5Cells) {
        const ring = h3.gridDisk(cell, 3); // k=1 includes the cell itself + immediate neighbors
        ring.forEach(c => expandedRes5.add(c));
      }
      
      // Step 3: Convert to parent resolution 2
      const res2Parents = new Set();
      for (const cell of expandedRes5) {
        const parent = h3.cellToParent(cell, FILTER_RES);
        res2Parents.add(parent);
      }
      
      // Step 4: Return as decimal strings for matching
      return Array.from(res2Parents).map(h => BigInt('0x' + h).toString());
    }

    async function readRowGroupWithRetry(path, rowgroupId, maxAttempts = 5) {
      const bufferPtr = wasmModule._malloc(4);
      const sizePtr  = wasmModule._malloc(8);
      try {
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          const rc = await wasmModule.ccall(
            'read_parquet_row_group',
            'number',
            ['string','string','number','number','number'],
            [path, META_URL, rowgroupId, bufferPtr, sizePtr],
            { async: true }
          );

          if (rc === 0) {
            const dataPtr = wasmModule.getValue(bufferPtr, '*');
            const dataSize = Number(wasmModule.getValue(sizePtr, 'i64'));
            return { dataPtr, dataSize };
          }

          const err = wasmModule.ccall('get_last_error', 'string', [], []) || '';
          if (!err.includes('Metadata is still being processed')) {
            throw new Error(err || 'FastTortoise error');
          }
        }
        throw new Error('FastTortoise: metadata still busy after retries');
      } finally {
        wasmModule._free(bufferPtr);
        wasmModule._free(sizePtr);
      }
    }

    async function fetchViewportData() {
      if (fetchInProgress) return;
      fetchInProgress = true;

      const status = document.getElementById('status');
      const path = document.getElementById('path').value.trim();

      try {
        tableReady = false;
        lastAggregatedData = [];
        setSqlStatus('Fetching viewport rows…');
        broadcastTimeseriesEmpty('fetching');
        clearHoverTimeseries(true);
        
        // Check zoom level
        const currentZoom = map.getZoom();
        if (currentZoom < 4.2) {
          status.textContent = 'Please zoom in more (zoom level 8+)';
          setSqlStatus('Zoom in to fetch data.');
          renderMapFromData([]);
          broadcastTimeseriesEmpty('zoom_too_low');
          clearHoverTimeseries(true);
          return;
        }
        
        if (!indexByHex2Dec.size) {
          status.textContent = 'Index empty';
          renderMapFromData([]);
          tableReady = false;
          setSqlStatus('Index not loaded.');
          broadcastTimeseriesEmpty('index_empty');
          clearHoverTimeseries(true);
          return;
        }

        status.textContent = 'Computing viewport H3@2…';
        const viewportDecs = bboxToHex2Decimal();
        const viewportSet = new Set(viewportDecs);
        document.getElementById('summary-h3').textContent = `hex2: ${viewportDecs.length}`;

        const neededRowgroups = new Set();
        for (const d of viewportDecs) {
          const s = indexByHex2Dec.get(d);
          if (s) s.forEach(rg => neededRowgroups.add(rg));
        }
        const rowgroups = Array.from(neededRowgroups).sort((a,b) => a - b);
        document.getElementById('summary-rg').textContent = `rowgroups: ${rowgroups.length}`;

        if (!rowgroups.length) {
          status.textContent = 'No rowgroups for this viewport';
          renderMapFromData([]);
          broadcastTimeseriesEmpty('no_rowgroups');
          clearHoverTimeseries(true);
          return;
        }

        // Check which rowgroups need to be fetched (not in cache)
        const uncachedRowgroups = rowgroups.filter(rg => !rowgroupCache.has(rg));
        const cachedRowgroups = rowgroups.filter(rg => rowgroupCache.has(rg));
        
        if (uncachedRowgroups.length > 0) {
          status.textContent = `Loading ${uncachedRowgroups.length} new rowgroups (${cachedRowgroups.length} cached)…`;
        } else {
          status.textContent = `Using ${cachedRowgroups.length} cached rowgroups…`;
        }
        
        // Fetch uncached rowgroups
        for (const rg of uncachedRowgroups) {
          const { dataPtr, dataSize } = await readRowGroupWithRetry(path, rg);
          const fileBuffer = new Uint8Array(dataSize);
          fileBuffer.set(new Uint8Array(wasmModule.HEAPU8.buffer, dataPtr, dataSize));
          wasmModule._free(dataPtr);

          fileCounter++;
          const fileName = `rg_${fileCounter}.parquet`;
          await db.registerFileBuffer(fileName, fileBuffer);
          
          // Store in cache
          rowgroupCache.set(rg, { fileName });
        }
        
        // Drop and recreate the data table
        await conn.query('DROP TABLE IF EXISTS data');
        
        // Collect all table names (cached + newly loaded)
        const tableNames = rowgroups.map(rg => rowgroupCache.get(rg).fileName);
        loadedRowgroups = new Set(rowgroups);

        // Create unified table from all parquet files and filter by viewport in DuckDB
        status.textContent = 'Filtering by viewport in DuckDB…';
        const viewportHexList = viewportDecs.map(d => {
          try {
            return "'" + BigInt(d).toString(16) + "'";
          } catch (e) {
            return null;
          }
        }).filter(Boolean).join(',');

        const unionQuery = tableNames.map(fn => 
          `SELECT * FROM read_parquet('${fn}')`
        ).join(' UNION ALL ');

        // normalize hex to a proper hex string first
        const hexNormExpr = `
          CASE
            WHEN typeof(hex) IN ('BIGINT','UBIGINT','HUGEINT') THEN printf('%x', hex)
            ELSE CAST(hex AS VARCHAR)
          END
        `;
        
        await conn.query(`
          CREATE TABLE data AS
          WITH src AS (${unionQuery})
          SELECT *
          FROM src
          WHERE h3_h3_to_string(
            h3_cell_to_parent(
              h3_string_to_h3(${hexNormExpr}),
              ${FILTER_RES}
            )
          ) IN (${viewportHexList})
        `);


        const countRes = await conn.query('SELECT COUNT(*) as cnt FROM data');
        const totalRows = countRes.toArray()[0].cnt;
        status.textContent = `Loaded ${totalRows} rows in DuckDB`;

        tableReady = true;
        setSqlStatus(`Data table ready (${totalRows.toLocaleString()} rows)`);
        await runSqlAndRender();
        await precomputeHoverTimeseriesForViewport();
        scheduleChartBroadcast(0);

      } catch (e) {
        console.error(e);
        status.textContent = 'Error: ' + e.message;
        setSqlStatus('Error fetching data.');
        broadcastTimeseriesEmpty('error');
        clearHoverTimeseries(true);
      } finally {
        fetchInProgress = false;
        if (pendingTimeseriesRequest && tableReady) {
          scheduleChartBroadcast(0);
        }
      }
    }

    function renderMapFromData(data) {
      if (!overlayReady) {
        if (map) {
          setTimeout(() => renderMapFromData(data), 100);
        }
        return;
      }
      
      if (!data || data.length === 0) {
        if (overlay) overlay.setProps({ layers: [] });
        return;
      }

      let minVal = Infinity, maxVal = -Infinity;

      for (const d of data) {
        const value = Number(d.avg ?? d.value ?? d.metric ?? d.daily_mean);
        if (Number.isFinite(value)) {
          if (value < minVal) minVal = value;
          if (value > maxVal) maxVal = value;
        }
      }

      if (!Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
        minVal = 0;
        maxVal = 1;
      }
      if (minVal === maxVal) maxVal = minVal + 1e-6;

      const layer = new deck.H3HexagonLayer({
        id: 'era5-layer',
        data,
        pickable: true,
        filled: true,
        extruded: false,
        coverage: 0.95,
        stroked: false,
        getHexagon: d => d.hex,
        getFillColor: d => {
          const value = Number(d.avg ?? d.value ?? d.metric ?? d.daily_mean);
          if (!Number.isFinite(value)) return [120, 120, 120, 160];
          const t = (value - minVal) / (maxVal - minVal);
          const r = Math.round(255 * t);
          const b = Math.round(255 * (1 - t));
          return [r, 125, b, 220];
        }
      });

      if (!overlay) {
        overlay = new deck.MapboxOverlay({
          interleaved: false,
          layers: [layer]
        });
        map.addControl(overlay);
      } else {
        overlay.setProps({ layers: [layer] });
      }
      
      if (!tooltipAttached) {
        const tt = document.getElementById('tooltip');
        map.on('mousemove', e => {
          if (!overlay || !overlayReady) return;
          if (!e?.point || !Number.isFinite(e.point.x) || !Number.isFinite(e.point.y)) return;

          let info = null;
          try {
            info = overlay.pickObject({ x: e.point.x, y: e.point.y, radius: 5 });
          } catch (err) {
            tt.style.display = 'none';
            map.getCanvas().style.cursor = '';
            return;
          }

          if (info && info.object) {
            const o = info.object;
            const avgVal = Number(o.avg ?? o.value ?? o.metric ?? o.daily_mean);
            tt.innerHTML = `
              <div><b>${o.hex}</b></div>
              <div>avg: ${Number.isFinite(avgVal) ? avgVal.toFixed(2) : 'n/a'}</div>
              <div>count: ${o.count ?? 'n/a'}</div>
            `;
            tt.style.left = (e.point.x + 8) + 'px';
            tt.style.top = (e.point.y + 8) + 'px';
            tt.style.display = 'block';
            map.getCanvas().style.cursor = 'pointer';
            handleHexHover(o.hex);
          } else {
            tt.style.display = 'none';
            map.getCanvas().style.cursor = '';
            clearHoverTimeseries(true);
          }
        });
        tooltipAttached = true;
      }

      map.triggerRepaint();
    }

    function sanitizeSQL(sql) {
      if (sql == null) return '';
      return String(sql).trim().replace(/;\s*$/g, '');
    }

    function setSqlStatus(text) {
      if (sqlStatus) {
        sqlStatus.textContent = text || '';
      }
    }

    function onSqlInput() {
      if (!sqlInput) return;
      currentSQL = sanitizeSQL(sqlInput.value) || DEFAULT_SQL;
      if (sqlTypingTimer) clearTimeout(sqlTypingTimer);
      sqlTypingTimer = setTimeout(() => {
        runSqlAndRender().catch(err => console.error('SQL run error', err));
      }, SQL_DEBOUNCE_MS);
    }

    async function runSqlAndRender() {
      if (!conn || !tableReady) {
        setSqlStatus('Table not ready yet.');
        return;
      }
      clearHoverTimeseries(false);
      
      const sqlText = sanitizeSQL(sqlInput?.value) || DEFAULT_SQL;
      currentSQL = sqlText;
      setSqlStatus('Running query…');
      
      try {
        const result = await conn.query(sqlText);
        const rows = result.toArray();
        
        // Convert DuckDB results to map format
        const mapData = [];
        for (const row of rows) {
          let hexStr = row.hex;
          if (!hexStr) continue;
          
          // Convert to string if needed
          if (typeof hexStr === 'bigint') {
            hexStr = hexStr.toString(16);
          } else {
            hexStr = String(hexStr);
          }
          
          // Remove 0x prefix if present
          hexStr = hexStr.replace(/^0x/i, '');
          
          // Get lat/lng from H3 (using JS h3 library for rendering only)
          let lat, lng;
          try {
            [lat, lng] = h3.cellToLatLng(hexStr);
          } catch (e) {
            console.warn('Invalid hex:', hexStr);
            continue;
          }
          
          const avg = Number(row.avg ?? row.value ?? row.metric ?? row.daily_mean);
          const count = Number(row.count ?? row.cnt ?? 1);
          
          mapData.push({ hex: hexStr, avg, count, lat, lng });
        }
        
        console.log('[SQL Result] Rows:', rows.length, 'Valid map data:', mapData.length);
        
        if (mapData.length > 0) {
          lastAggregatedData = mapData;
          renderMapFromData(mapData);
          setSqlStatus(`Query returned ${mapData.length.toLocaleString()} hexagons.`);
        } else {
          renderMapFromData([]);
          setSqlStatus('Query returned 0 valid hexagons.');
        }
        scheduleChartBroadcast(0);
      } catch (err) {
        console.error('DuckDB SQL error', err);
        setSqlStatus(`SQL error: ${err.message || err}`);
        renderMapFromData([]);
        broadcastTimeseriesEmpty('sql_error');
      }
    }

    init();
  </script>
</body>
</html>
'''
    return html