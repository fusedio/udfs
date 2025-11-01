@fused.udf
def udf():
    html = r'''<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Viewport → rowgroups → ERA5 rows → H3@5 map + chart</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">

  <!-- Mapbox -->
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <!-- h3 -->
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>

  <!-- deck.gl -->
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/mapbox@9.1.3/dist.min.js"></script>

  <!-- Chart.js -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>

  <style>
    :root {
      --bg: #0f172a;
      --panel: rgba(15,23,42,.8);
      --txt: #e2e8f0;
      --accent: #38bdf8;
    }

    /* ✱ CHANGED: make the page fit the iframe and hide the page scroll */
    html, body {
      margin: 0;
      height: 100%;
      overflow: hidden; /* only inner parts should scroll */
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
    #fetch-btn {
      background: var(--accent);
      color: #000;
      font-size: 12px;
      border: none;
      border-radius: 4px;
      padding: 4px 8px;
      cursor: pointer;
    }
    #fetch-btn[disabled] {
      opacity: .5;
      cursor: not-allowed;
    }
    #status {
      font-size: 12px;
      opacity: .7;
      max-width: 38vw;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    /* ✱ CHANGED: #main must only take what's left, not 100vh */
    #main {
      flex: 1 1 auto;
      display: grid;
      grid-template-columns: 360px 1fr;
      /* fit inside body (which is 100%) */
      min-height: 0; /* allow children to shrink */
    }

    #left {
      background: rgba(15,23,42,.2);
      border-right: 1px solid rgba(148,163,184,.05);
      display: flex;
      flex-direction: column;
      min-height: 0; /* ✱ CHANGED: so #results can scroll */
    }
    #chart-wrap {
      padding: 6px 8px 2px 8px;
      background: rgba(15,23,42,.25);
      flex: 0 0 auto;
    }
    #chart {
      width: 100%;
      height: 200px;
    }

    /* ✱ CHANGED: this is the thing you want scrollable */
    #results {
      flex: 1 1 auto;
      min-height: 0;
      overflow-y: auto;
      overflow-x: hidden;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 11px;
      white-space: pre;
      padding: 6px 8px 26px 8px;
      background: rgba(15,23,42,.02);
    }

    #map-wrap {
      position: relative;
      min-height: 0;
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
    <button id="fetch-btn">Fetch Viewport Data</button>
    <div id="status">Init…</div>
  </div>
  <div id="main">
    <div id="left">
      <div id="chart-wrap">
        <canvas id="chart"></canvas>
      </div>
      <div id="results">Waiting for user to fetch…</div>
    </div>
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
    <label for="sql-input">DuckDB query (default: SELECT * FROM data)</label>
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
    const DEFAULT_CENTER = [-74.0060, 40.7128]; // NYC
    const DEFAULT_ZOOM = 9;
    const RAW_TABLE_NAME = 'viewport_rows_raw';
    const DEFAULT_SQL = 'SELECT hexk.unnest as hex, avg(daily_mean) daily_mean FROM data, UNNEST(h3_grid_disk(h3_cell_to_parent(hex,6), 3)) AS hexk group by 1';
    const SQL_DEBOUNCE_MS = 450;

    // ================= STATE =================
    let wasmModule, db, conn;
    let map, chart;
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

    function normalizeRawHex(raw) {
      if (raw == null) return null;
      let cell = String(raw).trim();
      if (!cell) return null;
      if (cell.startsWith('0x') || cell.startsWith('0X')) {
        cell = cell.slice(2);
      }
      if (/^[0-9]+$/.test(cell)) {
        try {
          cell = BigInt(cell).toString(16);
        } catch (err) {
          return null;
        }
      } else {
        cell = cell.toLowerCase();
      }
      try {
        if (h3.isValidCell?.(cell)) return cell;
      } catch (_) {}
      return null;
    }

    function toSqlNumber(value) {
      const n = Number(value);
      return Number.isFinite(n) ? String(n) : 'NULL';
    }

    function toSqlString(value) {
      if (value == null) return 'NULL';
      return `'${String(value).replace(/'/g, "''")}'`;
    }

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
      try {
        await conn.query("INSTALL h3 FROM community");
      } catch (err) {
        console.warn('INSTALL h3 failed (continuing):', err?.message || err);
      }
      try {
        await conn.query('LOAD h3');
      } catch (err) {
        console.warn('LOAD h3 failed (continuing):', err?.message || err);
      }

      setupMap();
      setupChart();
      await loadIndex();

      document.getElementById('fetch-btn').addEventListener('click', fetchViewportData);

      sqlInput = document.getElementById('sql-input');
      sqlStatus = document.getElementById('sql-status');
      if (sqlInput) {
        sqlInput.value = DEFAULT_SQL.trim();
        sqlInput.addEventListener('input', onSqlInput);
      }
      setSqlStatus('Awaiting data…');

      status.textContent = 'Ready — pan the map and click "Fetch Viewport Data".';
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
      });
    }

    function setupChart() {
      const ctx = document.getElementById('chart');
      chart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: [],
          datasets: [{
            label: 'Avg daily_mean (viewport)',
            data: [],
            tension: 0.2
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: { x: { ticks: { maxTicksLimit: 6 } } }
        }
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

    function fitMapToIndex() {
      if (!map || !indexByHex2Dec.size) return;
      const firstDec = Array.from(indexByHex2Dec.keys())[0];
      try {
        const hex = BigInt(firstDec).toString(16);
        const [lat, lng] = h3.cellToLatLng(hex);
        map.flyTo({ center: [lng, lat], zoom: 10, essential: true });
      } catch (e) {}
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
      const cells = h3.polygonToCells(poly, FILTER_RES, true);
      return cells.map(h => BigInt('0x' + h).toString());
    }

    function rowToFilterResDecimal(row) {
      let raw = row.hex ?? row.h3 ?? row.index ?? row.id;
      if (raw == null) return null;
      let s = String(raw);
      let h;
      if (/[a-f]/i.test(s)) {
        h = s.replace(/^0x/i, '');
      } else {
        h = BigInt(s).toString(16);
      }
      const parent = h3.cellToParent(h, FILTER_RES);
      return BigInt('0x' + parent).toString();
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
      document.getElementById('fetch-btn').disabled = true;

      const status = document.getElementById('status');
      const results = document.getElementById('results');
      const path = document.getElementById('path').value.trim();

      try {
        tableReady = false;
        lastAggregatedData = [];
        setSqlStatus('Fetching viewport rows…');
        if (!indexByHex2Dec.size) {
          status.textContent = 'Index empty';
          updateChartFromRows([]);
          renderMapFromData([]);
          tableReady = false;
          setSqlStatus('Index not loaded.');
          await registerViewportTable([]);
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
          results.textContent = '(none)';
          updateChartFromRows([]);
          renderMapFromData([]);
          await registerViewportTable([]);
          return;
        }

        status.textContent = `Loading ${rowgroups.length} rowgroups…`;
        const allRows = [];

        for (const rg of rowgroups) {
          const { dataPtr, dataSize } = await readRowGroupWithRetry(path, rg);
          const fileBuffer = new Uint8Array(dataSize);
          fileBuffer.set(new Uint8Array(wasmModule.HEAPU8.buffer, dataPtr, dataSize));
          wasmModule._free(dataPtr);

        fileCounter++;
          const fileName = `viewport_${fileCounter}.parquet`;
        await db.registerFileBuffer(fileName, fileBuffer);
          const qres = await conn.query(`SELECT * FROM read_parquet('${fileName}')`);
        const rows = qres.toArray().map(r => {
          const o = {};
          for (const [k, v] of Object.entries(r)) {
            o[k] = (typeof v === 'bigint') ? v.toString() : v;
          }
          return o;
        });

          const filtered = rows.filter(r => {
            const d = rowToFilterResDecimal(r);
            return d && viewportSet.has(d);
          });
          allRows.push(...filtered);
        }

        status.textContent = `Loaded ${allRows.length} rows`;
        results.textContent =
          `Total rows: ${allRows.length}\n\n` +
          JSON.stringify(allRows.slice(0, 30), null, 2);

        updateChartFromRows(allRows);
        const aggregatedData = aggregateRows(allRows);
        lastAggregatedData = aggregatedData;
        setSqlStatus('Preparing DuckDB tables…');
        await registerViewportTable(allRows);
        await runSqlAndRender({ fallbackData: aggregatedData });

      } catch (e) {
        console.error(e);
        status.textContent = 'Error: ' + e.message;
        setSqlStatus('Error fetching data.');
      } finally {
        fetchInProgress = false;
        document.getElementById('fetch-btn').disabled = false;
      }
    }

    function updateChartFromRows(rows) {
      if (!chart) return;
      const byDate = new Map();
      for (const r of rows) {
        const ds = r.datestr;
        if (!ds) continue;
        let v = r.daily_mean ?? r.value ?? r.temp ?? r.t2m;
        if (typeof v === 'string') v = Number(v);
        if (!Number.isFinite(v)) continue;

        if (!byDate.has(ds)) byDate.set(ds, { sum: v, count: 1 });
        else {
          const x = byDate.get(ds);
          x.sum += v;
          x.count += 1;
        }
      }

      const sorted = Array.from(byDate.entries()).sort((a,b) => Number(a[0]) - Number(b[0]));
      const labels = [];
      const data = [];
      for (const [ds, {sum, count}] of sorted) {
        const d = new Date(Number(ds));
        labels.push(d.toISOString().slice(0,10));
        data.push(sum / count);
      }

      chart.data.labels = labels;
      chart.data.datasets[0].data = data;
      chart.update();
    }

    function updateMapFromRows(rows) {
      const aggregated = aggregateRows(rows);
      renderMapFromData(aggregated);
    }

    function resolveDisplayCell(row) {
      let raw = row.hex ?? row.h3 ?? row.index ?? row.id;
      if (raw == null) return null;
      let cell;
      const asString = String(raw);
      if (/[a-f]/i.test(asString)) {
        cell = asString.replace(/^0x/i, '');
        } else {
        try {
          cell = BigInt(asString).toString(16);
        } catch (e) {
          return null;
        }
      }
      try {
        const currentRes = h3.getResolution(cell);
        if (currentRes > DISPLAY_RES) {
          return h3.cellToParent(cell, DISPLAY_RES);
        }
        return cell;
      } catch (e) {
        return null;
      }
    }

    function aggregateRows(rows) {
      const agg = new Map();
      for (const r of rows) {
        const dispCell = resolveDisplayCell(r);
        if (!dispCell) continue;
        let v = r.daily_mean ?? r.avg ?? r.avg_daily ?? r.value ?? r.metric ?? r.temp ?? r.t2m;
        if (typeof v === 'string') v = Number(v);
        if (!Number.isFinite(v)) continue;
        if (!agg.has(dispCell)) {
          agg.set(dispCell, { sum: v, count: 1 });
        } else {
          const bucket = agg.get(dispCell);
          bucket.sum += v;
          bucket.count += 1;
        }
      }

      const data = [];
      for (const [cell, { sum, count }] of agg.entries()) {
        let lat, lng;
        try {
          [lat, lng] = h3.cellToLatLng(cell);
        } catch (e) {
          continue;
        }
        data.push({ hex: cell, avg: sum / count, count, lat, lng });
      }
      return data;
    }

    function renderMapFromData(data) {
      if (!overlayReady) {
        if (map) {
          map.once('load', () => {
            setTimeout(() => renderMapFromData(data), 100);
          });
        }
        return;
      }
      
      if (!data || data.length === 0) {
        if (overlay) overlay.setProps({ layers: [] });
        return;
      }

      let minVal = Infinity, maxVal = -Infinity;
      let minLat = 999, minLng = 999, maxLat = -999, maxLng = -999;

      for (const d of data) {
        const value = Number(d.avg ?? d.value ?? d.metric ?? d.daily_mean);
        if (Number.isFinite(value)) {
          if (value < minVal) minVal = value;
          if (value > maxVal) maxVal = value;
        }
        let { lat, lng } = d;
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
          try {
            [lat, lng] = h3.cellToLatLng(d.hex);
        } catch (e) {
          continue;
        }
        }
        d.lat = lat;
        d.lng = lng;
            if (lat < minLat) minLat = lat;
            if (lat > maxLat) maxLat = lat;
            if (lng < minLng) minLng = lng;
            if (lng > maxLng) maxLng = lng;
      }

      if (!Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
        minVal = 0;
        maxVal = 1;
      }
      if (minVal === maxVal) maxVal = minVal + 1e-6;

      const layer = new deck.H3HexagonLayer({
        id: 'era5-res5-layer',
        data,
        pickable: true,
        filled: true,
        extruded: false,
        coverage: 0.95,
        getHexagon: d => d.hex,
        getFillColor: d => {
          const value = Number(d.avg ?? d.value ?? d.metric ?? d.daily_mean);
          if (!Number.isFinite(value)) return [120, 120, 120, 160];
          const t = (value - minVal) / (maxVal - minVal);
          const r = Math.round(255 * t);
          const b = Math.round(255 * (1 - t));
          return [r, 125, b, 200];
        },
        getLineColor: [26, 26, 26, 90],
        lineWidthMinPixels: 1
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
          if (!overlay || !overlayReady) {
            return;
          }

          if (!e?.point || !Number.isFinite(e.point.x) || !Number.isFinite(e.point.y)) {
            return;
          }

          let info = null;
          try {
            info = overlay.pickObject({ x: e.point.x, y: e.point.y, radius: 5 });
          } catch (err) {
            console.warn('[deck] pickObject failed', err);
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
          } else {
            tt.style.display = 'none';
            map.getCanvas().style.cursor = '';
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
        runSqlAndRender({ fallbackData: lastAggregatedData }).catch(err => console.error('SQL run error', err));
      }, SQL_DEBOUNCE_MS);
    }

    async function registerViewportTable(rows) {
      if (!conn) return;
      tableReady = false;
      try {
        await conn.query(`DROP TABLE IF EXISTS ${RAW_TABLE_NAME}`);
      } catch (err) {
        console.warn('Failed to drop previous viewport table', err);
      }

      const createTableSql = `CREATE TABLE ${RAW_TABLE_NAME} (
          hex VARCHAR,
          datestr BIGINT,
          daily_mean DOUBLE,
          value DOUBLE,
          metric DOUBLE,
          cnt DOUBLE,
          count DOUBLE,
          temp DOUBLE,
          t2m DOUBLE,
          lat DOUBLE,
          lng DOUBLE
        )`;

      if (!rows || rows.length === 0) {
        await conn.query(createTableSql);
        await conn.query(`CREATE OR REPLACE VIEW data AS SELECT * FROM ${RAW_TABLE_NAME}`);
        tableReady = true;
        setSqlStatus('Viewport table is empty.');
        return;
      }

      try {
        await conn.query(createTableSql);

        const chunkSize = 250;
        for (let i = 0; i < rows.length; i += chunkSize) {
          const chunk = rows.slice(i, i + chunkSize);
          const values = [];
          for (const row of chunk) {
            const hex = normalizeRawHex(row.hex ?? row.h3 ?? row.index ?? row.id);
            if (!hex) continue;

            const datestr = toSqlNumber(row.datestr);
            const dailyMean = toSqlNumber(row.daily_mean);
            const value = toSqlNumber(row.value);
            const metric = toSqlNumber(row.metric);
            const cnt = toSqlNumber(row.cnt);
            const count = toSqlNumber(row.count ?? row.samples);
            const temp = toSqlNumber(row.temp);
            const t2m = toSqlNumber(row.t2m);

            let lat = toSqlNumber(row.lat ?? row.latitude);
            let lng = toSqlNumber(row.lng ?? row.lon ?? row.longitude);

            if (lat === 'NULL' || lng === 'NULL') {
              try {
                const [latVal, lngVal] = h3.cellToLatLng(hex);
                lat = toSqlNumber(latVal);
                lng = toSqlNumber(lngVal);
              } catch (_) {
                lat = 'NULL';
                lng = 'NULL';
              }
            }

            values.push(`(${toSqlString(hex)}, ${datestr}, ${dailyMean}, ${value}, ${metric}, ${cnt}, ${count}, ${temp}, ${t2m}, ${lat}, ${lng})`);
          }
          if (values.length) {
            await conn.query(`INSERT INTO ${RAW_TABLE_NAME} (hex, datestr, daily_mean, value, metric, cnt, count, temp, t2m, lat, lng) VALUES ${values.join(',')}`);
          }
        }

        await conn.query(`CREATE OR REPLACE VIEW data AS SELECT * FROM ${RAW_TABLE_NAME}`);
        tableReady = true;
        setSqlStatus(`${RAW_TABLE_NAME} ready (${rows.length.toLocaleString()} rows)`);
      } catch (err) {
        console.error('registerViewportTable error', err);
        tableReady = false;
        setSqlStatus(`Failed to load viewport rows: ${err.message || err}`);
        throw err;
      }
    }

    function normalizeQueryRows(rows) {
      const out = [];
      for (const row of rows) {
        const hex = resolveDisplayCell(row);
        if (!hex) continue;
        let lat = row.lat ?? row.latitude;
        let lng = row.lng ?? row.lon ?? row.longitude;
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
          try {
            [lat, lng] = h3.cellToLatLng(hex);
          } catch (e) {
            continue;
          }
        }
        const rawValue = row.avg ?? row.avg_daily ?? row.value ?? row.metric ?? row.daily_mean;
        const numericValue = Number(rawValue);
        const avg = Number.isFinite(numericValue) ? numericValue : null;
        const rawCount = row.count ?? row.cnt ?? row.samples;
        const count = Number.isFinite(Number(rawCount)) ? Number(rawCount) : rawCount;
        out.push({ ...row, hex, avg, count, lat, lng });
      }
      return out;
    }

    async function runSqlAndRender({ fallbackData } = {}) {
      if (!conn) {
        if (fallbackData) renderMapFromData(fallbackData);
        return;
      }
      if (!tableReady) {
        if (fallbackData) renderMapFromData(fallbackData);
        setSqlStatus('Viewport table not ready yet.');
        return;
      }
      const sqlText = sanitizeSQL(sqlInput?.value) || DEFAULT_SQL;
      currentSQL = sqlText;
      setSqlStatus('Running query…');
      try {
        const result = await conn.query(sqlText);
        const rows = result.toArray();
        console.log('[hex_map_era5] SQL result preview:', rows.slice(0, 25));
        const normalized = normalizeQueryRows(rows);
        const aggregated = aggregateRows(normalized);
        console.log('[hex_map_era5] Aggregated preview:', aggregated.slice(0, 25));
        if (aggregated.length === 0 && fallbackData) {
          renderMapFromData(fallbackData);
          setSqlStatus('Query returned 0 rows (showing fallback).');
        } else {
          renderMapFromData(aggregated);
          setSqlStatus(`Query returned ${aggregated.length.toLocaleString()} rows.`);
          if (aggregated.length) {
            lastAggregatedData = aggregated;
          }
        }
      } catch (err) {
        console.error('DuckDB SQL error', err);
        setSqlStatus(`SQL error: ${err.message || err}`);
        if (fallbackData) renderMapFromData(fallbackData);
      }
    }

    init();
  </script>
</body>
</html>
'''
    return html
