common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")

DEFAULT_CONFIG = r"""{
  "tileLayer": {
    "@@type": "TileLayer",
    "maxZoom": 12
  },
  "hexLayer": {
    "@@type": "H3HexagonLayer",
    "filled": true,
    "pickable": true,
    "extruded": false,
    "getHexagon": "@@=properties.hex",
    "getFillColor": {
      "@@function": "colorContinuous",
      "attr": "pop_per_hex",
      "domain": [0, 1000],
      "steps": 20,
      "colors": "OrYel"
    }
  }
}"""

 
DEFAULT_STYLE_URL = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"

@fused.udf
def udf( 
    data_url: str = "https://unstable.udf.ai/fsh_5fxZ3aJLfZz7RTRCbZmaRQ.parquet?name=McDonald",
    config_json: str = DEFAULT_CONFIG,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    # Updated to New York City coordinates
    center_lng: float = -74.0060,
    center_lat: float = 40.7128,
    zoom: float = 10,
    default_query: str = "SELECT * FROM data",
    map_style_url: str = "mapbox://styles/mapbox/dark-v11",
    metric_channel: str = "hex-mcdonald",
    map_sync_channel: str = "channel2",
    show_status_log: bool = False,
):
    from jinja2 import Template
 
    style_url = map_style_url or "mapbox://styles/mapbox/dark-v11"

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>H3 Parquet Viewer - Optimized</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>

  <script type="module">
    import * as duckdb_wasm from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm";
    window.__DUCKDB_WASM = duckdb_wasm;
  </script>
  
  <script type="module">
    import * as cartocolor from 'https://esm.sh/cartocolor@5.0.2';
    window.cartocolor = cartocolor;
  </script>

  <style>
    :root {
      --bg-panel: rgba(15,15,15,0.9);
      --bg-input: #1a1a1a;
      --border-color: #3a3a3a;
      --text-primary: #ffffff;
      --text-dim: #9e9e9e;
      --radius-md: 6px;
      --shadow-soft: 0 -4px 12px rgba(0,0,0,.7);
    }

    html, body {
      margin:0; padding:0; height:100%;
      background:#000;
      color:var(--text-primary);
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }

    #map { position:fixed; inset:0; }

    .status-pill {
      position:absolute;
      top:12px;
      right:12px;
      background:var(--bg-panel);
      color:var(--text-primary);
      padding:8px 12px;
      border-radius:var(--radius-md);
      border:1px solid var(--border-color);
      font-size:11px;
      z-index:7;
      pointer-events:none;
      box-shadow:var(--shadow-soft);
      max-width:320px;
      text-align:left;
      max-height:300px;
      overflow-y:auto;
      line-height:1.5;
      display: {{ 'block' if show_status_log else 'none' }};
    }

    .status-pill .status-header {
      font-weight:600;
      color:#4fc3f7;
      margin-bottom:4px;
      padding-bottom:4px;
      border-bottom:1px solid var(--border-color);
    }

    .status-pill .status-log {
      font-family: monospace;
      font-size:10px;
      color:var(--text-dim);
      margin:2px 0;
    }

    .status-pill .status-log.timing {
      color:#81c784;
      font-weight:500;
    }

    .loader {
      position: absolute;
      bottom: 16px;
      right: 16px;
      z-index: 8;
      display: none;
      align-items: center;
      gap: 12px;
      background: var(--bg-panel);
      padding: 12px 18px;
      border-radius: var(--radius-md);
      border: 1px solid var(--border-color);
      box-shadow: var(--shadow-soft);
      font-size: 14px;
      color: var(--text-primary);
    }

    .loader.active {
      display: flex;
    }

    .spinner {
      width: 24px;
      height: 24px;
      border: 3px solid var(--border-color);
      border-top-color: #4fc3f7;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }

    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    .loader-text {
      font-weight: 500;
    }

    .color-legend {
      position: fixed;
      left: 24px;
      bottom: 24px;
      background: rgba(15, 15, 15, 0.95);
      color: #fff;
      padding: 16px 18px;
      border-radius: 12px;
      font-size: 14px;
      z-index: 10;
      box-shadow: 0 12px 30px rgba(0,0,0,0.45);
      border: 1px solid rgba(255,255,255,0.15);
      min-width: 220px;
      display: none;
    }
    .color-legend .legend-title {
      margin-bottom: 12px;
      font-weight: 600;
      font-size: 16px;
      color: #fff;
    }
    .color-legend .legend-gradient {
      width: 100%;
      height: 24px;
      border-radius: 6px;
      margin-bottom: 8px;
      border: 1px solid rgba(255,255,255,0.3);
    }
    .color-legend .legend-labels {
      display: flex;
      justify-content: space-between;
      font-size: 13px;
      font-weight: 500;
      color: #ddd;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="status-pill" id="statusPill"><div class="status-header">Status Log</div></div>
  <div class="loader" id="loader">
    <div class="spinner"></div>
    <div class="loader-text" id="loaderText">Loading...</div>
  </div>
  <div id="color-legend" class="color-legend">
    <div class="legend-title"></div>
    <div class="legend-gradient"></div>
    <div class="legend-labels">
      <span class="legend-min"></span>
      <span class="legend-max"></span>
    </div>
  </div>


  <script type="module">
    const DATA_URL        = {{ data_url | tojson }};
    const MAPBOX_TOKEN    = {{ mapbox_token | tojson }};
    const MAP_STYLE_URL   = {{ map_style_url | tojson }};
    const START_CENTER    = [{{ center_lng }}, {{ center_lat }}];
    const START_ZOOM      = {{ zoom }};
    const INITIAL_CONFIG  = {{ config_json | tojson }};
    const MAP_SYNC_CHANNEL = {{ map_sync_channel | tojson }};

    const { MapboxOverlay, PolygonLayer } = deck;
    const H3HexagonLayer = deck.H3HexagonLayer || (deck.GeoLayers && deck.GeoLayers.H3HexagonLayer);
    const { colorContinuous } = deck.carto;

    let duckConn;
    let overlay;
    let map;
    let currentConfObj = {};
    let currentColorAttr = 'pop_per_hex';
    let queryInFlight = false;
    let rerunRequested = false;
    let viewportFramePending = false;
    const AGGREGATE_RESOLUTIONS = [4, 5, 6, 7, 8];
    let aggregatesReady = false;
    const METRIC_CHANNEL = {{ metric_channel | tojson }};
    const METRIC_EVENT_TYPE = "hex-window-metric";
    const HEX_TO_H3_EXPR = `
      CASE
        WHEN typeof(hex) IN ('BIGINT','UBIGINT','HUGEINT','INTEGER','SMALLINT','TINYINT') THEN CAST(hex AS UBIGINT)
        WHEN typeof(hex) IN ('VARCHAR','TEXT') THEN
          CASE
            WHEN regexp_full_match(CAST(hex AS VARCHAR), '^[0-9]+$') THEN TRY_CAST(CAST(hex AS HUGEINT) AS UBIGINT)
            ELSE h3_string_to_h3(CAST(hex AS VARCHAR))
          END
        WHEN typeof(hex) = 'BLOB' THEN h3_string_to_h3(CAST(hex AS VARCHAR))
        ELSE NULL
      END
    `;

    const statusPill = document.getElementById("statusPill");
    let statusLog = [];
    const MAX_LOG_LINES = 20;

    let lastSuccessfulData = [];
    let runToken = 0;
    let lastTotals = null;
    let metricBroadcast = null;

    const loaderEl = document.getElementById("loader");
    const loaderTextEl = document.getElementById("loaderText");

    function showLoader(text = "Loading...") {
      if (loaderEl) {
        loaderEl.classList.add("active");
        if (loaderTextEl) loaderTextEl.textContent = text;
      }
    }

    function hideLoader() {
      if (loaderEl) {
        loaderEl.classList.remove("active");
      }
    }

    if (METRIC_CHANNEL) {
      try {
        metricBroadcast = new BroadcastChannel(`hex-window-metric::${METRIC_CHANNEL}`);
      } catch (err) {
        console.warn("[hex_window] metric BroadcastChannel failed", err);
        metricBroadcast = null;
      }
    }

    let mapSyncBroadcast = null;
    let mapId = Math.random().toString(36).substr(2, 9);
    let isSyncing = false;
    let userInteracting = false;
    let lastBroadcast = {x: NaN, y: NaN, z: NaN};
    let moveTimeout = null;

    if (MAP_SYNC_CHANNEL) {
      try {
        mapSyncBroadcast = new BroadcastChannel(`map-sync::${MAP_SYNC_CHANNEL}`);
        mapSyncBroadcast.onmessage = (e) => {
          const state = e.data;
          if (!state || state.id === mapId) return;
          
          // Check if this is a location selector message (animate to location)
          if (state.source === 'location-selector') {
            console.log('[hex_window] Location selector message received:', state);
            isSyncing = true;
            map.flyTo({
              center: state.center,
              zoom: state.zoom,
              bearing: state.bearing || 0,
              pitch: state.pitch || 0,
              duration: 1500,
              essential: true
            });
            setTimeout(() => { isSyncing = false; }, 1600);
            return;
          }
          
          // Regular map sync (no animation)
          if (isSyncing || userInteracting) return;
          
          isSyncing = true;
          map.jumpTo({
            center: state.center,
            zoom: state.zoom,
            bearing: state.bearing,
            pitch: state.pitch,
            animate: false
          });
          requestAnimationFrame(() => { isSyncing = false; });
        };
      } catch (err) {
        console.warn("[hex_window] map sync BroadcastChannel failed", err);
        mapSyncBroadcast = null;
      }
    }

    const eq = (a, b, epsilon) => Math.abs(a - b) < epsilon;

    function getMapState() {
      if (!map) return null;
      const c = map.getCenter();
      return {
        center: [+c.lng.toFixed(6), +c.lat.toFixed(6)],
        zoom: +map.getZoom().toFixed(3),
        bearing: +map.getBearing().toFixed(1),
        pitch: +map.getPitch().toFixed(1),
        id: mapId,
        ts: Date.now()
      };
    }

    function broadcastMapState() {
      if (!mapSyncBroadcast || isSyncing) return;
      const state = getMapState();
      if (!state) return;
      
      const [x, y] = state.center;
      const z = state.zoom;
      
      if (eq(x, lastBroadcast.x, 1e-6) && 
          eq(y, lastBroadcast.y, 1e-6) && 
          eq(z, lastBroadcast.z, 1e-3)) return;
      
      lastBroadcast = {x, y, z};
      mapSyncBroadcast.postMessage(state);
    }

    function scheduleBroadcast() {
      if (moveTimeout) clearTimeout(moveTimeout);
      moveTimeout = setTimeout(broadcastMapState, 0);
    }

    function addLog(message, isTiming = false) {
      const timestamp = new Date().toLocaleTimeString();
      statusLog.push({ text: `[${timestamp}] ${message}`, timing: isTiming });
      if (statusLog.length > MAX_LOG_LINES) {
        statusLog.shift();
      }
      updateStatusDisplay();
    }

    function updateStatusDisplay() {
      if (!statusPill) return;
      let html = '<div class="status-header">Status Log</div>';
      statusLog.forEach(log => {
        const className = log.timing ? 'status-log timing' : 'status-log';
        html += `<div class="${className}">${log.text}</div>`;
      });
      statusPill.innerHTML = html;
    }


    function evalExpression(expr, object) {
      if (typeof expr === 'string' && expr.startsWith('@@=')) {
        const code = expr.slice(3);
        try {
          const fn = new Function('object', `
            const properties = object?.properties || object || {};
            return (${code});
          `);
          return fn(object);
        } catch (e) { 
          console.error('@@= eval error:', expr, e); 
          return null; 
        }
      }
      return expr;
    }

    function processColorContinuous(cfg) {
      let domain = cfg.domain;
      if (domain && domain.length === 2) {
        const start = Number(domain[0]);
        const end = Number(domain[1]);
        const steps = cfg.steps ?? 20;

        if (Number.isFinite(start) && Number.isFinite(end)) {
          if (steps > 1) {
            const stepSize = (end - start) / (steps - 1);
            domain = Array.from({ length: steps }, (_, i) => start + stepSize * i);
          } else {
            domain = [start];
          }
        } else {
          domain = [domain[0], domain[1]];
        }
      }
      return {
        attr: cfg.attr,
        domain: domain,
        colors: cfg.colors || 'TealGrn',
        nullColor: cfg.nullColor || [184,184,184]
      };
    }

    function parseHexLayerConfig(config) {
      const out = {};
      for (const [k, v] of Object.entries(config || {})) {
        if (k === '@@type') continue;
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          if (v['@@function'] === 'colorContinuous') {
            out[k] = colorContinuous(processColorContinuous(v));
          } else {
            out[k] = v;
          }
        } else if (typeof v === 'string' && v.startsWith('@@=')) {
          out[k] = (obj) => evalExpression(v, obj);
        } else {
          out[k] = v;
        }
      }
      return out;
    }

    function rowsToFeatures(rows){
      return rows.map(p => {
        const feature = { ...p };
        feature.properties = feature;
        return feature;
      });
    }

    function updateTotalsDisplay(total, medianIncome = 0) {
      if (!Number.isFinite(total)) {
        broadcastMetrics({ starbucks: 0, median_income: 0 });
        return;
      }
      broadcastMetrics({ 
        starbucks: total,
        median_income: Number.isFinite(medianIncome) ? medianIncome : 0
      });
    }

    function broadcastMetrics(totals) {
      if (!METRIC_CHANNEL) return;
      if (!totals || typeof totals !== 'object') return;
      const payload = {
        type: METRIC_EVENT_TYPE,
        channel: METRIC_CHANNEL,
        totals,
        zoom: map?.getZoom?.(),
        bounds: getViewportBounds(),
        ts: Date.now(),
      };
      try {
        const target =
          window.parent && window.parent !== window ? window.parent : window;
        target.postMessage(payload, "*");
        if (metricBroadcast) {
          metricBroadcast.postMessage(payload);
        }
        lastTotals = totals;
      } catch (err) {
        console.warn("[hex_window] metric postMessage failed", err);
      }
    }

    function buildDeckLayer(data, confObj) {
      const hexCfg = parseHexLayerConfig(confObj.hexLayer || {});
      
      // Set up tooltip
      const getTooltip = ({object}) => {
        if (!object) return null;
        
        const lines = [];
        // Add common fields
        if (object.pop_per_hex !== undefined && object.pop_per_hex !== null) {
          lines.push(`Population: ${Number(object.pop_per_hex).toFixed(0)}`);
        }
        if (object.median_income !== undefined && object.median_income !== null) {
          lines.push(`Median Income: $${Number(object.median_income).toFixed(0)}`);
        }
        if (object.poi_count !== undefined && object.poi_count !== null) {
          lines.push(`POI Count: ${object.poi_count}`);
        }
        
        // Add any other numeric fields
        Object.keys(object).forEach(key => {
          if (!['hex', 'lat', 'lng', 'pop_per_hex', 'median_income', 'poi_count', 'sum', 'count', 'total_pop', 'weighted_median_income', 'properties'].includes(key)) {
            const val = object[key];
            if (typeof val === 'number' && Number.isFinite(val)) {
              lines.push(`${key}: ${val.toFixed(2)}`);
            }
          }
        });
        
        return lines.length > 0 ? {
          html: `<div style="background: rgba(0,0,0,0.85); color: #fff; padding: 6px 10px; border-radius: 4px; font-family: monospace; font-size: 11px; line-height: 1.6;">
            ${lines.join('<br/>')}
          </div>`,
          style: {
            backgroundColor: 'transparent',
            fontSize: '11px'
          }
        } : null;
      };
      
      return H3HexagonLayer ? new H3HexagonLayer({
        id: 'hex-layer',
        data,
        pickable: true,
        stroked: !!confObj.hexLayer?.stroked,
        filled: true,
        extruded: !!confObj.hexLayer?.extruded,
        coverage: 0.9,
        lineWidthMinPixels: confObj.hexLayer?.lineWidthMinPixels ?? 1,
        getHexagon: d => d.hex,
        ...hexCfg
      }) : new PolygonLayer({
        id: 'hex-layer-fallback',
        data: data.map(d => {
          const ring = h3.cellToBoundary(d.hex, true).map(([lat,lng]) => [lng, lat]);
          return { ...d, polygon: ring };
        }),
        pickable: true,
        stroked: true,
        filled: true,
        extruded: false,
        getPolygon: d => d.polygon,
        getFillColor: [184,184,184,220],
        getLineColor: [200,200,200,255],
        lineWidthMinPixels: 1
      });
    }

    function updateOverlayLayers(data, confObj){
      const layer = buildDeckLayer(data, confObj);
      
      // Set up tooltip
      const getTooltip = ({object}) => {
        if (!object) return null;
        
        const lines = [];
        // Add common fields
        if (object.pop_per_hex !== undefined && object.pop_per_hex !== null) {
          lines.push(`Population: ${Number(object.pop_per_hex).toFixed(0)}`);
        }
        if (object.median_income !== undefined && object.median_income !== null) {
          lines.push(`Median Income: $${Number(object.median_income).toFixed(0)}`);
        }
        if (object.poi_count !== undefined && object.poi_count !== null) {
          lines.push(`POI Count: ${object.poi_count}`);
        }
        
        // Add any other numeric fields
        Object.keys(object).forEach(key => {
          if (!['hex', 'lat', 'lng', 'pop_per_hex', 'median_income', 'poi_count', 'sum', 'count', 'total_pop', 'weighted_median_income', 'properties'].includes(key)) {
            const val = object[key];
            if (typeof val === 'number' && Number.isFinite(val)) {
              lines.push(`${key}: ${val.toFixed(2)}`);
            }
          }
        });
        
        return lines.length > 0 ? {
          html: `<div style="background: rgba(0,0,0,0.85); color: #fff; padding: 6px 10px; border-radius: 4px; font-family: monospace; font-size: 11px; line-height: 1.6;">
            ${lines.join('<br/>')}
          </div>`,
          style: {
            backgroundColor: 'transparent',
            fontSize: '11px'
          }
        } : null;
      };

      if (!overlay) {
        overlay = new MapboxOverlay({
          interleaved: false,
          layers: [layer],
          getTooltip
        });
        map.addControl(overlay);
      } else {
        overlay.setProps({
          layers: [layer],
          getTooltip
        });
      }

      lastSuccessfulData = data;
      
      // Generate color legend after updating layers
      generateColorLegend(confObj);
    }

    function selectAggregateTable(zoom) {
      if (!Number.isFinite(zoom)) {
        return { table: 'agg_res5', resolution: 5 };
      }
      if (zoom >= 10) {
        return { table: 'agg_res8', resolution: 8 };
      }
      if (zoom >= 8) {
        return { table: 'agg_res7', resolution: 7 };
      }
      if (zoom >= 8) {
        return { table: 'agg_res6', resolution: 6 };
      }
      if (zoom >= 6) {
        return { table: 'agg_res5', resolution: 5 };
      }
      return { table: 'agg_res4', resolution: 4 };
    }

    function generateColorLegend(confObj) {
      const hexLayer = confObj?.hexLayer || {};
      const colorCfg = hexLayer.getFillColor;
      
      if (!colorCfg || colorCfg['@@function'] !== 'colorContinuous') {
        const legendEl = document.getElementById('color-legend');
        if (legendEl) legendEl.style.display = 'none';
        return;
      }
      
      const attr = colorCfg.attr;
      let domain = Array.isArray(colorCfg.domain) ? colorCfg.domain.slice() : null;
      const steps = colorCfg.steps || 20;
      const colors = colorCfg.colors || 'OrYel';
      
      if (!attr || !domain || domain.length === 0) {
        return;
      }
      
      // Accept arrays with more than 2 elements (Deck.GL allows multi-stop domains)
      if (domain.length >= 2) {
        const numericDomain = domain
          .map(Number)
          .filter((v) => Number.isFinite(v));
        if (numericDomain.length >= 2) {
          domain = [Math.min(...numericDomain), Math.max(...numericDomain)];
        } else {
          return;
        }
      } else {
        return;
      }
      
      if (!window.cartocolor) {
        setTimeout(() => generateColorLegend(confObj), 100);
        return;
      }
      
      try {
        const palette = window.cartocolor[colors];
        if (!palette) {
          console.warn('[legend] Palette not found:', colors);
          return;
        }
        
        let colorsArray = null;
        if (palette[steps]) {
          colorsArray = palette[steps];
        } else {
          const availableSteps = Object.keys(palette)
            .map(Number)
            .filter((n) => !isNaN(n))
            .sort((a, b) => b - a);
          const closestStep = availableSteps.find((s) => s <= steps) || availableSteps[availableSteps.length - 1];
          colorsArray = palette[closestStep];
        }
        
        if (!colorsArray || !Array.isArray(colorsArray)) {
          console.warn('[legend] Invalid color array for palette:', colors);
          return;
        }
        
        const gradient = colorsArray
          .map((c, i) => {
            const pct = (i / (colorsArray.length - 1 || 1)) * 100;
            return `${c} ${pct}%`;
          })
          .join(', ');
        
        const legendEl = document.getElementById('color-legend');
        if (legendEl) {
          legendEl.querySelector('.legend-title').textContent = attr;
          legendEl.querySelector('.legend-gradient').style.background = `linear-gradient(to right, ${gradient})`;
          legendEl.querySelector('.legend-min').textContent = Number(domain[0]).toFixed(1);
          legendEl.querySelector('.legend-max').textContent = Number(domain[1]).toFixed(1);
          legendEl.style.display = 'block';
        }
      } catch (err) {
        console.error('[legend] Failed to generate color legend:', err);
      }
    }

    async function buildAggregates() {
      showLoader("Building aggregates...");
      addLog("Building multi-resolution aggregates...");
      const start = performance.now();
      aggregatesReady = false;
      try {
        for (const res of AGGREGATE_RESOLUTIONS) {
          const tableName = `agg_res${res}`;
          showLoader(`Aggregating res ${res}...`);
          addLog(`Aggregating resolution ${res}...`);
          await duckConn.query(`
            CREATE OR REPLACE TABLE ${tableName} AS
            WITH base AS (
              SELECT
                ${HEX_TO_H3_EXPR} AS cell,
                pop_per_hex,
                median_income
              FROM data
              WHERE pop_per_hex IS NOT NULL
            ),
            parents AS (
              SELECT
                h3_cell_to_parent(cell, ${res}) AS parent_hex,
                pop_per_hex,
                median_income
              FROM base
              WHERE cell IS NOT NULL
            )
            SELECT
              h3_h3_to_string(parent_hex) AS hex,
              h3_cell_to_lat(parent_hex) AS lat,
              h3_cell_to_lng(parent_hex) AS lng,
              SUM(pop_per_hex) AS sum,
              COUNT(*) AS count,
              AVG(median_income) AS median_income
            FROM parents
            WHERE parent_hex IS NOT NULL
            GROUP BY 1, 2, 3
          `);
        }
        aggregatesReady = true;
        const elapsed = ((performance.now() - start) / 1000).toFixed(2);
        addLog(`Aggregates ready in ${elapsed}s`, true);
        hideLoader();
      } catch (err) {
        aggregatesReady = false;
        console.error('Aggregate build error', err);
        addLog('ERROR building aggregates: ' + (err?.message || err));
        hideLoader();
      }
    }

    async function initMapAndDuckDB(){
      showLoader("Initializing map...");
      addLog("Initializing map...");
      
      mapboxgl.accessToken = MAPBOX_TOKEN;
      
      const style = MAP_STYLE_URL.startsWith('mapbox://') 
        ? MAP_STYLE_URL 
        : (MAP_STYLE_URL || 'mapbox://styles/mapbox/dark-v11');
      
      map = new mapboxgl.Map({
        container:'map',
        style,
        center: START_CENTER,
        zoom: START_ZOOM,
        dragRotate:false,
        pitchWithRotate:false,
        projection: 'mercator'
      });

      await new Promise(resolve => {
        map.once('load', () => {
          addLog("Map ready");
          resolve();
        });
      });

      map.on('move', onViewportChange);
      map.on('zoom', onViewportChange);

      if (MAP_SYNC_CHANNEL) {
        const startInteraction = () => { userInteracting = true; };
        const finishInteraction = () => {
          if (!userInteracting) return;
          userInteracting = false;
          broadcastMapState();
        };
        
        map.on('dragstart', startInteraction);
        map.on('zoomstart', startInteraction);
        map.on('rotatestart', startInteraction);
        map.on('pitchstart', startInteraction);
        map.on('moveend', finishInteraction);
        
        map.on('move', () => {
          if (userInteracting && !isSyncing) scheduleBroadcast();
        });
        
        map.on('load', () => {
          setTimeout(() => mapSyncBroadcast?.postMessage(getMapState()), 100 + Math.random() * 100);
        });
      }

      showLoader("Loading DuckDB...");
      addLog("Loading DuckDB WASM...");
      const duckmod = window.__DUCKDB_WASM;
      const bundle = await duckmod.selectBundle(duckmod.getJsDelivrBundles());
      const worker = new Worker(
        URL.createObjectURL(
          new Blob([await (await fetch(bundle.mainWorker)).text()], {type:'application/javascript'})
        )
      );
      const db = new duckmod.AsyncDuckDB(new duckmod.ConsoleLogger(), worker);
      await db.instantiate(bundle.mainModule);
      duckConn = await db.connect();
      addLog("DuckDB initialized");

      try {
        await duckConn.query("LOAD parquet");
        addLog("Parquet extension loaded");
      } catch (loadErr) {
        console.error("LOAD parquet failed:", loadErr?.message || loadErr);
        addLog("ERROR: Parquet extension failed to load");
      }

      showLoader("Installing H3 extension...");
      addLog("Installing H3 extension...");
      try {
        await duckConn.query("INSTALL h3 FROM community");
        await duckConn.query("LOAD h3");
        addLog("H3 extension loaded");
      } catch (err) {
        console.error("H3 extension error:", err);
        addLog("ERROR: H3 extension failed - " + (err?.message || err));
        hideLoader();
        throw new Error("H3 extension required but failed to load");
      }

      showLoader("Downloading data...");
      addLog("Downloading data...");
      const r = await fetch(DATA_URL);
      if(!r.ok){ 
        addLog("Download failed: HTTP " + r.status);
        hideLoader();
        throw new Error(r.status); 
      }
      const bytes = new Uint8Array(await r.arrayBuffer());
      addLog("Data downloaded: " + (bytes.length / 1024 / 1024).toFixed(2) + " MB");

      await db.registerFileBuffer('data.parquet', bytes);
      await duckConn.query("CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('data.parquet')");
      addLog("Parquet view created");

      await buildAggregates();
    }

    function getViewportBounds() {
      if (!map) return null;
      const bounds = map.getBounds();
      return {
        west: bounds.getWest(),
        south: bounds.getSouth(),
        east: bounds.getEast(),
        north: bounds.getNorth()
      };
    }

    async function runQuery() {
      if (!duckConn || !currentConfObj) return;
      if (queryInFlight) {
        rerunRequested = true;
        if (duckConn && typeof duckConn.interrupt === 'function') {
          try {
            await duckConn.interrupt();
          } catch (interruptErr) {
            console.warn('duckdb interrupt failed', interruptErr);
          }
        }
        return;
      }
      queryInFlight = true;
      rerunRequested = false;

      runToken += 1;
      const thisRunToken = runToken;
      const shouldContinue = () => !rerunRequested && thisRunToken === runToken;
      const queryStartTime = performance.now();

      try {
        const bounds = getViewportBounds();
        const zoom = map?.getZoom?.();

        if (!aggregatesReady) {
          addLog("Waiting for aggregates to build...");
          queryInFlight = false;
          return;
        }

        const { table, resolution } = selectAggregateTable(zoom);
        
        // Build WHERE clause with bounds and optional histogram filter
        let whereClauses = [];
        if (bounds) {
          whereClauses.push(`lat BETWEEN ${bounds.south} AND ${bounds.north}`);
          whereClauses.push(`lng BETWEEN ${bounds.west} AND ${bounds.east}`);
        }
        
        // Add histogram filter if active
        if (activeHistogramFilter) {
          const { field, min, max } = activeHistogramFilter;
          whereClauses.push(`${field} BETWEEN ${min} AND ${max}`);
        }
        
        const whereClause = whereClauses.length > 0 
          ? `WHERE ${whereClauses.join(' AND ')}`
          : "";
        
        const aggregateSQL = `
          WITH viewport AS (
            SELECT
              hex,
              lat,
              lng,
              sum,
              CASE WHEN count > 0 THEN sum / count ELSE NULL END AS pop_per_hex,
              count,
              median_income
            FROM ${table}
            ${whereClause}
          ),
          totals AS (
            SELECT
              SUM(sum) AS total_pop,
              ROUND(SUM(COALESCE(median_income, 0) * (CASE WHEN count > 0 THEN sum / count ELSE 0 END)) / NULLIF(SUM(CASE WHEN count > 0 THEN sum / count ELSE 0 END), 0)) AS weighted_median_income
            FROM viewport
          )
          SELECT
            viewport.*,
            totals.total_pop,
            totals.weighted_median_income
          FROM viewport
          CROSS JOIN totals
        `;

        addLog(`Querying aggregates (res${resolution})...`);
        const res = await duckConn.query(aggregateSQL);
        const rows = res.toArray();

        if (!shouldContinue()) {
          return;
        }

        if (rows.length === 0) {
          addLog("No data in aggregated viewport");
          updateOverlayLayers([], currentConfObj);
          broadcastMetrics({ starbucks: 0, median_income: 0 });
          const elapsed = ((performance.now() - queryStartTime) / 1000).toFixed(2);
          addLog(`Aggregate query completed in ${elapsed}s`, true);
          return;
        }

        const conversionStart = performance.now();
        const data = rowsToFeatures(rows);
        const conversionTime = ((performance.now() - conversionStart) / 1000).toFixed(2);

        if (!shouldContinue()) {
          return;
        }

        updateOverlayLayers(data, currentConfObj);
        const totalPop = Number(rows[0]?.total_pop);
        const weightedMedianIncome = Number(rows[0]?.weighted_median_income);
        updateTotalsDisplay(
          Number.isFinite(totalPop) ? totalPop : 0,
          Number.isFinite(weightedMedianIncome) ? weightedMedianIncome : 0
        );

        const elapsed = ((performance.now() - queryStartTime) / 1000).toFixed(2);
        addLog(`✓ Rendered ${data.length.toLocaleString()} aggregated points`);
        addLog(`Feature conversion: ${conversionTime}s`, true);
        addLog(`Aggregate query time: ${elapsed}s`, true);
      } catch(e) {
        const message = String(e?.message || e || '');
        const elapsed = ((performance.now() - queryStartTime) / 1000).toFixed(2);
        if (/interrupt/i.test(message)) {
          console.warn('duckdb query interrupted');
          addLog('Query interrupted by new viewport');
          addLog(`Query interrupted after ${elapsed}s`, true);
        } else {
          console.error("SQL error:", e);
          addLog("ERROR: " + message);
          addLog(`Query failed after ${elapsed}s`, true);
        }
      } finally {
        queryInFlight = false;
        if (rerunRequested) {
          rerunRequested = false;
          runQuery().catch(err => console.error('rerun error', err));
        }
      }
    }

    function onViewportChange() {
      if (!map) return;
      if (viewportFramePending) return;
      viewportFramePending = true;
      requestAnimationFrame(() => {
        viewportFramePending = false;
        const promise = runQuery();
        if (promise && typeof promise.catch === 'function') {
          promise.catch(err => console.error('viewport query error', err));
        }
      });
    }

    // --- Histogram Filter State ---
    let activeHistogramFilter = null;

    function applyHistogramFilter(field, min, max) {
      console.log(`🎯 Applying histogram filter: ${field} [${min}, ${max}]`);
      activeHistogramFilter = { field, min, max };
      addLog(`Filter: ${field} ${min.toFixed(0)}-${max.toFixed(0)}`);
      
      // Trigger a re-query with the filter applied
      runQuery().catch(err => console.error('filter query error', err));
    }

    function clearHistogramFilter() {
      console.log('🧹 Clearing histogram filter');
      activeHistogramFilter = null;
      addLog('Filter cleared');
      
      // Trigger a re-query without the filter
      runQuery().catch(err => console.error('clear filter query error', err));
    }

    // --- Listen for Histogram Filter Messages ---
    function onHistogramFilterMessage(message) {
      try {
        if (typeof message === 'string') {
          try { message = JSON.parse(message); } catch (_) { return; }
        }
        if (!message || !message.type) return;

        // Listen for range filter from histogram
        if (message.type === 'histogram-range-filter') {
          if (METRIC_CHANNEL && message.channel !== METRIC_CHANNEL) return;
          
          const { field, min, max } = message;
          if (field && Number.isFinite(min) && Number.isFinite(max)) {
            applyHistogramFilter(field, min, max);
          }
          return;
        }

        // Listen for clear filter from histogram
        if (message.type === 'histogram-clear-filter') {
          if (METRIC_CHANNEL && message.channel !== METRIC_CHANNEL) return;
          clearHistogramFilter();
          return;
        }
      } catch (e) {
        console.warn('onHistogramFilterMessage error', e);
      }
    }

    // Set up listeners for histogram filters
    if (metricBroadcast) {
      metricBroadcast.addEventListener('message', (event) => {
        onHistogramFilterMessage(event.data);
      });
    }

    window.addEventListener('message', (event) => {
      onHistogramFilterMessage(event.data);
    });

    async function main(){
      await initMapAndDuckDB();

      try {
        currentConfObj = JSON.parse(INITIAL_CONFIG || "{}");
      } catch (_) {
        currentConfObj = {};
      }
      if (!currentConfObj || typeof currentConfObj !== 'object') {
        currentConfObj = {};
      }

      addLog("Ready. Pan/zoom map to query.");
      
      // Generate legend after config is loaded
      generateColorLegend(currentConfObj);
      
      await runQuery();
    }

    main().catch(e=>{
      console.error("init error", e);
      addLog("INIT ERROR: " + e.message);
    });

    window.addEventListener('beforeunload', () => {
      try {
        metricBroadcast?.close();
      } catch (_) {}
      try {
        mapSyncBroadcast?.close();
      } catch (_) {}
    });

    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        isSyncing = false;
        userInteracting = false;
      }
    });

    document.addEventListener('wheel', function(e){
      if (e.ctrlKey || e.metaKey) {
        e.preventDefault();
        e.stopPropagation();
      }
    }, { passive: false });
  </script>
</body>
</html>
""").render(
        data_url=data_url,
        config_json=config_json,
        mapbox_token=mapbox_token,
        map_style_url=style_url,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        metric_channel=metric_channel,
        map_sync_channel=map_sync_channel,
        show_status_log=show_status_log,
    )

    return common.html_to_obj(html)