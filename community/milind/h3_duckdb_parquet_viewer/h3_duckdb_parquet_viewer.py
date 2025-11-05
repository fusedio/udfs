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
      "attr": "elevation",
      "domain": [0, 3000],
      "steps": 20,
      "colors": "OrYel"
    }
  } 
}"""
 
DEFAULT_STYLE_URL = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"

@fused.udf
def udf( 
    data_url: str = "https://unstable.udf.ai/fsh_7hTX7DQTltN4DJl350b4ZE/run?dtype_out_raster=png&dtype_out_vector=parquet",
    config_json: str = DEFAULT_CONFIG,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -119.4179,
    center_lat: float = 36.7783,
    zoom: float = 4,
    tooltip_columns: list = ["crop_rank_1", "elevation", "daily_mean"],
    default_query: str = "SELECT * FROM data",
    map_style_url: str = "mapbox://styles/mapbox/dark-v11",
):
    from jinja2 import Template
 
    style_url = map_style_url or DEFAULT_STYLE_URL

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>H3 Parquet Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>

  <script src="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/dist/duckdb-wasm.js"></script>
  <script type="module">
    import * as duckdb_wasm from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm";
    window.__DUCKDB_WASM = duckdb_wasm;
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

    #tooltip {
      position: absolute;
      pointer-events: none;
      background: rgba(0,0,0,0.8);
      color: var(--text-primary);
      padding: 4px 8px;
      border-radius: var(--radius-md);
      font-size: 12px;
      display: none;
      z-index: 6;
      white-space: nowrap;
      border:1px solid var(--border-color);
    }

    .status-pill {
      position:absolute;
      top:12px;
      left:12px;
      background:var(--bg-panel);
      color:var(--text-primary);
      padding:6px 10px;
      border-radius:var(--radius-md);
      border:1px solid var(--border-color);
      font-size:11px;
      z-index:7;
      pointer-events:none;
      box-shadow:var(--shadow-soft);
      min-width:120px;
      text-align:center;
    }

    .bottombar {
      position:fixed;
      left:0; right:0; bottom:0;
      z-index:6;
      background: var(--bg-panel);
      color:var(--text-primary);
      border-top:1px solid var(--border-color);
      padding:8px;
      box-shadow: var(--shadow-soft);
      font-family: monospace;
      font-size:12px;
      line-height:1.4;
    }
    .bottombar .labelrow {
      display:flex;
      align-items:center;
      justify-content:space-between;
      font-size:11px;
      color:var(--text-dim);
      margin-bottom:4px;
    }
    .bottombar .labelrow .left {
      font-weight:500;
      color:var(--text-primary);
    }
    .bottombar textarea {
      width:100%;
      height:80px;
      resize:vertical;
      padding:6px;
      border:1px solid var(--border-color);
      border-radius:var(--radius-md);
      background:var(--bg-input);
      color:var(--text-primary);
      font-family: inherit;
      font-size:12px;
      line-height:1.4;
      outline:none;
    }
    .bottombar textarea:focus {
      border:1px solid var(--text-dim);
      box-shadow:0 0 0 2px rgba(255,255,255,0.07);
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="tooltip"></div>
  <div class="status-pill" id="statusPill">initializing…</div>

  <div class="bottombar">
    <div class="labelrow">
      <div class="left">SQL query</div>
    </div>
    <textarea id="queryInput"></textarea>
  </div>

  <script type="module">
    const DATA_URL        = {{ data_url | tojson }};
    const MAPBOX_TOKEN    = {{ mapbox_token | tojson }};
    const MAP_STYLE_URL   = {{ map_style_url | tojson }};
    const DEFAULT_STYLE_URL = {{ default_style_url | tojson }};
    const START_CENTER    = [{{ center_lng }}, {{ center_lat }}];
    const START_ZOOM      = {{ zoom }};
    const TOOLTIP_COLUMNS = {{ tooltip_columns | tojson }};
    const DEFAULT_QUERY   = {{ default_query | tojson }};
    const INITIAL_CONFIG  = {{ config_json | tojson }};

    const { MapboxOverlay, PolygonLayer } = deck;
    const H3HexagonLayer = deck.H3HexagonLayer || (deck.GeoLayers && deck.GeoLayers.H3HexagonLayer);
    const { colorContinuous } = deck.carto;

    let duckConn;
    let overlay;
    let map;
    let didInitialFit = false;
    let sqlTypingTimer = 0;
    const DEBOUNCE_MS = 500;
    let currentConfObj = {};

    const statusPill = document.getElementById("statusPill");

    function updateStatus(text) {
      if (statusPill) statusPill.textContent = text;
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

    function resolveStyleUrl(styleUrl, token) {
      const trimmed = (styleUrl || '').trim();
      if (!trimmed) return 'mapbox://styles/mapbox/dark-v11';
      return trimmed;
    }

    function toH3String(hex) {
      try {
        if (hex == null) return null;
        if (typeof hex === 'string') {
          const s = hex.startsWith('0x') ? hex.slice(2) : hex;
          return (/^\d+$/.test(s) ? BigInt(s).toString(16) : s.toLowerCase());
        }
        if (typeof hex === 'number') return BigInt(Math.trunc(hex)).toString(16);
        if (typeof hex === 'bigint') return hex.toString(16);
        if (Array.isArray(hex) && hex.length === 2) {
          const a = (BigInt(hex[0]) << 32n) | BigInt(hex[1]);
          const b = (BigInt(hex[1]) << 32n) | BigInt(hex[0]);
          const sa = a.toString(16), sb = b.toString(16);
          if (h3.isValidCell?.(sa)) return sa;
          if (h3.isValidCell?.(sb)) return sb;
          return sa;
        }
      } catch(_) {}
      return null;
    }

    function rowsToFeatures(rows, confObj){
      const colorAttr = confObj?.hexLayer?.getFillColor?.attr || "metric";

      return rows.map(p => {
        const rawHex = p.hex ?? p.h3 ?? p.index ?? p.id;
        const hex = toH3String(rawHex);
        if (!hex) return null;

        const obj = { ...p };

        if (obj[colorAttr] === undefined) {
          const lower = colorAttr.toLowerCase();
          if (obj[lower] !== undefined) {
            obj[colorAttr] = obj[lower];
          }
        }

        if (obj[colorAttr] !== undefined) {
          const n = Number(obj[colorAttr]);
          obj[colorAttr] = Number.isFinite(n) ? n : null;
        }

        (TOOLTIP_COLUMNS || []).forEach(col => {
          if (obj[col] === undefined) {
            const lowerCol = col.toLowerCase();
            if (obj[lowerCol] !== undefined) {
              obj[col] = obj[lowerCol];
            }
          }
          if (obj[col] !== undefined && typeof obj[col] !== "number") {
            const nn = Number(obj[col]);
            if (Number.isFinite(nn)) obj[col] = nn;
          }
        });

        obj.hex = hex;

        return {
          ...obj,
          properties: { ...obj }
        };
      }).filter(Boolean);
    }

    function fitToDataOnce(data){
      if (didInitialFit || !data.length) return;
      let minX=Infinity, minY=Infinity, maxX=-Infinity, maxY=-Infinity;
      for (const d of data){
        const [lat,lng] = h3.cellToLatLng(d.hex);
        if (lng<minX)minX=lng;
        if (lat<minY)minY=lat;
        if (lng>maxX)maxX=lng;
        if (lat>maxY)maxY=lat;
      }
      if (minX<Infinity) {
        map.fitBounds([[minX,minY],[maxX,maxY]], {padding:20,duration:0});
        didInitialFit = true;
      }
    }

    function buildDeckLayer(data, confObj) {
      const hexCfg = parseHexLayerConfig(confObj.hexLayer || {});
      return H3HexagonLayer ? new H3HexagonLayer({
        id: 'hex-layer',
        data,
        pickable: true,
        stroked: false,
        filled: true,
        extruded: !!confObj.hexLayer?.extruded,
        coverage: 0.9,
        lineWidthMinPixels: 1,
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

    function handleHoverFactory(confObj){
      const colorAttr = confObj?.hexLayer?.getFillColor?.attr || "metric";
      return function handleHover(info){
        const tooltipEl = document.getElementById("tooltip");
        if(info?.object){
          map.getCanvas().style.cursor='pointer';
          const p = info.object;

          const vals = [];
          if (TOOLTIP_COLUMNS && TOOLTIP_COLUMNS.length){
            TOOLTIP_COLUMNS.forEach(col=>{
              if (p[col] !== undefined){
                const v = p[col];
                if (typeof v === "number" && Number.isFinite(v)) {
                  vals.push(col+": "+v.toFixed(2));
                } else {
                  vals.push(col+": "+v);
                }
              }
            });
          } else {
            const v = p[colorAttr];
            if (v != null) {
              if (typeof v === "number" && Number.isFinite(v)) {
                vals.push(colorAttr+": "+v.toFixed(2));
              } else {
                vals.push(colorAttr+": "+v);
              }
            }
          }
          vals.unshift("hex "+p.hex);

          tooltipEl.innerHTML = vals.join(" • ");
          tooltipEl.style.left = (info.x+10)+"px";
          tooltipEl.style.top  = (info.y+10)+"px";
          tooltipEl.style.display='block';
        } else {
          map.getCanvas().style.cursor='';
          tooltipEl.style.display='none';
        }
      };
    }

    function updateOverlayLayers(data, confObj){
      const layer = buildDeckLayer(data, confObj);
      const onHover = handleHoverFactory(confObj);

      if (!overlay) {
        overlay = new MapboxOverlay({
          interleaved: false,
          layers: [layer],
          onHover
        });
        map.addControl(overlay);
      } else {
        overlay.setProps({
          layers: [layer],
          onHover
        });
      }
    }

    async function initMapAndDuckDB(){
      updateStatus('initializing map…');
      
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
          updateStatus('map ready');
          resolve();
        });
      });

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

      try {
        await duckConn.query("INSTALL h3 FROM community");
      } catch (installErr) {
        console.warn("INSTALL h3 failed:", installErr?.message || installErr);
      }

      try {
        await duckConn.query("LOAD h3");
      } catch (loadErr) {
        console.warn("LOAD h3 failed:", loadErr?.message || loadErr);
      }

      updateStatus('downloading data…');
      const r = await fetch(DATA_URL);
      if(!r.ok){ 
        console.error("http", r.status); 
        throw new Error(r.status); 
      }
      const bytes = new Uint8Array(await r.arrayBuffer());

      await db.registerFileBuffer('data.parquet', bytes);
      await duckConn.query("CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('data.parquet')");
    }

    async function runQuery() {
      if (!duckConn || !currentConfObj) return;

      const inputEl = document.getElementById("queryInput");
      const sqlText = (inputEl?.value || DEFAULT_QUERY).trim() || DEFAULT_QUERY;

      try {
        updateStatus('running query…');
        const res = await duckConn.query(sqlText);
        const rows = res.toArray();

        const data = rowsToFeatures(rows, currentConfObj);
        fitToDataOnce(data);
        updateOverlayLayers(data, currentConfObj);
        updateStatus(`rendered ${data.length.toLocaleString()} features`);
      } catch(e) {
        console.error("SQL error:", e);
        updateStatus('sql error');
      }
    }

    function onSQLChanged(){
      clearTimeout(sqlTypingTimer);
      sqlTypingTimer = setTimeout(runQuery, DEBOUNCE_MS);
    }

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

      const queryInput = document.getElementById("queryInput");
      queryInput.value = DEFAULT_QUERY;
      queryInput.oninput = onSQLChanged;

      await runQuery();
    }

    main().catch(e=>{
      console.error("init error", e);
      updateStatus('init error');
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
        default_style_url=DEFAULT_STYLE_URL,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        tooltip_columns=tooltip_columns,
        default_query=default_query,
    )

    return common.html_to_obj(html)