common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")

@fused.udf
def udf(
    data_url: str = "https://staging.fused.io/server/v1/realtime-shared/UDF_Airbnb_listings_nyc_parquet/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -73.9857,
    center_lat: float = 40.7484,
    zoom: int = 9,
    initial_sql: str = "SELECT * FROM df LIMIT 100;"
):
    import json

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Points Map (DuckDB-WASM + Mapbox)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <style>
    html, body, #map {{ margin:0; padding:0; height:100%; }}
    .note {{ position:absolute; top:8px; left:8px; font:12px monospace; background:#fff; color:#000; padding:6px 8px; border:1px solid #ccc; border-radius:4px; }}
    .panel {{ position:absolute; top:8px; right:8px; width:420px; display:flex; flex-direction:column; gap:6px; }}
    textarea {{ width:100%; height:120px; font-family: monospace; padding:6px; border:1px solid #ccc; border-radius:4px; }}
    button {{ width:80px; padding:6px; font-family: monospace; }}
    .status {{ font:12px monospace; color:#333; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="note" id="note">Initializing…</div>

  <div class="panel">
    <textarea id="queryInput" placeholder="SELECT * FROM df LIMIT 100;">{initial_sql}</textarea>
    <div style="display:flex; align-items:center; gap:8px;">
      <button id="runBtn" disabled>Run</button>
      <span class="status" id="status">Loading DuckDB and dataset…</span>
    </div>
  </div>

  <script type="module">
    const MAPBOX_TOKEN = {json.dumps(mapbox_token)};
    const DATA_URL     = {json.dumps(data_url)};

    let map, duckdb, conn;
    let didInitialFit = false;
    let typingTimer = 0;
    const DEBOUNCE_MS = 250;

    // --- Map setup ---
    mapboxgl.accessToken = MAPBOX_TOKEN;
    map = new mapboxgl.Map({{
      container: 'map',
      style: 'mapbox://styles/mapbox/light-v11',
      center: [{center_lng}, {center_lat}],
      zoom: {zoom},
      dragRotate: false, pitchWithRotate: false
    }});
    map.on('load', onLoad);

    function setNote(t) {{ const n=document.getElementById('note'); if (n) n.textContent=t; }}
    function setStatus(t) {{ const s=document.getElementById('status'); if (s) s.textContent=t; }}

    async function onLoad() {{
      map.addSource('pts', {{ type:'geojson', data: emptyFC() }});
      map.addLayer({{
        id:'points',
        type:'circle',
        source:'pts',
        paint: {{
          'circle-color':'#1f78b4',
          'circle-radius': 3,
          'circle-opacity': 0.85,
          'circle-stroke-color': '#ffffff',
          'circle-stroke-width': 0.5
        }}
      }});

      const popup = new mapboxgl.Popup({{ closeButton:false, closeOnClick:false }});
      map.on('mouseenter','points', () => map.getCanvas().style.cursor = 'pointer');
      map.on('mouseleave','points', () => {{ map.getCanvas().style.cursor=''; popup.remove(); }});
      map.on('mousemove','points', (e) => {{
        const f = e.features?.[0]; if(!f) return;
        const p = f.properties || {{}};
        const keys = Object.keys(p);
        const html = keys.slice(0,10).map(k => `<div><b>${{k}}:</b> ${{p[k]}}</div>`).join('');
        popup.setLngLat(e.lngLat).setHTML(html).addTo(map);
      }});

      try {{
        setNote('Initializing DuckDB…');
        await initDuckDB();
        setNote('Fetching Parquet…');
        const buf = await fetchParquet(DATA_URL);
        setNote('Loading data…');
        await loadParquet(buf);
        setNote('Data ready');
        document.getElementById('runBtn').disabled = false;

        // Auto-run initial SQL
        await runQuery();
      }} catch (e) {{
        console.error(e);
        setNote('Error: ' + (e?.message || e));
      }}

      // Wire up textarea & button
      const q = document.getElementById('queryInput');
      q.addEventListener('input', () => {{
        clearTimeout(typingTimer);
        typingTimer = setTimeout(runQuery, DEBOUNCE_MS);
      }});
      document.getElementById('runBtn').addEventListener('click', runQuery);
      document.addEventListener('keydown', (e) => {{
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') runQuery();
      }});
    }}

    const emptyFC = () => ({{ type:'FeatureCollection', features:[] }});

    async function initDuckDB() {{
      const m = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm');
      const b = await m.selectBundle(m.getJsDelivrBundles());
      const w = new Worker(URL.createObjectURL(new Blob([await (await fetch(b.mainWorker)).text()], {{type:'application/javascript'}})));
      duckdb = new m.AsyncDuckDB(new m.ConsoleLogger(), w);
      await duckdb.instantiate(b.mainModule);
      conn = await duckdb.connect();
    }}

    async function fetchParquet(url) {{
      const r = await fetch(url); if(!r.ok) throw new Error(`HTTP ${{r.status}}`);
      return new Uint8Array(await r.arrayBuffer());
    }}

    async function loadParquet(bytes) {{
      await duckdb.registerFileBuffer('data.parquet', bytes);
      await conn.query("CREATE OR REPLACE VIEW df AS SELECT * FROM read_parquet('data.parquet')");
    }}

    // ---- JSON-safe coercion for properties (fixes BigInt serialization) ----
    function safeVal(v) {{
      if (typeof v === 'bigint') {{
        const n = Number(v);
        return Number.isSafeInteger(n) ? n : v.toString(); // keep precision if too big
      }}
      if (v && typeof v === 'object') {{
        // shallow convert objects/arrays if they sneak in
        if (Array.isArray(v)) return v.map(safeVal);
        const out = {{}};
        for (const [k,val] of Object.entries(v)) out[k] = safeVal(val);
        return out;
      }}
      return v;
    }}

    // Detect latitude/longitude column names from an Arrow result schema
    function detectLatLon(fields) {{
      const names = fields.map(f => f.name.toLowerCase());
      const latCandidates = ['lat','latitude','y'];
      const lonCandidates = ['lon','lng','long','longitude','x'];
      let lat=null, lon=null;
      for (const c of latCandidates) if (names.includes(c)) {{ lat = fields[names.indexOf(c)].name; break; }}
      for (const c of lonCandidates) if (names.includes(c)) {{ lon = fields[names.indexOf(c)].name; break; }}
      return {{ lat, lon }};
    }}

    // Convert an Arrow table (query result) to GeoJSON using given lat/lon columns
    function arrowToGeoJSON(result, latKey, lonKey) {{
      const rows = result.toArray();
      const feats = [];
      for (const row of rows) {{
        // Coerce lat/lon safely (BigInt → Number/String)
        const rawLat = safeVal(row[latKey]);
        const rawLon = safeVal(row[lonKey]);
        const lat = Number(rawLat);
        const lon = Number(rawLon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

        // include up to ~20 props, JSON-safe
        const props = {{}};
        let count = 0;
        for (const k of Object.keys(row)) {{
          if (k === latKey || k === lonKey) continue;
          props[k] = safeVal(row[k]);
          if (++count >= 20) break;
        }}
        feats.push({{
          type:'Feature',
          geometry: {{ type:'Point', coordinates:[lon, lat] }},
          properties: props
        }});
      }}
      return {{ type:'FeatureCollection', features: feats }};
    }}

    async function runQuery() {{
      if (!conn) return;
      const sql = document.getElementById('queryInput').value.trim();
      if (!sql) return;
      try {{
        setStatus('Running…');
        const res = await conn.query(sql);

        // detect lat/lon from the result set (so aliases work)
        const fields = res.schema.fields;
        const {{ lat, lon }} = detectLatLon(fields);
        if (!lat || !lon) {{
          setStatus('No lat/lon columns found in result');
          map.getSource('pts').setData(emptyFC());
          return;
        }}

        const gj = arrowToGeoJSON(res, lat, lon);
        // At this point, gj is JSON-safe (no BigInt anywhere)
        map.getSource('pts').setData(gj);

        if (!didInitialFit && gj.features.length) {{
          fitToOnce(gj);
          didInitialFit = true;
        }}
        setStatus('Done');
      }} catch (e) {{
        console.error(e);
        setStatus('Error: ' + (e?.message || e));
      }}
    }}

    function fitToOnce(gj) {{
      if (!gj.features.length) return;
      let minX=  Infinity, minY=  Infinity, maxX= -Infinity, maxY= -Infinity;
      for (const f of gj.features) {{
        const [x,y] = f.geometry.coordinates;
        if (x<minX)minX=x; if (y<minY)minY=y; if (x>maxX)maxX=x; if (y>maxY)maxY=y;
      }}
      map.fitBounds([[minX,minY],[maxX,maxY]], {{ padding: 20, duration: 0 }});
    }}
  </script>
</body>
</html>"""
    return common.html_to_obj(html)
