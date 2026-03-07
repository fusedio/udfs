common = fused.load("https://github.com/fusedio/udfs/tree/3991434/public/common/")

@fused.udf
def udf(
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_Airbnb_listings_nyc_parquet/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -73.9857,
    center_lat: float = 40.7484,
    zoom: int = 9,
    color_field: str = "price_per_person"  
):
    import json

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Generic Points Map (DuckDB-WASM + Mapbox)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <style>
    html, body, #map {{ margin:0; padding:0; height:100%; }}
    .note {{ position:absolute; top:8px; left:8px; font:12px monospace; background:#fff; color:#000; padding:6px 8px; border:1px solid #ccc; border-radius:4px; }}
    .legend {{ position:absolute; bottom:12px; left:12px; background:#fff; border:1px solid #ccc; border-radius:4px; padding:6px 8px; font:12px monospace; }}
    .legend-bar {{ width:200px; height:8px; background: linear-gradient(90deg, #2ecc71, #f1c40f, #e74c3c); margin:6px 0; }}
    .legend-row {{ display:flex; justify-content:space-between; }}
    .pill {{ position:absolute; top:8px; right:8px; background:#fff; border:1px solid #ccc; border-radius:14px; padding:6px 10px; font:12px monospace; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="note" id="note">Initializing…</div>
  <div class="pill" id="pill"></div>

  <div class="legend" id="legend" style="display:none;">
    <div id="legLabel">color</div>
    <div class="legend-bar"></div>
    <div class="legend-row"><span id="legMin">low</span><span id="legMid">mid</span><span id="legMax">high</span></div>
  </div>

  <script type="module">
    const MAPBOX_TOKEN = {json.dumps(mapbox_token)};
    const DATA_URL     = {json.dumps(data_url)};
    const COLOR_FIELD  = {json.dumps(color_field)}; // param from Python

    let map, duckdb, conn;
    let didInitialFit = false;

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
    function setPill(t) {{ const p=document.getElementById('pill'); if (p) p.textContent=t; }}

    async function onLoad() {{
      // Source + layer (colors driven by _color_norm written to properties)
      map.addSource('pts', {{ type:'geojson', data: emptyFC() }});
      map.addLayer({{
        id:'points',
        type:'circle',
        source:'pts',
        paint: {{
          'circle-color': [
            'interpolate', ['linear'], ['coalesce', ['get','_color_norm'], 0.5],
            0, '#2ecc71', 0.5, '#f1c40f', 1, '#e74c3c'
          ],
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
        const val = p._color_val !== undefined ? Number(p._color_val) : null;
        const label = (p._color_label || COLOR_FIELD);
        const html = `
          <div><b>${{label}}:</b> ${{val !== null && Number.isFinite(val) ? val.toFixed(2) : 'n/a'}}</div>
        `;
        popup.setLngLat(e.lngLat).setHTML(html).addTo(map);
      }});

      try {{
        setNote('Initializing DuckDB…');
        await initDuckDB();
        setNote('Fetching Parquet…');
        const buf = await fetchParquet(DATA_URL);
        setNote('Loading data…');
        await loadParquet(buf);
        setNote('Reading rows…');
        await plotAllPoints();
        setNote('Done');
      }} catch (e) {{
        console.error(e);
        setNote('Error: ' + (e?.message || e));
      }}
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

    // Helpers
    function safeVal(v) {{
      if (typeof v === 'bigint') {{
        const n = Number(v);
        return Number.isSafeInteger(n) ? n : v.toString();
      }}
      return v;
    }}

    function detectLatLon(fields) {{
      const names = fields.map(f => f.name.toLowerCase());
      const latCandidates = ['lat','latitude','y'];
      const lonCandidates = ['lon','lng','long','longitude','x'];
      let lat=null, lon=null;
      for (const c of latCandidates) if (names.includes(c)) {{ lat = fields[names.indexOf(c)].name; break; }}
      for (const c of lonCandidates) if (names.includes(c)) {{ lon = fields[names.indexOf(c)].name; break; }}
      return {{ lat, lon }};
    }}

    function arrowToGeoJSON(result, latKey, lonKey) {{
      const rows = result.toArray();
      const feats = [];
      for (const row of rows) {{
        const lat = Number(safeVal(row[latKey]));
        const lon = Number(safeVal(row[lonKey]));
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

        // keep a few handy props
        const p = {{
          price_in_dollar: safeVal(row["price_in_dollar"]),
          accommodates: safeVal(row["accommodates"]),
          review_scores_rating: safeVal(row["review_scores_rating"])
        }};
        // also forward candidate color field if it exists as raw column
        if (row.hasOwnProperty(COLOR_FIELD)) p[COLOR_FIELD] = safeVal(row[COLOR_FIELD]);

        feats.push({{
          type:'Feature',
          geometry: {{ type:'Point', coordinates:[lon, lat] }},
          properties: p
        }});
      }}
      return {{ type:'FeatureCollection', features: feats }};
    }}

    // derive color value (generic); supports special "price_per_person"
    function computeColorValue(props) {{
      if (COLOR_FIELD === "price_per_person") {{
        const price = Number(props.price_in_dollar);
        const acc = Number(props.accommodates);
        if (Number.isFinite(price) && Number.isFinite(acc) && acc > 0) return price / acc;
        return null;
      }}
      const raw = props[COLOR_FIELD];
      const num = Number(raw);
      return Number.isFinite(num) ? num : null;
    }}

    function normalizeAndAnnotate(fc) {{
      if (!fc.features.length) return fc;

      const vals = [];
      for (const f of fc.features) {{
        const v = computeColorValue(f.properties || (f.properties={{}}));
        if (v !== null) {{
          f.properties._color_val = v;  // raw value for tooltip
          vals.push(v);
        }}
      }}
      if (!vals.length) {{
        document.getElementById('legend').style.display = 'none';
        setPill(`color: ${{COLOR_FIELD}} (no numeric data)`);
        return fc;
      }}

      // robust 5–95% clip
      vals.sort((a,b)=>a-b);
      const q = (arr, t) => {{
        const i = (arr.length - 1) * t;
        const lo = Math.floor(i), hi = Math.ceil(i);
        if (lo === hi) return arr[lo];
        return arr[lo] * (hi - i) + arr[hi] * (i - lo);
      }};
      const p5 = q(vals, 0.05), p95 = q(vals, 0.95);
      const span = Math.max(1e-9, p95 - p5);

      for (const f of fc.features) {{
        const v = f.properties?._color_val;
        if (Number.isFinite(v)) {{
          let t = (v - p5) / span;
          if (t < 0) t = 0; else if (t > 1) t = 1;
          f.properties._color_norm = t;
          f.properties._color_label = COLOR_FIELD;
        }}
      }}

      // legend + pill
      document.getElementById('legend').style.display = 'block';
      document.getElementById('legLabel').textContent = COLOR_FIELD;
      document.getElementById('legMin').textContent = String(p5.toFixed(0));
      document.getElementById('legMid').textContent = String(((p5+p95)/2).toFixed(0));
      document.getElementById('legMax').textContent = String(p95.toFixed(0));
      setPill(`color: ${{COLOR_FIELD}}`);

      return fc;
    }}

    async function plotAllPoints() {{
      const res = await conn.query("SELECT * FROM df");
      const fields = res.schema.fields;
      const {{ lat, lon }} = detectLatLon(fields);
      if (!lat || !lon) throw new Error("No latitude/longitude columns found");

      let gj = arrowToGeoJSON(res, lat, lon);
      gj = normalizeAndAnnotate(gj);

      map.getSource('pts').setData(gj);

      if (!didInitialFit && gj.features.length) {{
        fitToOnce(gj);
        didInitialFit = true;
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
