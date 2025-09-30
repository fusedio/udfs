common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")

@fused.udf
def udf(
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_Airbnb_listings_nyc_parquet/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
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
    #map {{ position:fixed; inset:0; }}
    .note {{ position:absolute; top:8px; left:8px; font:12px monospace; background:#fff; color:#000; padding:6px 8px; border:1px solid #ccc; border-radius:4px; z-index:2; }}
    .legend {{ position:absolute; bottom:92px; left:12px; background:#fff; border:1px solid #ccc; border-radius:4px; padding:6px 8px; font:12px monospace; z-index:2; }}
    .legend-bar {{ width:200px; height:8px; background: linear-gradient(90deg, #2ecc71, #f1c40f, #e74c3c); margin:6px 0; }}
    .legend-row {{ display:flex; justify-content:space-between; }}
    .bottombar {{
      position:fixed; left:0; right:0; bottom:0; z-index:3;
      background: rgba(255,255,255,0.96); border-top:1px solid #ccc;
      padding:8px;
      box-shadow: 0 -4px 12px rgba(0,0,0,0.06);
      font-family: monospace;
    }}
    .bottombar textarea {{ width:100%; height:80px; resize:vertical; padding:6px; border:1px solid #bbb; border-radius:4px; }}
    @media (max-width: 640px) {{
      .legend {{ bottom: 140px; }}
    }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="note" id="note">Initializing…</div>

  <div class="legend" id="legend" style="display:none;">
    <div>price per person</div>
    <div class="legend-bar"></div>
    <div class="legend-row"><span id="legMin">low</span><span id="legMid">mid</span><span id="legMax">high</span></div>
  </div>

  <!-- Bottom SQL bar (input only) -->
  <div class="bottombar">
    <textarea id="queryInput" placeholder="SELECT * FROM df LIMIT 100;">{initial_sql}</textarea>
  </div>

  <script type="module">
    const MAPBOX_TOKEN = {json.dumps(mapbox_token)};
    const DATA_URL     = {json.dumps(data_url)};

    let map, duckdb, conn;
    let didInitialFit = false;
    let typingTimer = 0;
    const DEBOUNCE_MS = 250;

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

    async function onLoad() {{
      map.addSource('pts', {{ type:'geojson', data: emptyFC() }});
      map.addLayer({{
        id:'points',
        type:'circle',
        source:'pts',
        paint: {{
          'circle-color': [
            'interpolate', ['linear'], ['coalesce', ['get','_pp_norm'], 0.5],
            0, '#2ecc71',
            0.5, '#f1c40f',
            1, '#e74c3c'
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
        const pricePP = p._pp !== undefined ? Number(p._pp).toFixed(2) : 'n/a';
        const html = `<div><b>price_per_person:</b> $${{pricePP}}</div>` +
                     Object.keys(p).slice(0,8).filter(k => !['_pp','_pp_norm'].includes(k))
                       .map(k => `<div><b>${{k}}:</b> ${{p[k]}}</div>`).join('');
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
        await runQuery();
      }} catch (e) {{
        console.error(e);
        setNote('Error: ' + (e?.message || e));
      }}

      document.getElementById('queryInput').addEventListener('input', () => {{
        clearTimeout(typingTimer);
        typingTimer = setTimeout(runQuery, DEBOUNCE_MS);
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

    function safeVal(v) {{
      if (typeof v === 'bigint') {{
        const n = Number(v);
        return Number.isSafeInteger(n) ? n : v.toString();
      }}
      if (v && typeof v === 'object') {{
        if (Array.isArray(v)) return v.map(safeVal);
        const out = {{}};
        for (const [k,val] of Object.entries(v)) out[k] = safeVal(val);
        return out;
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

    function addPricePerPersonAndNormalize(fc) {{
      if (!fc.features.length) return fc;
      const vals = [];
      for (const f of fc.features) {{
        const p = f.properties || (f.properties = {{}});

        let pp = p.price_per_person;
        if (pp === undefined) {{
          const price = Number(p.price_in_dollar);
          const acc = Number(p.accommodates);
          if (Number.isFinite(price) && Number.isFinite(acc) && acc > 0) {{
            pp = price / acc;
            p.price_per_person = pp;
          }}
        }}
        if (Number.isFinite(pp)) {{
          p._pp = pp;
          vals.push(pp);
        }}
      }}
      if (vals.length) {{
        vals.sort((a,b)=>a-b);
        const q = (arr, t) => {{
          const i = (arr.length - 1) * t;
          const lo = Math.floor(i), hi = Math.ceil(i);
          if (lo === hi) return arr[lo];
          return arr[lo] * (hi - i) + arr[hi] * (i - lo);
        }};
        const p5  = q(vals, 0.05);
        const p95 = q(vals, 0.95);
        const span = Math.max(1e-9, p95 - p5);

        for (const f of fc.features) {{
          const v = f.properties?._pp;
          if (Number.isFinite(v)) {{
            let t = (v - p5) / span;
            if (t < 0) t = 0; else if (t > 1) t = 1;
            f.properties._pp_norm = t;
          }}
        }}

        document.getElementById('legend').style.display = 'block';
        document.getElementById('legMin').textContent = '$' + p5.toFixed(0);
        document.getElementById('legMid').textContent = '$' + ((p5+p95)/2).toFixed(0);
        document.getElementById('legMax').textContent = '$' + p95.toFixed(0);
      }} else {{
        document.getElementById('legend').style.display = 'none';
      }}
      return fc;
    }}

    async function runQuery() {{
      if (!conn) return;
      const sql = document.getElementById('queryInput').value.trim();
      if (!sql) return;
      try {{
        const res = await conn.query(sql);
        const fields = res.schema.fields;
        const {{ lat, lon }} = detectLatLon(fields);
        if (!lat || !lon) {{
          map.getSource('pts').setData(emptyFC());
          document.getElementById('legend').style.display = 'none';
          return;
        }}
        let gj = arrowToGeoJSON(res, lat, lon);
        gj = addPricePerPersonAndNormalize(gj);
        map.getSource('pts').setData(gj);
        if (!didInitialFit && gj.features.length) {{
          fitToOnce(gj);
          didInitialFit = true;
        }}
      }} catch (e) {{
        console.error(e);
        setNote('Error: ' + (e?.message || e));
      }}
    }}

    function fitToOnce(gj) {{
      if (!gj.features.length) return;
      let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
      for (const f of gj.features) {{
        const [x,y] = f.geometry.coordinates;
        if (x<minX)minX=x; if (y<minY)minY=y; if (x>maxX)maxX=x; if (y>maxY)maxY=y;
      }}
      map.fitBounds([[minX,minY],[maxX,maxY]], {{ padding:20, duration:0 }});
    }}
  </script>
</body>
</html>"""
    return common.html_to_obj(html)
