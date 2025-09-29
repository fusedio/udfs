common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")

@fused.udf
def udf(
    data_url: str = "https://staging.fused.io/server/v1/realtime-shared/fsh_2ogfbAa1ORr5VKMkwv7Er9/run/file?dtype_out_raster=png&dtype_out_vector=parquet&hex_res=5",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -96.0,
    center_lat: float = 39.0,
    zoom: int = 4,
    auto_fetch: bool = True,
    initial_filter: str = "data = 2"
):
    import json
    
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>H3 Hex Map (Instant Filter + Tooltip)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet" />
  <style>
    :root {{
      --lime: #E8FF59;
      --bg: #1c1c1c;
      --text: #ddd;
      --border: #444;
    }}
    html, body, #map {{ margin:0; padding:0; height:100%; background:#111; }}
    .note {{ position:absolute; top:8px; left:8px; font:12px/1.2 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif; color:#bbb; background:rgba(0,0,0,.4); padding:6px 8px; border-radius:6px; }}

    /* —— 1.5× BIGGER STATS BOX —— */
    .stats {{
      position:absolute; bottom:32px; left:8px;
      display:flex; align-items:center; gap:14px; flex-wrap:wrap;
      font:18px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;
      color:var(--text); background:var(--bg); padding:14px 18px; border-radius:10px;
      border:1px solid var(--border);
    }}
    .stats .k {{ color:var(--lime); margin-right:6px; font-weight:600; }}
    .stats b {{ color:#fff; font-weight:700; }}
    .stats .sep {{ color:#555; }}
    .stats .u {{ color:var(--lime); margin-left:6px; }}

    /* —— 1.5× BIGGER SEARCH BAR —— */
    .filter {{
      position:absolute; top:8px; right:8px; display:flex; gap:8px; align-items:center;
      background:rgba(0,0,0,.4); padding:8px 10px; border-radius:10px;
    }}
    .filter input {{
      width:510px; padding:12px 16px; border:1px solid var(--border); background:var(--bg); color:#fff;
      border-radius:10px; font:21px -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;
      transition: box-shadow .12s ease, border-color .12s ease;
    }}
    .filter input::placeholder {{ color: rgba(232,255,89,0.7); }}
    .filter input:focus {{ outline:none; border-color:var(--lime); box-shadow: 0 0 12px 2px rgba(232,255,89,0.14); }}

    .mapboxgl-popup {{ max-width: 320px; font:13px -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif; }}
    .mapboxgl-popup-content {{ background:#0f0f0f; color:#eee; border:1px solid #333; }}
    .mapboxgl-popup-tip {{ border-top-color:#0f0f0f !important; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="note" id="note">Loading…</div>

  <!-- Stats pill: Area in million m² -->
  <div class="stats" id="stats">
    <span><span class="k">Count</span><b id="stCount">–</b></span>
    <span class="sep">•</span>
    <span><span class="k">Area</span><b id="stArea">–</b><span class="u">million m²</span></span>
    <span class="sep">•</span>
    <span><span class="k">Avg</span><b id="stPct">–</b><span class="u">%</span></span>
  </div>

  <div class="filter">
    <input id="filterInput" placeholder="SQL WHERE… e.g. data = 111" />
  </div>

  <script type="module">
    const MAPBOX_TOKEN = {json.dumps(mapbox_token)};
    const DATA_URL     = {json.dumps(data_url)};
    const AUTO_FETCH   = {str(auto_fetch).lower()};
    const INIT_FILTER  = {json.dumps(initial_filter)};

    let map, duckdb, conn, currentFilter = INIT_FILTER;
    let didInitialFit = false;

    mapboxgl.accessToken = MAPBOX_TOKEN;
    map = new mapboxgl.Map({{
      container: 'map',
      style: 'mapbox://styles/mapbox/dark-v10',
      center: [{center_lng}, {center_lat}],
      zoom: {zoom},
      dragRotate: false, pitchWithRotate: false
    }});
    map.on('load', onLoad);

    async function onLoad() {{
      map.addSource('h3', {{ type:'geojson', data: emptyFC() }});
      map.addLayer({{ id:'h3-fill', type:'fill', source:'h3',
        paint: {{ 'fill-color': [
          'interpolate',['linear'],['get','pct'],
          0,'#2E294E', 1,'#1B998B', 5,'#C5D86D', 15,'#F7931E', 30,'#FFD23F', 50,'#E8FF59'
        ], 'fill-opacity':0.8 }} }});
      map.addLayer({{ id:'h3-line', type:'line', source:'h3',
        paint: {{ 'line-color':'#fff', 'line-width':0.3, 'line-opacity':0.35 }} }});

      const popup = new mapboxgl.Popup({{ closeButton:false, closeOnClick:false }});
      map.on('mouseenter','h3-fill', () => map.getCanvas().style.cursor = 'pointer');
      map.on('mouseleave','h3-fill', () => {{ map.getCanvas().style.cursor=''; popup.remove(); }});
      map.on('mousemove','h3-fill', (e) => {{
        if (!e.features?.length) return;
        const p = e.features[0].properties || {{}};
        const pct  = isFinite(+p.pct)  ? (+p.pct).toFixed(2)   : p.pct;
        const area = isFinite(+p.area) ? (+p.area).toFixed(2) : p.area; // m² in tooltip
        popup.setLngLat(e.lngLat).setHTML(
          '<div><b>data:</b> ' + p.data + '</div>' +
          '<div><b>pct:</b> ' + pct + ' <span style="color:#E8FF59">%</span></div>' +
          '<div><b>area:</b> ' + area + ' <span style="color:#E8FF59">m²</span></div>' +
          '<div style="opacity:.7"><b>hex:</b> ' + p.hex + '</div>'
        ).addTo(map);
      }});

      const input = document.getElementById('filterInput');
      input.value = currentFilter;
      input.addEventListener('input', applyFilterFromInput);
      input.addEventListener('keypress', (e) => {{ if (e.key === 'Enter') applyFilterFromInput(); }});

      if (!AUTO_FETCH) {{ setNote('Map ready (AUTO_FETCH=false)'); return; }}

      try {{
        setNote('Initializing DuckDB…'); await initDuckDB();
        setNote('Fetching Parquet…');     const buf = await fetchParquet(DATA_URL);
        setNote('Loading data…');         await loadParquet(buf);
        setNote('Applying filter…');      await applyFilter(currentFilter);
        const gj = await toGeoJSON();
        map.getSource('h3').setData(gj);
        fitToOnce(gj);
        await updateStats();
        clearNote();
      }} catch (e) {{ setNote('Error: ' + (e?.message||e)); console.error(e); }}
    }}

    function setNote(t) {{ const n=document.getElementById('note'); if(n) n.textContent=t; }}
    function clearNote() {{ const n=document.getElementById('note'); if(n) n.remove(); }}
    const emptyFC = () => ({{ type:'FeatureCollection', features:[] }});

    async function initDuckDB() {{
      const m = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm');
      const b = await m.selectBundle(m.getJsDelivrBundles());
      const w = new Worker(URL.createObjectURL(new Blob([await (await fetch(b.mainWorker)).text()],{{type:'application/javascript'}})));
      duckdb = new m.AsyncDuckDB(new m.ConsoleLogger(), w);
      await duckdb.instantiate(b.mainModule);
      conn = await duckdb.connect();
      try {{ await conn.query('INSTALL spatial; LOAD spatial;'); }} catch {{}}
      try {{ await conn.query('INSTALL h3 FROM community; LOAD h3;'); }} catch {{}}
    }}

    async function fetchParquet(url) {{
      const r = await fetch(url); if(!r.ok) throw new Error(`HTTP ${{r.status}}`);
      return new Uint8Array(await r.arrayBuffer());
    }}

    async function loadParquet(bytes) {{
      await duckdb.registerFileBuffer('data.parquet', bytes);
      await conn.query('DROP TABLE IF EXISTS spatial_data_full;');
      await conn.query(`
        CREATE TABLE spatial_data_full AS
        SELECT row_number() OVER() AS id,
               CAST(hex AS BIGINT) AS h3_cell,
               CAST(data AS INTEGER) AS data,
               CAST(area AS DOUBLE) AS area,
               CAST(pct  AS DOUBLE) AS pct
        FROM read_parquet('data.parquet')
        WHERE hex IS NOT NULL
      `);
    }}

    async function applyFilter(expr) {{
      await conn.query('DROP TABLE IF EXISTS spatial_data;');
      await conn.query('CREATE TABLE spatial_data AS SELECT * FROM spatial_data_full WHERE (' + expr + ')');
    }}

    async function toGeoJSON() {{
      const res = await conn.query(`
        SELECT
          '{{"type":"FeatureCollection","features":[' ||
            string_agg(
              '{{"type":"Feature","geometry":' ||
                ST_AsGeoJSON(ST_GeomFromText(h3_cell_to_boundary_wkt(h3_cell))) ||
                ',"properties":{{"hex":"' || h3_cell || '","data":'||data||',"area":'||area||',"pct":'||pct||'}}}}',
              ','
            )
          || ']}}' AS gj
        FROM spatial_data
        WHERE h3_is_valid_cell(h3_cell)
      `);
      const rows=res.toArray(); return JSON.parse(rows?.[0]?.gj || '{{"type":"FeatureCollection","features":[]}}');
    }}

    function fitToOnce(gj) {{
      if (didInitialFit || !gj.features.length) return;
      let minX=1e9,minY=1e9,maxX=-1e9,maxY=-1e9;
      for (const f of gj.features) for (const ring of f.geometry.coordinates) for (const [x,y] of ring) {{
        if(x<minX)minX=x; if(y<minY)minY=y; if(x>maxX)maxX=x; if(y>maxY)maxY=y;
      }}
      map.fitBounds([[minX,minY],[maxX,maxY]],{{padding:20,duration:0}});
      didInitialFit = true;
    }}

    async function applyFilterFromInput() {{
      const val = document.getElementById('filterInput').value.trim();
      if (!val || val === currentFilter) return;
      currentFilter = val;
      try {{
        setNote('Filtering…');
        const view = map.getCenter(), z = map.getZoom();
        await applyFilter(currentFilter);
        const gj = await toGeoJSON();
        map.getSource('h3').setData(gj);
        map.jumpTo({{ center: view, zoom: z }});
        await updateStats();
        clearNote();
      }} catch(e) {{
        setNote('Filter error: ' + (e?.message||e));
        console.error(e);
      }}
    }}

    // —— Stats: convert total area m² → million m² for display
    async function updateStats() {{
      try {{
        const q = await conn.query(`
          SELECT COUNT(*) AS cnt, COALESCE(SUM(area),0) AS total_area_m2, AVG(pct) AS avg_pct
          FROM spatial_data
        `);
        const r = q.toArray()?.[0] || {{ cnt:0, total_area_m2:0, avg_pct:0 }};

        const million_m2 = (+r.total_area_m2 || 0) / 1_000_000;

        const fmtMil = (v) => {{
          v = +v || 0;
          if (v < 1) return v.toFixed(3);
          if (v < 100) return v.toFixed(2);
          if (v < 10000) return v.toFixed(1);
          return v.toLocaleString(undefined, {{ maximumFractionDigits: 0 }});
        }};
        const fmtPct = (v) => (isFinite(+v) ? (+v).toFixed(2) : '0.00');

        document.getElementById('stCount').textContent = Number(r.cnt||0).toLocaleString();
        document.getElementById('stArea').textContent  = fmtMil(million_m2);
        document.getElementById('stPct').textContent   = fmtPct(r.avg_pct||0);
      }} catch(e) {{
        console.error('[stats] error:', e);
      }}
    }}
  </script>
</body>
</html>"""
    return common.html_to_obj(html)
