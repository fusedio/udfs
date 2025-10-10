common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")

@fused.udf()
def udf(
    data_url: str = "https://staging.fused.io/server/v1/realtime-shared/fsh_603UJrHzEiwDvEaHJtW2lC/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = 20.0,
    center_lat: float = 30.0,
    zoom: float = 2.0,
    num_bins: int = 30,
    hist_field: str = "status_year"  # or "discovery_year"
):
    from jinja2 import Template

    html = Template(r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Oil & Gas — Map • Filters • Histogram • Counter</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://d3js.org/d3.v7.min.js"></script>

  <!-- noUiSlider (dual-handle) -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/nouislider@15.8.1/dist/nouislider.min.css">
  <script src="https://cdn.jsdelivr.net/npm/nouislider@15.8.1/dist/nouislider.min.js"></script>

  <style>
    :root { --bg:#0b0b0f; --panel:#15151a; --border:#2a2a33; --text:#eaeaea; --accent:#E8FF59; --track:#2b2b34; --range:#7cff72; }
    html,body { height:100%; margin:0; background:var(--bg); color:var(--text); font:13px/1.4 Inter,system-ui,sans-serif; }
    #wrap { display:grid; grid-template-rows: 92px 1fr 230px; height:100%; gap:10px; padding:10px; box-sizing:border-box; }

    .topbar { display:grid; grid-template-columns: auto 1fr auto; align-items:center; gap:10px; background:var(--panel);
              border:1px solid var(--border); border-radius:10px; padding:10px; }
    .pill { display:flex; align-items:center; gap:8px; padding:6px 10px; background:#00000055; border:1px solid var(--border); border-radius:10px; white-space:nowrap; }
    .dot { width:8px; height:8px; background:var(--accent); border-radius:999px; }
    .controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
    label { font-size:11px; opacity:.85; margin-right:6px; white-space:nowrap; }

    /* Dropdown */
    .dd { position:relative; }
    .dd-btn { background:#1b1b22; color:#fff; border:1px solid var(--border); border-radius:8px; padding:6px 10px; cursor:pointer; }
    .dd-panel { position:absolute; top:110%; left:0; min-width:240px; max-height:260px; overflow:auto;
                background:#0e0e12; border:1px solid var(--border); border-radius:10px; padding:8px; z-index:20; display:none; }
    .dd.open .dd-panel { display:block; }
    .dd-search { width:100%; padding:6px 8px; margin-bottom:8px; border:1px solid var(--border); border-radius:8px; background:#121218; color:#fff; }
    .dd-list label { display:flex; align-items:center; gap:8px; padding:4px 2px; cursor:pointer; }
    .dd-list input { accent-color: var(--accent); }

    select { background:#1b1b22; color:#fff; border:1px solid var(--border); border-radius:8px; padding:6px 8px; }

    /* Year dual slider container */
    .year-block { display:flex; align-items:center; gap:10px; min-width:280px; }
    .dual { width:360px; }

    /* noUiSlider theme tweaks */
    #yearSlider .noUi-connect { background: var(--range); }
    #yearSlider .noUi-base, #yearSlider .noUi-target { background: var(--track); }
    #yearSlider .noUi-handle { box-shadow:0 0 0 3px rgba(232,255,89,0.25); border:1px solid #000; }
    #yearSlider .noUi-tooltip { background:#00000088; border:1px solid var(--border); color:#fff; font-size:11px; }

    /* Map */
    #mapBox { position:relative; border:1px solid var(--border); border-radius:12px; overflow:hidden; }
    #map { position:absolute; inset:0; }
    .counter {
      position:absolute; left:12px; top:12px; z-index:5;
      background:#00000077; backdrop-filter: blur(2px);
      border:1px solid var(--border); border-radius:10px; padding:10px 12px;
      display:flex; flex-direction:column; gap:2px; min-width:160px;
    }
    .counter .label { font-size:11px; opacity:.85; }
    .counter .value { font-size:28px; font-weight:800; color:var(--accent); line-height:1; }
    .counter .sub   { font-size:11px; opacity:.75; }

    /* Map tooltip */
    #mapTip {
      position:absolute; z-index:6; pointer-events:none;
      display:none; max-width:280px;
      background:#0f0f14; border:1px solid var(--border); color:#fff;
      padding:8px 10px; border-radius:8px; box-shadow:0 6px 16px rgba(0,0,0,.35);
      font-size:12px; line-height:1.35;
    }
    #mapTip .h { color:var(--accent); font-weight:600; margin-bottom:4px; }
    #mapTip .row { display:flex; gap:6px; }
    #mapTip .k { opacity:.65; min-width:92px; }
    #mapTip .v { opacity:.95; }
    .mapboxgl-canvas { cursor: default; }

    /* Bottom histogram panel */
    #panel { background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:10px; display:grid; grid-template-rows: 24px 1fr 18px; }
    #histTitle { font-size:12px; color:var(--accent); }
    #histBox { position:relative; }
    #tooltip { position:absolute; pointer-events:none; display:none; background:#111; border:1px solid var(--border); color:#fff; padding:6px 8px; border-radius:6px; font-size:12px; z-index:5; }
    .legend { font-size:11px; opacity:.75; display:flex; justify-content:space-between; }
  </style>
</head>
<body>
  <div id="wrap">
    <div class="topbar">
      <div class="pill"><div class="dot"></div><div id="status">loading…</div></div>

      <div class="controls">
        <label>Status</label>
        <!-- Dropdown -->
        <div class="dd" id="statusDD">
          <button class="dd-btn" id="statusBtn" aria-expanded="false">All</button>
          <div class="dd-panel" role="menu">
            <input class="dd-search" id="statusSearch" placeholder="Search…">
            <div class="dd-list" id="statusList"></div>
          </div>
        </div>

        <label>Year field</label>
        <select id="metricSel">
          <option value="status_year" {{ 'selected' if hist_field=='status_year' else '' }}>Status year</option>
          <option value="discovery_year" {{ 'selected' if hist_field=='discovery_year' else '' }}>Discovery year</option>
        </select>

        <div class="year-block">
          <label>Year</label>
          <div id="yearSlider" class="dual"></div>
        </div>
      </div>

      <div style="justify-self:end; opacity:.8; font-size:11px;">pan/zoom: counter + histogram update instantly</div>
    </div>

    <!-- Map -->
    <div id="mapBox">
      <div id="map"></div>
      <div id="mapTip"></div>
      <div class="counter">
        <div class="label">Units in view (filtered)</div>
        <div class="value" id="counterValue">–</div>
        <div class="sub" id="counterSub">Total: –</div>
      </div>
    </div>

    <!-- Histogram -->
    <div id="panel">
      <div id="histTitle">Histogram</div>
      <div id="histBox">
        <svg id="histSvg" width="100%" height="100%"></svg>
        <div id="tooltip"></div>
      </div>
      <div class="legend"><span id="histMin">–</span><span id="histMax">–</span></div>
    </div>
  </div>

<script type="module">
const DATA_URL   = {{ data_url|tojson }};
const MAPBOX_TOKEN = {{ mapbox_token|tojson }};
let   METRIC     = {{ hist_field|tojson }};
let   numBins    = {{ num_bins }};

let all = [], inView = [], yearValues = [];
let yearIdx = { minI:0, maxI:0 };
let selectedStatuses = new Set();
let yearSlider = null;

const statusEl = document.getElementById('status');
const counterValueEl = document.getElementById('counterValue');
const counterSubEl = document.getElementById('counterSub');
const histTitleEl = document.getElementById('histTitle');

const dd = document.getElementById('statusDD');
const statusBtn = document.getElementById('statusBtn');
const statusList = document.getElementById('statusList');
const statusSearch = document.getElementById('statusSearch');
const ddPanel = dd.querySelector('.dd-panel');

const mapTip = document.getElementById('mapTip');

mapboxgl.accessToken = MAPBOX_TOKEN;
const map = new mapboxgl.Map({
  container:'map',
  style:'mapbox://styles/mapbox/dark-v10',
  center:[{{ center_lng }}, {{ center_lat }}],
  zoom: {{ zoom }},
  dragRotate:false, pitchWithRotate:false
});

map.on('load', async ()=>{
  map.addSource('pts', { type:'geojson', data: emptyFC(), cluster:false });
  map.addLayer({
    id:'pts',
    type:'circle',
    source:'pts',
    paint:{
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 0, 2.0, 6, 3.5, 10, 5.0, 14, 6.5 ],
      'circle-color': [
        'match', ['downcase', ['get','Fuel type']],
        'gas', '#1B998B',
        'oil', '#F7931E',
        'oil and gas', '#E8FF59',
        /* other */ '#8A8D93'
      ],
      'circle-stroke-color':'#000',
      'circle-stroke-width':0.5,
      'circle-opacity':0.9
    }
  });

  // Map tooltip
  map.on('mouseenter', 'pts', () => map.getCanvas().style.cursor = 'pointer');
  map.on('mouseleave', 'pts', () => {
    map.getCanvas().style.cursor = '';
    mapTip.style.display = 'none';
  });
  map.on('mousemove', 'pts', (e) => {
    const f = e.features && e.features[0];
    if (!f) { mapTip.style.display = 'none'; return; }
    const p = f.properties || {};
    const unit = p["Unit Name"] || p.unit_name || p.name || '—';
    const uid  = p["Unit ID"] || p.unit_id || '—';
    const fuel = p["Fuel type"] || p.fuel_type || '—';
    const stat = p["Status"] || p.status || '—';
    const country = p["Country"] || p.country || '—';
    const sy = p.status_year ?? p["status_year"] ?? p["Status year"] ?? '—';
    const dy = p.discovery_year ?? p["discovery_year"] ?? p["Discovery year"] ?? '—';
    mapTip.innerHTML = `
      <div class="h">${unit}</div>
      <div class="row"><div class="k">Unit ID</div><div class="v">${uid}</div></div>
      <div class="row"><div class="k">Status</div><div class="v">${stat}</div></div>
      <div class="row"><div class="k">Fuel type</div><div class="v">${fuel}</div></div>
      <div class="row"><div class="k">Country</div><div class="v">${country}</div></div>
      <div class="row"><div class="k">Status year</div><div class="v">${sy}</div></div>
      <div class="row"><div class="k">Discovery year</div><div class="v">${dy}</div></div>
    `;
    const pad = 12;
    mapTip.style.left = (e.point.x + pad) + 'px';
    mapTip.style.top  = (e.point.y + pad) + 'px';
    mapTip.style.display = 'block';
  });

  // ---- DROPDOWN BEHAVIOR: persistent until outside click ----
  statusBtn.addEventListener('click', (e)=>{
    e.stopPropagation();
    dd.classList.toggle('open');
    statusBtn.setAttribute('aria-expanded', dd.classList.contains('open') ? 'true' : 'false');
    if (dd.classList.contains('open')) statusSearch.focus();
  });

  // Keep panel open while interacting inside it (checkboxes, search, scroll)
  ddPanel.addEventListener('mousedown', (e)=> e.stopPropagation());
  ddPanel.addEventListener('click', (e)=> e.stopPropagation());
  ddPanel.addEventListener('touchstart', (e)=> e.stopPropagation(), {passive:true});

  // Close only when clicking OUTSIDE
  window.addEventListener('click', ()=>{
    if (dd.classList.contains('open')) {
      dd.classList.remove('open');
      statusBtn.setAttribute('aria-expanded', 'false');
    }
  });

  // Close with Esc
  window.addEventListener('keydown', (e)=>{
    if (e.key === 'Escape' && dd.classList.contains('open')) {
      dd.classList.remove('open');
      statusBtn.setAttribute('aria-expanded', 'false');
      statusBtn.focus();
    }
  });
  // ---- end dropdown persistence ----

  // Search filter within dropdown
  statusSearch.addEventListener('input', ()=>{
    const q = statusSearch.value.toLowerCase();
    for (const row of statusList.querySelectorAll('label')) {
      const t = row.dataset.text || '';
      row.style.display = t.includes(q) ? '' : 'none';
    }
  });

  await loadData();
  setSource(all);
  initFilterUI();     // populate dropdown + build year slider
  applyMapFilter();
  updateFromMapBounds();

  map.on('move', updateFromMapBounds);
  map.on('moveend', updateFromMapBounds);
});

// ------------------------ Data Loading ------------------------
async function loadData(){
  statusEl.textContent = 'fetching…';
  const resp = await fetch(DATA_URL);
  if (!resp.ok) { statusEl.textContent = `HTTP ${resp.status}`; throw new Error('fetch failed'); }
  const ct = (resp.headers.get('content-type')||'').toLowerCase();

  if (ct.includes('application/json')) {
    const raw = await resp.json();
    all = processJSON(raw);
  } else {
    const bytes = new Uint8Array(await resp.arrayBuffer());
    all = await processParquet(bytes);
  }
  statusEl.textContent = `loaded ${all.length.toLocaleString()} units`;
  counterSubEl.textContent = `Total: ${all.length.toLocaleString()}`;
}

function processJSON(raw){
  if (raw?.features && Array.isArray(raw.features)){
    return raw.features.map(f=>{
      const c = f.geometry?.coordinates || [0,0];
      const p = f.properties || {};
      return {
        lng:+c[0], lat:+c[1], props:p,
        status_year: toInt(p["Status year"]),
        discovery_year: toInt(p["Discovery year"]),
        status_txt: (p["Status"]||'').toString().toLowerCase()
      };
    }).filter(ok);
  } else if (Array.isArray(raw)){
    return raw.map(r=>({
      lng:+(r.lng ?? r.longitude ?? r.Longitude ?? 0),
      lat:+(r.lat ?? r.latitude ?? r.Latitude ?? 0),
      props:r,
      status_year: toInt(r.status_year ?? r["Status year"]),
      discovery_year: toInt(r.discovery_year ?? r["Discovery year"]),
      status_txt: (r.Status || r.status || '').toString().toLowerCase()
    })).filter(ok);
  }
  return [];
}

async function processParquet(bytes){
  const m = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm');
  const b = await m.selectBundle(m.getJsDelivrBundles());
  const worker = new Worker(URL.createObjectURL(new Blob([await (await fetch(b.mainWorker)).text()], {type:'application/javascript'})));
  const db = new m.AsyncDuckDB(new m.ConsoleLogger(), worker);
  await db.instantiate(b.mainModule);
  const conn = await db.connect();

  await db.registerFileBuffer('og.parquet', bytes);
  await conn.query('DROP TABLE IF EXISTS og_raw;');
  await conn.query(`
    CREATE TABLE og_raw AS
    SELECT
      TRY_CAST("Latitude"  AS DOUBLE)  AS lat,
      TRY_CAST("Longitude" AS DOUBLE)  AS lng,
      CAST("Status"        AS VARCHAR) AS "Status",
      CAST("Fuel type"     AS VARCHAR) AS "Fuel type",
      CAST("Country"       AS VARCHAR) AS "Country",
      TRY_CAST("Status year"    AS INTEGER)   AS status_year,
      TRY_CAST("Discovery year" AS INTEGER)   AS discovery_year,
      COALESCE("Unit ID",'')   AS "Unit ID",
      COALESCE("Unit Name",'') AS "Unit Name"
    FROM read_parquet('og.parquet')
  `);
  const q = await conn.query(`
    SELECT lat, lng, "Status", "Fuel type", "Country", "Unit ID", "Unit Name", status_year, discovery_year
    FROM og_raw
    WHERE lat IS NOT NULL AND lng IS NOT NULL
  `);
  const rows = q.toArray();
  return rows.map(r=>({
    lng:+r.lng, lat:+r.lat,
    props:{
      "Status": r["Status"], "Fuel type": r["Fuel type"], "Country": r["Country"],
      "Unit ID": r["Unit ID"], "Unit Name": r["Unit Name"],
      status_year: r.status_year, discovery_year: r.discovery_year
    },
    status_year: toInt(r.status_year),
    discovery_year: toInt(r.discovery_year),
    status_txt: (r["Status"]||'').toString().toLowerCase()
  })).filter(ok);
}

function ok(d){ return Number.isFinite(d.lng) && Number.isFinite(d.lat); }
function toInt(v){ const n = +v; return Number.isFinite(n) ? Math.trunc(n) : null; }

// ------------------------ UI init & filter state ------------------------
function initFilterUI(){
  // Status dropdown build
  const uniqStatuses = Array.from(new Set(all.map(d => (d.props?.["Status"]||'').toString()).filter(Boolean))).sort();
  statusList.innerHTML = uniqStatuses.map(s => {
    const v = s.toLowerCase();
    return `<label data-text="${s.toLowerCase()}"><input type="checkbox" value="${v}"/><span>${s}</span></label>`;
  }).join('');
  selectedStatuses.clear();
  updateStatusButton();

  statusList.addEventListener('change', ()=>{
    selectedStatuses = new Set(Array.from(statusList.querySelectorAll('input:checked')).map(i=>i.value));
    updateStatusButton();
    applyMapFilter(); updateFromMapBounds();
  });

  // Build / rebuild year slider domain based on current metric (exclude 0)
  rebuildYearIndex();

  // Metric selector
  document.getElementById('metricSel').addEventListener('change', (e)=>{
    METRIC = e.target.value;
    rebuildYearIndex(); applyMapFilter(); updateFromMapBounds();
  });
}

function updateStatusButton(){
  const n = selectedStatuses.size;
  statusBtn.textContent = n ? `${n} selected` : 'All';
}

function nearestIndex(v) {
  if (!yearValues.length) return 0;
  let best = 0, bestDiff = Infinity;
  for (let i = 0; i < yearValues.length; i++) {
    const diff = Math.abs(yearValues[i] - v);
    if (diff < bestDiff) { best = i; bestDiff = diff; }
  }
  return best;
}

function rebuildYearIndex(){
  const vals = all.map(d => METRIC==='status_year' ? d.status_year : d.discovery_year)
                  .filter(v => Number.isFinite(v) && v > 0);

  if (!vals.length){
    yearValues = [];
    if (yearSlider && yearSlider.noUiSlider){ yearSlider.noUiSlider.destroy(); yearSlider = null; }
    document.getElementById('histMin').textContent = '–';
    document.getElementById('histMax').textContent = '–';
    return;
  }

  yearValues = Array.from(new Set(vals)).sort((a,b)=>a-b);
  yearIdx.minI = 0;
  yearIdx.maxI = yearValues.length-1;

  document.getElementById('histMin').textContent = String(yearValues[0]);
  document.getElementById('histMax').textContent = String(yearValues[yearValues.length-1]);

  // (re)build slider
  yearSlider = document.getElementById('yearSlider');
  if (yearSlider.noUiSlider) yearSlider.noUiSlider.destroy();

  noUiSlider.create(yearSlider, {
    start: [yearValues[yearIdx.minI], yearValues[yearIdx.maxI]],
    connect: true,
    step: 1,
    tooltips: [
      { to: v => String(Math.round(v)) },
      { to: v => String(Math.round(v)) }
    ],
    range: { min: yearValues[0], max: yearValues[yearValues.length-1] },
    behaviour: 'drag'
  });

  // keep indices in sync while sliding
  yearSlider.noUiSlider.on('update', (values) => {
    const lo = Math.round(values[0]);
    const hi = Math.round(values[1]);
    yearIdx.minI = nearestIndex(lo);
    yearIdx.maxI = nearestIndex(hi);
  });

  // apply filters when change commits
  yearSlider.noUiSlider.on('change', () => {
    applyMapFilter(); updateFromMapBounds();
  });
}

// ------------------------ Map filtering & viewport stats ------------------------
function setSource(list){
  const fc = { type:'FeatureCollection',
    features: list.map((d,i)=>({
      type:'Feature', id:i,
      geometry:{ type:'Point', coordinates:[d.lng,d.lat] },
      properties: { ...(d.props||{}), status_year:d.status_year, discovery_year:d.discovery_year }
    }))
  };
  map.getSource('pts').setData(fc);
}

function applyMapFilter(){
  const yMin = yearValues.length ? yearValues[Math.max(0, Math.min(yearIdx.minI, yearIdx.maxI))] : -999999;
  const yMax = yearValues.length ? yearValues[Math.max(yearIdx.minI, yearIdx.maxI)] :  999999;

  const yearOk = ['coalesce', ['get', METRIC], -999999];
  const yearExpr = ['all', ['>=', yearOk, yMin], ['<=', yearOk, yMax]];

  const sel = Array.from(selectedStatuses);
  const statusExpr = sel.length ? ['match', ['downcase',['get','Status']], sel, true, false] : ['boolean', true];

  const finalExpr = ['all', yearExpr, statusExpr];
  map.setFilter('pts', finalExpr);
}

function passesFilters(d){
  if (selectedStatuses.size){
    if (!d.status_txt || !selectedStatuses.has(d.status_txt)) return false;
  }
  const val = METRIC==='status_year' ? d.status_year : d.discovery_year;
  if (!Number.isFinite(val) || val <= 0) return false;
  const yMin = yearValues.length ? yearValues[Math.min(yearIdx.minI, yearIdx.maxI)] : -Infinity;
  const yMax = yearValues.length ? yearValues[Math.max(yearIdx.minI, yearIdx.maxI)] : Infinity;
  return val >= yMin && val <= yMax;
}

function updateFromMapBounds(){
  const b = map.getBounds();
  const west=b.getWest(), south=b.getSouth(), east=b.getEast(), north=b.getNorth();
  inView = all.filter(d => d.lng>=west && d.lng<=east && d.lat>=south && d.lat<=north)
              .filter(passesFilters);
  counterValueEl.textContent = inView.length.toLocaleString();
  drawHistogram(inView);
}

// ------------------------ Histogram (D3) ------------------------
const svg = d3.select("#histSvg");
const tooltip = document.getElementById('tooltip');

function drawHistogram(rows){
  const values = rows.map(d => (METRIC==='status_year' ? d.status_year : d.discovery_year))
                     .filter(v => Number.isFinite(v) && v > 0);
  histTitleEl.textContent = (METRIC==='status_year' ? 'Status year' : 'Discovery year');

  const bbox = svg.node().getBoundingClientRect();
  const width = Math.max(300, bbox.width - 8);
  const height = Math.max(150, bbox.height - 6);
  svg.attr("viewBox", `0 0 ${width} ${height}`);
  svg.selectAll("*").remove();

  if (!values.length){
    const left = yearValues.length ? yearValues[Math.min(yearIdx.minI, yearIdx.maxI)] : '–';
    const right = yearValues.length ? yearValues[Math.max(yearIdx.minI, yearIdx.maxI)] : '–';
    document.getElementById('histMin').textContent = String(left);
    document.getElementById('histMax').textContent = String(right);
    return;
  }

  const min = d3.min(values), max = d3.max(values);
  document.getElementById('histMin').textContent = String(min);
  document.getElementById('histMax').textContent = String(max);

  const margin = {top:6,right:8,bottom:22,left:34};
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;

  const x = d3.scaleLinear().domain([min, max]).nice().range([0, innerW]);
  const bins = d3.bin().thresholds(numBins)(values);
  const y = d3.scaleLinear().domain([0, d3.max(bins, d=>d.length)||1]).nice().range([innerH, 0]);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  g.selectAll("rect").data(bins).enter().append("rect")
    .attr("x", d => x(d.x0) + 1)
    .attr("y", d => y(d.length))
    .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 2))
    .attr("height", d => innerH - y(d.length))
    .attr("fill", "#E8FF59")
    .attr("opacity", 0.9)
    .on("mousemove", (evt,d)=>{
      tooltip.style.display = 'block';
      tooltip.style.left = (evt.pageX + 12) + 'px';
      tooltip.style.top  = (evt.pageY - 10) + 'px';
      tooltip.innerHTML = `<b>${METRIC.replace('_',' ')}:</b> ${Math.round(d.x0)}–${Math.round(d.x1)}<br><b>count:</b> ${d.length.toLocaleString()}`;
    })
    .on("mouseout", ()=> tooltip.style.display = 'none')
    .on("click", (evt,d)=>{
      if (!yearValues.length) return;
      const lo = Math.round(d.x0), hi = Math.round(d.x1);
      const loClamped = Math.max(yearValues[0], Math.min(lo, yearValues.at(-1)));
      const hiClamped = Math.max(yearValues[0], Math.min(hi, yearValues.at(-1)));
      yearSlider.noUiSlider.set([loClamped, hiClamped]);
      applyMapFilter(); updateFromMapBounds();
    });

  g.append("g").attr("transform", `translate(0,${innerH})`)
    .call(d3.axisBottom(x).ticks(6).tickFormat(d3.format("d")))
    .selectAll("text").attr("fill","#bbb");
  g.append("g").call(d3.axisLeft(y).ticks(4))
    .selectAll("text").attr("fill","#bbb");
  g.selectAll(".domain,.tick line").attr("stroke","#444");
}

// ------------------------ Helpers ------------------------
function emptyFC(){ return { type:'FeatureCollection', features:[] }; }
</script>
</body>
</html>
    """).render(
        data_url=data_url,
        mapbox_token=mapbox_token,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        num_bins=num_bins,
        hist_field=hist_field
    )
    return common.html_to_obj(html)
