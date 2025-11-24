
@fused.udf
def udf(
  data_url: str = "https://unstable.udf.ai/fsh_5fxZ3aJLfZz7RTRCbZmaRQ.parquet?name=McDonald",
  theme: str = "workbench",
  auto_fetch: bool = True,
  num_bins: int = 50,
  value_field: str = "median_income",
  title: str = "Income Distribution",
  metric_channel: str = "hex-mcdonald",  # IMPORTANT: match hex_window.py's metric_channel
  sync_channel: str = "hist"
):
  common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
  _ = fused.load("join_era5_cdl_elevation")
  themes = {
      'workbench': {
          'background': '#1a1a1a',
          'text': '#ffffff',
          'accent': '#E8FF59',
          'chart_bg': '#2a2a2a'
      }
  }
  selected = themes.get(theme, themes['workbench'])

  safe_title = (
      title.replace("&", "&amp;")
           .replace("<", "&lt;")
           .replace(">", "&gt;")
           .replace('"', "&quot;")
           .replace("'", "&#39;")
  )

  html_content = """
  <!DOCTYPE html>
  <html>
  <head>
      <meta charset="utf-8" />
      <title>""" + safe_title + """</title>
      <script src="https://d3js.org/d3.v7.min.js"></script>
      <script src="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.30.0/dist/duckdb-wasm.js"></script>
      <script type="module">
          import * as duckdb_wasm from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.30.0/+esm";
          window.__DUCKDB_WASM = duckdb_wasm;
      </script>
      <style>
          * { margin: 0; padding: 0; border: none; outline: none; box-sizing: border-box; }
          body {
              padding: 15px;
              font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
              background: """ + selected['background'] + """;
              color: """ + selected['text'] + """;
              height: 100vh; display: flex; flex-direction: column;
          }
          .header { text-align: center; margin-bottom: 10px; flex-shrink: 0; }
          .title { font-size: 16px; color: """ + selected['accent'] + """; margin-bottom: 6px; font-weight: 600; }
          .chart-container { flex: 1; position: relative; background: """ + selected['chart_bg'] + """; border-radius: 6px; padding: 10px; margin-bottom: 10px; min-height: 200px; }
          .selection-info { text-align: center; font-size: 11px; color: """ + selected['accent'] + """; margin-bottom: 8px; min-height: 14px; flex-shrink: 0; }
          .controls { text-align: center; flex-shrink: 0; margin-bottom: 10px; display: flex; flex-direction: column; gap: 8px; }
          .controls-row { display: flex; justify-content: center; align-items: center; gap: 10px; }
          .btn { background: """ + selected['accent'] + """; color: """ + selected['background'] + """; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 11px; font-weight: 600; margin: 0 4px; transition: opacity 0.2s; }
          .btn:hover { opacity: 0.8; }
          .status { text-align: center; font-size: 10px; opacity: 0.7; flex-shrink: 0; }
          .loading-state { display: flex; flex-direction: column; justify-content: center; align-items: center; height: 100%; opacity: 0.6; }
          .bar { fill: """ + selected['accent'] + """; transition: fill 0.2s, opacity 0.2s; cursor: pointer; }
          .bar.selected { fill: """ + selected['accent'] + """; opacity: 1; }
          .bar.unselected { fill: rgba(232, 255, 89, 0.2); stroke: none; opacity: 1; }
          .bar:hover { opacity: 0.8; }
          .tooltip { position: absolute; background: rgba(0, 0, 0, 0.9); color: white; padding: 8px 12px; border-radius: 4px; font-size: 12px; pointer-events: none; z-index: 1000; display: none; border: 1px solid #444; }
      </style>
  </head>
  <body>
      <div class="tooltip" id="tooltip"></div>
      <div class="header">
          <div class="title">""" + safe_title + """</div>
      </div>
      <div class="chart-container" id="chartContainer">
          <div id="loadingState" class="loading-state">
              <div style="font-size: 24px;">📊</div>
              <div style="margin-top: 8px; font-weight: 500;">Loading full dataset</div>
          </div>
          <svg id="histogram" style="display: none; width: 100%; height: 100%;"></svg>
      </div>
      <div class="histogram-minmax" id="histogramMinMax" style="display: flex; justify-content: space-between; font-size: 11px; color: """ + selected['accent'] + """; margin-bottom: 8px; min-height: 14px;">
          <span id="histMin"></span><span id="histMax"></span>
      </div>
      <div class="selection-info" id="selectionInfo"></div>
      <div class="controls">
          <div class="controls-row">
              <button class="btn" onclick="clearSelection()">Clear Selection</button>
              <button class="btn" onclick="reloadData()">Reload Data</button>
          </div>
      </div>
      <div class="status" id="status">Loading data from URL...</div>

      <script>
          // --- config ---
          const componentId = 'histogram-v3-' + Math.random().toString(36).substr(2, 9);
          const DATA_URL  = """ + repr(data_url) + """;
          const AUTO_FETCH = """ + str(auto_fetch).lower() + """;
          const NUM_BINS  = """ + str(num_bins) + """;
          const VALUE_FIELD = """ + repr(value_field) + """;
          const METRIC_CHANNEL = """ + repr(metric_channel) + """;
          const SYNC_CHANNEL = """ + repr(sync_channel) + """;
          const METRIC_EVENT_TYPE = "hex-window-metric";
          const DEFAULT_SQL = "SELECT * FROM data";

          // --- broadcast channels ---
          let metricBroadcast = null;
          try {
              if ('BroadcastChannel' in window && METRIC_CHANNEL) {
                  metricBroadcast = new BroadcastChannel(`hex-window-metric::${METRIC_CHANNEL}`);
              }
          } catch (e) {
              console.warn('BroadcastChannel failed', e);
          }

          let histogramSyncBroadcast = null;
          try {
              if ('BroadcastChannel' in window && SYNC_CHANNEL) {
                  histogramSyncBroadcast = new BroadcastChannel(`histogram-sync::${SYNC_CHANNEL}`);
              }
          } catch (e) {
              console.warn('Histogram BroadcastChannel failed', e);
          }

          function busSend(obj) {
              const s = JSON.stringify(obj);
              try { if (metricBroadcast) metricBroadcast.postMessage(obj); } catch(e) {}
              try { window.parent.postMessage(s, '*'); } catch(e) {}
              try { if (window.top && window.top !== window.parent) window.top.postMessage(s, '*'); } catch(e) {}
              try {
                  if (window.top && window.top.frames) {
                      for (let i=0; i<window.top.frames.length; i++) {
                          const f = window.top.frames[i];
                          if (f !== window) try { f.postMessage(s, '*'); } catch(e) {}
                      }
                  }
              } catch(e) {}
          }

          function fanoutBrushPayload(payload) {
              const serialized = JSON.stringify(payload);
              try { if (histogramSyncBroadcast) histogramSyncBroadcast.postMessage(payload); } catch (e) {}
              try { window.parent.postMessage(serialized, '*'); } catch (e) {}
              try {
                  if (window.top && window.top !== window.parent) {
                      window.top.postMessage(serialized, '*');
                  }
              } catch (e) {}
              try {
                  if (window.top && window.top.frames) {
                      for (let i = 0; i < window.top.frames.length; i++) {
                          const f = window.top.frames[i];
                          if (f !== window) {
                              try { f.postMessage(serialized, '*'); } catch (_) {}
                          }
                      }
                  }
              } catch (e) {}
          }

          function sanitizeSQL(sql) {
              if (sql == null) return "";
              return String(sql).trim().replace(/;+\s*$/g, '');
          }

          function getFeatureValue(feature, fieldName = VALUE_FIELD) {
              if (!feature) return 0;
              if (feature[fieldName] !== undefined) {
                  const direct = Number(feature[fieldName]);
                  if (Number.isFinite(direct)) return direct;
              }
              if (feature.properties && feature.properties[fieldName] !== undefined) {
                  const propVal = Number(feature.properties[fieldName]);
                  if (Number.isFinite(propVal)) return propVal;
              }
              return 0;
          }

          // --- state ---
          let duckConn = null;
          let duckDBReady = false;
          let currentSQL = DEFAULT_SQL;
          let remoteBaseSQL = DEFAULT_SQL;
          let pendingDuckQuery = null;
          let currentRangeClause = null;
          let hasExecutedDuckQuery = false;
          let fullDataset = [];
          let filteredDataset = [];
          let activeFilters = new Map();
          let dynamicBins = [];
          let svg = null, chartGroup = null, barsGroup = null, xScale = null, yScale = null, brush = null;
          let selectedBins = [];
          let chartWidth = 0, chartHeight = 0;
          const RANGE_DEBOUNCE_MS = 0;
          let rangeDebounceTimer = null;
          let pendingRangeSelection = null;
          let currentMinValue = null;
          let currentMaxValue = null;
          let suppressBrushBroadcast = false;
          let pendingRemoteBrush = null;
          let pendingRemoteClear = false;

          // UI handles
          const loadingState = document.getElementById('loadingState');
          const histogramSvg = document.getElementById('histogram');
          const statusElement = document.getElementById('status');

          // --- utils ---
          function formatValue(val) { return (val >= 1000) ? `${(val/1000).toFixed(1)}k  ` : `${val.toFixed(1)}  `; }

          function broadcastBrushSelection(min, max) {
              if (!SYNC_CHANNEL) return;
              const payload = {
                  type: 'histogram-brush',
                  channel: SYNC_CHANNEL,
                  field: VALUE_FIELD,
                  min,
                  max,
                  fromComponent: componentId,
                  timestamp: Date.now()
              };
              fanoutBrushPayload(payload);
          }

          function broadcastBrushClear() {
              if (!SYNC_CHANNEL) return;
              const payload = {
                  type: 'histogram-brush-clear',
                  channel: SYNC_CHANNEL,
                  fromComponent: componentId,
                  timestamp: Date.now()
              };
              fanoutBrushPayload(payload);
          }

          function queueRemoteBrush(min, max) {
              pendingRemoteBrush = { min, max };
              pendingRemoteClear = false;
              applyPendingRemoteBrush();
          }

          function queueRemoteBrushClear() {
              pendingRemoteBrush = null;
              pendingRemoteClear = true;
              applyPendingRemoteBrush();
          }

          function applyPendingRemoteBrush() {
              if ((!pendingRemoteBrush && !pendingRemoteClear) || !brush || !xScale) return;
              if (pendingRemoteClear) {
                  pendingRemoteClear = false;
                  suppressBrushBroadcast = true;
                  if (chartGroup && brush) {
                      chartGroup.select('.brush').call(brush.move, null);
                  } else {
                      clearRangeFilter();
                  }
                  return;
              }
              const { min, max } = pendingRemoteBrush;
              const indices = [];
              dynamicBins.forEach((bin, idx) => {
                  if (bin && Number.isFinite(bin.min) && Number.isFinite(bin.max)) {
                      if (max > bin.min && min < bin.max) {
                          indices.push(idx);
                      }
                  }
              });
              pendingRemoteBrush = null;
              if (!indices.length) {
                  suppressBrushBroadcast = true;
                  if (chartGroup && brush) {
                      chartGroup.select('.brush').call(brush.move, null);
                  }
                  return;
              }
              selectedBins = indices;
              updateBarSelection();
              updateSelectionInfo();
              const start = indices[0];
              const end = indices[indices.length - 1];
              const x0 = xScale(start);
              const x1 = xScale(end) + xScale.bandwidth();
              if (Number.isFinite(x0) && Number.isFinite(x1) && chartGroup && brush) {
                  suppressBrushBroadcast = true;
                  chartGroup.select('.brush').call(brush.move, [x0, x1]);
              }
          }

          function handleHistogramSyncPayload(message) {
              try {
                  if (typeof message === 'string') {
                      message = JSON.parse(message);
                  }
              } catch (_) {
                  return;
              }
              if (!message) return;
              if (message.fromComponent && message.fromComponent === componentId) return;
              if (SYNC_CHANNEL && message.channel && message.channel !== SYNC_CHANNEL) return;
              if (message.type === 'histogram-brush') {
                  if (Number.isFinite(message.min) && Number.isFinite(message.max)) {
                      queueRemoteBrush(message.min, message.max);
                  }
              } else if (message.type === 'histogram-brush-clear') {
                  queueRemoteBrushClear();
              }
          }

          // --- bins ---
          function calculateDynamicBins(data) {
              if (!data || data.length === 0) return [];
              const areas = data.map(f => getFeatureValue(f)).filter(a => a > 0).sort((a,b)=>a-b);
              if (!areas.length) return [];
              const min = areas[0], max = areas[areas.length - 1];
              const logMin = Math.log10(min), logMax = Math.log10(max), logStep = (logMax - logMin) / NUM_BINS;
              const bins = [];
              for (let i=0;i<NUM_BINS;i++){
                  const binMin = Math.pow(10, logMin + i*logStep);
                  const binMax = Math.pow(10, logMin + (i+1)*logStep);
                  const label = binMax >= 1000 ? `${(binMin/1000).toFixed(1)}-${(binMax/1000).toFixed(1)}  `
                                               : `${binMin.toFixed(1)}-${binMax.toFixed(1)}  `;
                  bins.push({ label, min:binMin, max:binMax, count:0 });
              }
              return bins;
          }

          // --- duckdb init + querying ---
          async function initDuckDB() {
              if (duckDBReady && duckConn) return;
              setUIState('loading');
              statusElement.textContent = 'Initializing DuckDB…';
              try {
                  const duckmod = window.__DUCKDB_WASM;
                  if (!duckmod) throw new Error('DuckDB WASM runtime not available');

                  const bundle = await duckmod.selectBundle(duckmod.getJsDelivrBundles());
                  const worker = new Worker(
                      URL.createObjectURL(
                          new Blob([await (await fetch(bundle.mainWorker)).text()], { type: 'application/javascript' })
                      )
                  );
                  const db = new duckmod.AsyncDuckDB(new duckmod.ConsoleLogger(), worker);
                  await db.instantiate(bundle.mainModule);
                  duckConn = await db.connect();

                  statusElement.textContent = 'Installing H3 extension…';
                  try {
                      await duckConn.query("INSTALL h3 FROM community");
                      await duckConn.query("LOAD h3");
                      statusElement.textContent = 'H3 extension loaded';
                  } catch (h3Err) {
                      console.error("H3 extension error:", h3Err?.message || h3Err);
                      statusElement.textContent = 'ERROR: H3 extension failed - ' + (h3Err?.message || h3Err);
                      throw h3Err;
                  }

                  statusElement.textContent = 'Downloading dataset…';
                  const res = await fetch(DATA_URL);
                  if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
                  const bytes = new Uint8Array(await res.arrayBuffer());
                  await db.registerFileBuffer('histogram.parquet', bytes);
                  
                  statusElement.textContent = 'Creating view with lat/lng…';
                  await duckConn.query(`
                      CREATE OR REPLACE VIEW data AS 
                      SELECT 
                          *,
                          h3_cell_to_lat(CAST(hex AS UBIGINT)) AS lat,
                          h3_cell_to_lng(CAST(hex AS UBIGINT)) AS lng
                      FROM read_parquet('histogram.parquet')
                  `);

                  duckDBReady = true;
                  statusElement.textContent = 'DuckDB ready';
              } catch (e) {
                  duckDBReady = false;
                  console.error(e);
                  setUIState('loading');
                  statusElement.textContent = `DuckDB init failed: ${e.message}`;
                  throw e;
              }
          }

          async function runHistogramQuery(options = {}) {
              if (!duckConn) {
                  throw new Error('DuckDB connection not ready');
              }

              const targetSql = sanitizeSQL(options.sql ?? currentSQL ?? DEFAULT_SQL) || sanitizeSQL(DEFAULT_SQL);
              const baseSql = sanitizeSQL(options.baseSQL ?? remoteBaseSQL ?? targetSql) || targetSql;
              const showLoading = options.showLoading !== false;

              if (options.updateBase !== false) {
                  remoteBaseSQL = baseSql;
              }
              currentSQL = targetSql;

              try {
                  if (showLoading) {
                      setUIState('loading');
                      statusElement.textContent = 'Running DuckDB query…';
                  } else {
                      statusElement.textContent = 'Updating selection…';
                  }
                  const res = await duckConn.query(targetSql);
                  const rows = res.toArray();

                  fullDataset = processRawData(rows);
                  dynamicBins = calculateDynamicBins(fullDataset);
                  applyAllFilters(true);
                  hasExecutedDuckQuery = true;
              } catch (e) {
                  console.error(e);
                  if (showLoading) setUIState('loading');
                  statusElement.textContent = `SQL error: ${e.message}`;
              }
          }

          function processRawData(raw) {
              if (raw?.features && Array.isArray(raw.features)) {
                  return raw.features.map(f => {
                      const areaMeters = f.properties?.area_meters || 0;
                      const areaKm = areaMeters / 1_000_000;
                      let valueCandidate;
                      if (f.properties && Object.prototype.hasOwnProperty.call(f.properties, VALUE_FIELD)) {
                          valueCandidate = f.properties[VALUE_FIELD];
                      } else if (VALUE_FIELD === 'count_mmsi') {
                          valueCandidate = areaKm;
                      } else {
                          valueCandidate = f.properties?.value ?? f.properties?.cnt ?? areaKm;
                      }
                      const numericValue = Number(valueCandidate);
                      const resolvedValue = Number.isFinite(numericValue) ? numericValue : 0;
                      const props = Object.assign({}, f.properties, { count_mmsi: areaKm, [VALUE_FIELD]: resolvedValue });
                      return {
                          id: f.properties?.id || Math.random().toString(36),
                          geometry: f.geometry,
                          lng: f.geometry?.coordinates?.[0],
                          lat: f.geometry?.coordinates?.[1],
                          properties: props,
                          count_mmsi: areaKm,
                          area_meters: areaMeters,
                          cnt: f.properties?.cnt || 0,
                          [VALUE_FIELD]: resolvedValue
                      };
                  });
              } else if (Array.isArray(raw)) {
                  return raw.map(item => {
                      const valueCandidate = item[VALUE_FIELD] ?? item.value ?? item.count ?? item.count_mmsi ?? 0;
                      const numericValue = Number(valueCandidate);
                      const resolvedValue = Number.isFinite(numericValue) ? numericValue : 0;
                      const copy = Object.assign({}, item);
                      copy[VALUE_FIELD] = resolvedValue;
                      return copy;
                  });
              }
              return [];
          }

          function reloadData() {
              clearAllFilters();
              if (duckDBReady && duckConn) {
                  runHistogramQuery({ sql: remoteBaseSQL, baseSQL: remoteBaseSQL, updateBase: true });
              }
          }

          // --- external filters application (so histogram reflects spatial from map) ---
          function applyAllFilters(fromQuery = false) {
              let filtered = [...fullDataset];
              for (let [,f] of activeFilters.entries()) filtered = applyFilter(filtered, f);
              filteredDataset = filtered;
              updateHistogram(false);
              const summary = `${filtered.length}/${fullDataset.length} features (${activeFilters.size} filters)`;
              statusElement.textContent = summary;
          }

          function applyFilter(data, filter) {
              switch (filter.type) {
                  case 'spatial': return applySpatialFilter(data, filter);
                  case 'range':
                  case 'multi-range': return applyRangeFilter(data, filter);
                  case 'categorical': return applyCategoricalFilter(data, filter);
                  default: return data;
              }
          }

          function applySpatialFilter(data, filter) {
              const b = filter.bounds; if (!b) return data;
              return data.filter(f => f.lng>=b.sw.lng && f.lng<=b.ne.lng && f.lat>=b.sw.lat && f.lat<=b.ne.lat);
          }

          function applyRangeFilter(data, filter) {
              const field = filter.field || VALUE_FIELD;
              const [min,max] = filter.values || [0, Number.POSITIVE_INFINITY];
              return data.filter(f => {
                  const val = getFeatureValue(f, field);
                  return val >= min && val <= max;
              });
          }

          function applyCategoricalFilter(data, filter) {
              const field = filter.field || 'category';
              const values = filter.values || [];
              return data.filter(f => values.includes(f[field]));
          }

          function clearAllFilters() {
              activeFilters.clear();
              clearRangeFilter();
              filteredDataset = [...fullDataset];
              updateHistogram();
              statusElement.textContent = `${fullDataset.length} features (no filters)`;
              
              console.log('🧹 Broadcasting clear filter');
              
              // Send clear filter message
              busSend({ 
                  type: 'histogram-clear-filter', 
                  fromComponent: componentId, 
                  channel: METRIC_CHANNEL,
                  timestamp: Date.now() 
              });
          }

          // --- UI state ---
          function setUIState(state) {
              loadingState.style.display = state === 'loading' ? 'flex' : 'none';
              histogramSvg.style.display = state === 'chart' ? 'block' : 'none';
          }

          // --- chart ---
          function initializeChart() {
              d3.select('#histogram').selectAll('*').remove();
              const container = document.getElementById('chartContainer');
              chartWidth = container.clientWidth - 20;
              chartHeight = container.clientHeight - 20;

              svg = d3.select('#histogram').attr('width', chartWidth).attr('height', chartHeight);
              chartGroup = svg.append('g').attr('class', 'chart-group');
              barsGroup = svg.append('g').attr('class', 'bars-group');

              xScale = d3.scaleBand().domain(dynamicBins.map((_,i)=>i)).range([0, chartWidth]).padding(0.1);
              yScale = d3.scaleLinear().domain([0, d3.max(dynamicBins, d => d.count)]).range([chartHeight, 0]).nice();

              setupBrush();
              createBars();
              applyPendingRemoteBrush();
          }

          function createBars() {
              barsGroup.selectAll('.bar')
                  .data(dynamicBins).enter().append('rect').attr('class','bar')
                  .attr('x', (d,i)=>xScale(i))
                  .attr('y', d=>yScale(d.count))
                  .attr('width', xScale.bandwidth())
                  .attr('height', d=>chartHeight - yScale(d.count))
                  .on('mouseover', function(evt,d){
                      d3.select(this).style('opacity',0.8);
                      const t = document.getElementById('tooltip');
                      t.innerHTML = `<div><strong>Range:</strong> ${formatValue(d.min)} - ${formatValue(d.max)}</div><div><strong>Count:</strong> ${d.count} areas</div>`;
                      t.style.display = 'block'; t.style.left = (evt.pageX+15)+'px'; t.style.top = (evt.pageY-10)+'px';
                  })
                  .on('mousemove', function(evt){ const t=document.getElementById('tooltip'); t.style.left=(evt.pageX+15)+'px'; t.style.top=(evt.pageY-10)+'px'; })
                  .on('mouseout', function(){ d3.select(this).style('opacity',1); document.getElementById('tooltip').style.display='none'; });
          }

          function setupBrush() {
              brush = d3.brushX().extent([[0,0],[chartWidth,chartHeight]]).on('start brush end', brushed);
              const brushGroup = chartGroup.append('g').attr('class','brush').call(brush);
              brushGroup.select('.selection').style('fill','rgba(128,128,128,0.25)').style('stroke','#999').style('stroke-width','0.5px').style('rx','3px');
              brushGroup.select('.overlay').style('cursor','crosshair').style('fill','none');
          }

          function brushed(event) {
              const sel = event.selection;
              if (!sel) {
                  selectedBins = [];
                  updateBarSelection(); updateSelectionInfo();
                  pendingRangeSelection = null;
                  if (rangeDebounceTimer) {
                      clearTimeout(rangeDebounceTimer);
                      rangeDebounceTimer = null;
                  }
                  if (event.type === 'end') clearRangeFilter();
                  return;
              }
              const [x0,x1] = sel;
              selectedBins = [];
              dynamicBins.forEach((bin, idx) => {
                  const left = xScale(idx), right = left + xScale.bandwidth();
                  if (right > x0 && left < x1) selectedBins.push(idx);
              });
              updateBarSelection();
              updateSelectionInfo();
              if ((event.type === 'brush' || event.type === 'end') && selectedBins.length > 0) {
                  const minArea = Math.min(...selectedBins.map(i => dynamicBins[i]?.min ?? Infinity));
                  const maxArea = Math.max(...selectedBins.map(i => dynamicBins[i]?.max ?? -Infinity));
                  if (Number.isFinite(minArea) && Number.isFinite(maxArea)) {
                      scheduleRangeFilter(minArea, maxArea);
                  }
              }
          }

          function updateBarSelection() {
              if (!barsGroup) return;
              barsGroup.selectAll('.bar')
                  .attr('class', (d,i) => selectedBins.length===0 ? 'bar' : (selectedBins.includes(i) ? 'bar selected' : 'bar unselected'));
          }

          function updateHistogram(animate=false) {
              const data = filteredDataset;
              if (!data.length) {
                  currentMinValue = null;
                  currentMaxValue = null;
                  updateMinMaxLabels();
                  setUIState('loading');
                  statusElement.textContent = 'No data matches current filters';
                  return;
              }

              let minVal = Infinity;
              let maxVal = -Infinity;
              dynamicBins.forEach(b => b.count = 0);
              data.forEach(f => {
                  const value = getFeatureValue(f);
                  if (Number.isFinite(value)) {
                      if (value < minVal) minVal = value;
                      if (value > maxVal) maxVal = value;
                  }
                  for (let b of dynamicBins) {
                      if (value >= b.min && value < b.max) { b.count++; break; }
                  }
              });

              currentMinValue = Number.isFinite(minVal) ? minVal : null;
              currentMaxValue = Number.isFinite(maxVal) ? maxVal : null;
              updateMinMaxLabels();

              if (!svg) {
                  initializeChart();
                  setUIState('chart');
                  return;
              }

              yScale.domain([0, d3.max(dynamicBins, d => d.count)]).nice();
              const bars = barsGroup.selectAll('.bar').data(dynamicBins);
              if (animate) {
                  bars.transition().duration(200).attr('y', d=>yScale(d.count)).attr('height', d=>chartHeight - yScale(d.count));
              } else {
                  bars.attr('y', d=>yScale(d.count)).attr('height', d=>chartHeight - yScale(d.count));
              }
              updateBarSelection();
              setUIState('chart');
              applyPendingRemoteBrush();
          }

          function updateSelectionInfo() {
              const el = document.getElementById('selectionInfo');
              if (!selectedBins.length) { el.textContent=''; return; }
              const total = selectedBins.reduce((s,i)=>s+dynamicBins[i].count,0);
              const minArea = Math.min(...selectedBins.map(i=>dynamicBins[i].min));
              const maxArea = Math.max(...selectedBins.map(i=>dynamicBins[i].max));
              const rangeLabel = maxArea >= 1000 ? `${(minArea/1000).toFixed(1)}-${(maxArea/1000).toFixed(1)}  ` : `${minArea.toFixed(1)}-${maxArea.toFixed(1)}  `;
              el.textContent = selectedBins.length===1
                ? `Selected: ${dynamicBins[selectedBins[0]].label} (${total} areas)`
                : `Selected: ${selectedBins.length} ranges (${rangeLabel}, ${total} areas)`;
          }

          // --- selection actions -> send filters to others ---
          function scheduleRangeFilter(minArea, maxArea) {
              pendingRangeSelection = { min: minArea, max: maxArea };
              if (rangeDebounceTimer) clearTimeout(rangeDebounceTimer);
              rangeDebounceTimer = setTimeout(() => {
                  rangeDebounceTimer = null;
                  const payload = pendingRangeSelection;
                  pendingRangeSelection = null;
                  if (!payload) return;
                  sendRangeFilter(payload.min, payload.max);
              }, RANGE_DEBOUNCE_MS);
          }

          function sendRangeFilter(minArea, maxArea) {
              if (!Number.isFinite(minArea) || !Number.isFinite(maxArea)) return;
              const lower = Math.min(minArea, maxArea);
              const upper = Math.max(minArea, maxArea);
              const base = sanitizeSQL(remoteBaseSQL || DEFAULT_SQL) || DEFAULT_SQL;

              currentRangeClause = `${VALUE_FIELD} BETWEEN ${lower} AND ${upper}`;
              currentSQL = base;

              statusElement.textContent = `Selected ${formatValue(lower)} – ${formatValue(upper)}`;

              console.log(`📤 Broadcasting range filter: ${VALUE_FIELD} [${lower}, ${upper}]`);

              // Always notify map consumers
              busSend({
                  type: 'histogram-range-filter',
                  field: VALUE_FIELD,
                  min: lower,
                  max: upper,
                  fromComponent: componentId,
                  channel: METRIC_CHANNEL,
                  timestamp: Date.now()
              });

              if (suppressBrushBroadcast) {
                  suppressBrushBroadcast = false;
              } else {
                  broadcastBrushSelection(lower, upper);
              }
          }

          function clearSelection() {
              selectedBins = [];
              if (chartGroup && brush) chartGroup.select('.brush').call(brush.clear);
              updateBarSelection(); updateSelectionInfo(); clearRangeFilter();
          }

          function clearRangeFilter() {
              if (rangeDebounceTimer) {
                  clearTimeout(rangeDebounceTimer);
                  rangeDebounceTimer = null;
              }
              pendingRangeSelection = null;
              const hadRange = !!currentRangeClause;
              currentRangeClause = null;
              if (hadRange) {
                  statusElement.textContent = 'Range cleared';
                  console.log('🧹 Broadcasting clear filter from clearRangeFilter');
                  busSend({ 
                      type: 'histogram-clear-filter', 
                      fromComponent: componentId, 
                      channel: METRIC_CHANNEL,
                      timestamp: Date.now() 
                  });
                  if (suppressBrushBroadcast) {
                      suppressBrushBroadcast = false;
                  } else {
                      broadcastBrushClear();
                  }
              }
          }

          // --- incoming messages (listen for viewport bounds from hex_window.py) ---
          function handleMetricMessage(message) {
              try {
                  if (typeof message === 'string') { 
                      try { message = JSON.parse(message); } catch (_) { return; }
                  }
                  if (!message) return;

                  // ignore our own messages
                  if (message.fromComponent && message.fromComponent === componentId) return;

                  // Listen for viewport metrics from hex_window.py
                  if (message.type === METRIC_EVENT_TYPE && message.channel === METRIC_CHANNEL) {
                      const bounds = message.bounds;
                      if (bounds && Number.isFinite(bounds.west) && Number.isFinite(bounds.south) && 
                          Number.isFinite(bounds.east) && Number.isFinite(bounds.north)) {
                          
                          console.log('📍 Viewport update:', {
                              west: bounds.west.toFixed(2),
                              south: bounds.south.toFixed(2),
                              east: bounds.east.toFixed(2),
                              north: bounds.north.toFixed(2)
                          });
                          
                          activeFilters.set('viewport_spatial', { 
                              type:'spatial', 
                              bounds:{ 
                                  sw:{lng:bounds.west, lat:bounds.south}, 
                                  ne:{lng:bounds.east, lat:bounds.north} 
                              } 
                          });
                          applyAllFilters();
                      }
                      return;
                  }
              } catch (e) { 
                  console.warn('handleMetricMessage error', e);
              }
          }

          if (metricBroadcast) {
              metricBroadcast.onmessage = (ev) => {
                  console.log('📡 BroadcastChannel message received');
                  handleMetricMessage(ev.data);
              };
          }

          if (histogramSyncBroadcast) {
              histogramSyncBroadcast.onmessage = (ev) => {
                  handleHistogramSyncPayload(ev.data);
              };
          }

          window.addEventListener('message', (ev) => {
              const data = ev.data;
              if (!data) return;
              if (typeof data === 'object') {
                  if (data.type === METRIC_EVENT_TYPE) {
                      console.log('📬 PostMessage received');
                      handleMetricMessage(data);
                  } else if (data.type === 'histogram-brush' || data.type === 'histogram-brush-clear') {
                      handleHistogramSyncPayload(data);
                  }
              } else if (typeof data === 'string') {
                  let parsed = null;
                  try { parsed = JSON.parse(data); } catch (_) {}
                  if (!parsed) return;
                  if (parsed.type === METRIC_EVENT_TYPE) {
                      handleMetricMessage(parsed);
                  } else if (parsed.type === 'histogram-brush' || parsed.type === 'histogram-brush-clear') {
                      handleHistogramSyncPayload(parsed);
                  }
              }
          });

          // --- init ---
          document.addEventListener('DOMContentLoaded', async function() {
              if (!AUTO_FETCH || !DATA_URL) {
                  setUIState('loading');
                  statusElement.textContent = 'Manual data loading required';
              }

              if (AUTO_FETCH && DATA_URL) {
                  try {
                      await initDuckDB();
                      if (pendingDuckQuery) {
                          const queued = pendingDuckQuery;
                          pendingDuckQuery = null;
                          await runHistogramQuery({ sql: queued.sql, baseSQL: queued.base, suppressBroadcast: true, updateBase: true });
                      } else if (!hasExecutedDuckQuery) {
                          await runHistogramQuery({ sql: remoteBaseSQL, baseSQL: remoteBaseSQL, suppressBroadcast: true, updateBase: true });
                      }
                  } catch (e) {
                      console.error(e);
                  }
              }

              setTimeout(() => {
                  busSend({
                      type: 'component_ready',
                      componentType: 'histogram',
                      componentId: componentId,
                      capabilities: ['filter'],
                      dataSource: 'independent',
                      protocol: 'unified'
                  });
              }, 300);
          });

          function updateMinMaxLabels() {
              const minEl = document.getElementById('histMin');
              const maxEl = document.getElementById('histMax');
              if (!minEl || !maxEl) return;
              if (!Number.isFinite(currentMinValue) || !Number.isFinite(currentMaxValue)) {
                  minEl.textContent = '';
                  maxEl.textContent = '';
                  return;
              }
              minEl.textContent = formatValue(currentMinValue);
              maxEl.textContent = formatValue(currentMaxValue);
          }
      </script>
  </body>
  </html>
  """

  return common.html_to_obj(html_content)