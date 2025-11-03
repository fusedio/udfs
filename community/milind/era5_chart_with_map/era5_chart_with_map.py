@fused.udf
def udf(channel: str = "hexmap-bus", dataset: str = "era5"):
    import json

    channel_json = json.dumps(channel)
    dataset_json = json.dumps(dataset)

    template = """<!DOCTYPE html>
<html>
<head>
  <meta charset=\"UTF-8\">
  <title>Viewport Timeseries</title>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js\"></script>
  <style>
    :root {
      --bg: #050505;
      --panel: rgba(18,20,29,0.3);
      --border: rgba(255,255,255,0.08);
      --text: #dfe2e8;
      --muted: #9fa5b2;
      --accent: rgba(210,210,210,0.75);
    }

    html, body {
      margin: 0;
      height: 100%;
      background: #050505;
      color: var(--text);
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    #panel {
      display: flex;
      flex-direction: column;
      height: 100%;
      background: var(--panel);
      border-right: 1px solid var(--border);
      backdrop-filter: blur(22px);
      -webkit-backdrop-filter: blur(22px);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.02);
    }

    #header {
      padding: 14px 18px 10px 18px;
      background: rgba(8,10,16,0.36);
      border-bottom: 1px solid var(--border);
      backdrop-filter: blur(22px);
      -webkit-backdrop-filter: blur(22px);
    }

    #title {
      font-size: 14px;
      font-weight: 600;
      letter-spacing: 0.01em;
      margin-bottom: 4px;
    }

    #status {
      font-size: 11px;
      color: var(--muted);
    }

    #chart-wrap {
      flex: 1;
      min-height: 0;
      padding: 12px 16px 10px 16px;
      background: rgba(8,10,16,0.28);
      backdrop-filter: blur(22px);
      -webkit-backdrop-filter: blur(22px);
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.04);
      box-shadow: 0 18px 45px rgba(0,0,0,0.45);
    }

    #chart {
      width: 100%;
      height: 100%;
      background: rgba(0,0,0,0.75);
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.08);
      box-shadow: 0 14px 40px rgba(0,0,0,0.55), inset 0 0 0 1px rgba(255,255,255,0.04);
      backdrop-filter: blur(8px);
      -webkit-backdrop-filter: blur(8px);
    }

    #footer {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      padding: 10px 18px 14px 18px;
      font-size: 11px;
      color: var(--muted);
      background: rgba(8,10,16,0.36);
      border-top: 1px solid var(--border);
      backdrop-filter: blur(22px);
      -webkit-backdrop-filter: blur(22px);
    }

    #footer span strong {
      color: var(--text);
      font-weight: 600;
    }
  </style>
</head>
<body>
  <div id="panel">
    <div id="header">
      <div id="title">Viewport Daily Mean</div>
      <div id="status">Waiting for map…</div>
    </div>
    <div id="chart-wrap">
      <canvas id="chart"></canvas>
    </div>
    <div id="footer">
      <span id="meta-bounds"><strong>Bounds:</strong> —</span>
      <span id="meta-count"><strong>Samples:</strong> —</span>
      <span id="meta-updated"><strong>Updated:</strong> —</span>
    </div>
  </div>

  <script type="module">
    const CHANNEL = __CHANNEL__;
    const DATASET = __DATASET__;
    const componentId = 'hexchart-' + Math.random().toString(36).slice(2);

    let bc = null;
    try {
      if ('BroadcastChannel' in window) {
        bc = new BroadcastChannel(CHANNEL);
      }
    } catch (err) {
      console.warn('BroadcastChannel init failed', err);
    }

    let hasReceivedData = false;
    let requestTimer = null;
    let lastHoverMessage = null;

    function busSend(obj) {
      const payload = JSON.stringify(obj);
      try { if (bc) bc.postMessage(obj); } catch (err) { console.warn('BroadcastChannel send error', err); }
      try { window.parent.postMessage(payload, '*'); } catch (_) {}
      try { if (window.top && window.top !== window.parent) window.top.postMessage(payload, '*'); } catch (_) {}
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

    const statusEl = document.getElementById('status');
    const boundsEl = document.getElementById('meta-bounds');
    const countEl = document.getElementById('meta-count');
    const updatedEl = document.getElementById('meta-updated');

    const chartCtx = document.getElementById('chart');
    const chart = new Chart(chartCtx, {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: '',
          data: [],
          borderColor: 'rgba(120,120,120,0.35)',
          backgroundColor: 'rgba(120,120,120,0.18)',
          pointRadius: 0,
          borderWidth: 1,
          fill: false,
          tension: 0.25,
          order: 1,
          spanGaps: true
        }, {
          label: '',
          data: [],
          borderColor: 'rgba(120,120,120,0.35)',
          backgroundColor: 'rgba(120,120,120,0.18)',
          pointRadius: 0,
          borderWidth: 1,
          fill: '-1',
          tension: 0.25,
          order: 1,
          spanGaps: true
        }, {
          label: 'Avg temp (C)',
          data: [],
          borderColor: 'rgba(200,200,200,0.85)',
          backgroundColor: 'rgba(170,170,170,0.22)',
          tension: 0.25,
          pointRadius: 0,
          borderWidth: 0.8,
          fill: false,
          order: 2
        }, {
          label: 'Max temp (hover, C)',
          data: [],
          borderColor: 'rgba(240,240,0,0.85)',
          backgroundColor: 'rgba(240,240,240,0.12)',
          tension: 0.25,
          pointRadius: 1.2,
          borderWidth: 2,
          fill: false,
          order: 3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        scales: {
          x: {
            ticks: { maxTicksLimit: 8, color: '#cbd5f5', font: { size: 11 } },
            grid: { color: 'rgba(90,90,90,0.22)' }
          },
          y: {
            ticks: { color: '#cbd5f5', font: { size: 11 } },
            grid: { color: 'rgba(90,90,90,0.18)' }
          }
        },
        plugins: {
          legend: { display: true, labels: { color: '#e2e8f0', boxWidth: 12 } },
          tooltip: {
            callbacks: {
              label(ctx) {
                const v = ctx.parsed.y;
                if (!Number.isFinite(v)) return `${ctx.dataset.label}: —`;
                return `${ctx.dataset.label}: ${v.toFixed(2)} C`;
              }
            }
          }
        }
      }
    });

    function formatDate(value) {
      if (value == null) return '—';
      const num = Number(value);
      if (Number.isFinite(num)) {
        const d = new Date(num);
        if (!Number.isNaN(d.getTime())) {
          return d.toISOString().slice(0, 10);
        }
      }
      return String(value);
    }

    function formatBounds(bounds) {
      if (!bounds) return '—';
      const { west, south, east, north, zoom } = bounds;
      if (![west, south, east, north].every(Number.isFinite)) return '—';
      const zoomText = Number.isFinite(zoom) ? ` · z=${zoom.toFixed(1)}` : '';
      return `${south.toFixed(2)}°, ${west.toFixed(2)}° → ${north.toFixed(2)}°, ${east.toFixed(2)}°${zoomText}`;
    }

    function describeReason(reason) {
      if (!reason) return 'No data in bounds';
      return reason.replace(/[_-]+/g, ' ');
    }

    function shortHexLabel(hex) {
      if (!hex) return 'hover';
      const str = String(hex).toUpperCase();
      if (str.length <= 6) return str;
      return str.slice(0, 6) + '…';
    }

    function resolveMaxValue(row) {
      return Number(row?.max ?? row?.max_temp ?? row?.value ?? row?.metric ?? row?.daily_mean ?? row?.y ?? row?.v);
    }

    function updateHoverSeries(msg, { alignOnly = false } = {}) {
      lastHoverMessage = msg;
      const dataset = chart.data.datasets[3];
      const rows = Array.isArray(msg?.data) ? msg.data : [];
      const labels = chart.data.labels;

      if (!rows.length) {
        dataset.data = labels.map(() => null);
        dataset.label = 'Max temp (hover, C)';
        if (!alignOnly) chart.update('none');
        return;
      }

      const valueByLabel = new Map();
      for (const row of rows) {
        const dateLabel = formatDate(row?.date);
        const value = resolveMaxValue(row);
        if (dateLabel && Number.isFinite(value)) {
          valueByLabel.set(dateLabel, value);
        }
      }

      if (!labels.length) {
        const newLabels = Array.from(valueByLabel.keys());
        chart.data.labels = newLabels;
        for (let i = 0; i < chart.data.datasets.length - 1; i += 1) {
          chart.data.datasets[i].data = newLabels.map(() => null);
        }
      }

      dataset.data = chart.data.labels.map(label => {
        return valueByLabel.has(label) ? valueByLabel.get(label) : null;
      });
      dataset.label = `Max temp (${shortHexLabel(msg?.meta?.hex)} · C)`;

      if (!alignOnly) {
        chart.update('none');
      }
    }

    function updateChart(msg) {
      const rows = Array.isArray(msg?.data) ? msg.data : [];
      if (!rows.length) {
        const reasonKey = (msg?.meta?.reason || '').toLowerCase();
        const reason = describeReason(msg?.meta?.reason);
        if (reasonKey === 'fetching' && chart.data.datasets[2].data.length) {
          statusEl.textContent = reason;
          boundsEl.innerHTML = `<strong>Bounds:</strong> ${formatBounds(msg?.meta?.bounds)}`;
          const ts = msg?.timestamp ? new Date(msg.timestamp) : new Date();
          updatedEl.innerHTML = `<strong>Updated:</strong> ${ts.toLocaleTimeString()}`;
          return;
        }
        chart.data.labels = [];
        for (const ds of chart.data.datasets) {
          ds.data = [];
        }
        chart.data.datasets[3].label = 'Max temp (hover, C)';
        chart.update('none');
        statusEl.textContent = reason;
        boundsEl.innerHTML = `<strong>Bounds:</strong> ${formatBounds(msg?.meta?.bounds)}`;
        countEl.innerHTML = '<strong>Samples:</strong> —';
        const ts = msg?.timestamp ? new Date(msg.timestamp) : new Date();
        updatedEl.innerHTML = `<strong>Updated:</strong> ${ts.toLocaleTimeString()}`;
        return;
      }

      const labels = [];
      const minValues = [];
      const maxValues = [];
      const values = [];
      let totalSamples = 0;
      const monthBuckets = new Map(); // key => { samples: [{ idx, value }] }
      for (const row of rows) {
        labels.push(formatDate(row?.date));
        const avgVal = Number(row?.avg);
        const listIndex = values.length;
        if (Number.isFinite(avgVal)) {
          values.push(avgVal);
          const dateNum = Number(row?.date);
          if (Number.isFinite(dateNum)) {
            const d = new Date(dateNum);
            if (!Number.isNaN(d.getTime())) {
              const key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
              if (!monthBuckets.has(key)) {
                monthBuckets.set(key, []);
              }
              monthBuckets.get(key).push({ idx: listIndex, value: avgVal });
            }
          }
        } else {
          values.push(null);
        }
        const minVal = Number(row?.min ?? row?.min_temp);
        const maxVal = Number(row?.max ?? row?.max_temp);
        minValues.push(Number.isFinite(minVal) ? minVal : null);
        maxValues.push(Number.isFinite(maxVal) ? maxVal : null);
        const samples = Number(row?.samples);
        if (Number.isFinite(samples)) totalSamples += samples;
      }

      // Clip average temperatures to ±20% of the monthly mean (by calendar month)
      monthBuckets.forEach(entries => {
        if (!entries.length) return;
        const mean = entries.reduce((sum, entry) => sum + entry.value, 0) / entries.length;
        const tolerance = Math.abs(mean) * 0.2;
        const lower = mean - tolerance;
        const upper = mean + tolerance;
        for (const entry of entries) {
          const original = values[entry.idx];
          if (!Number.isFinite(original)) continue;
          const clamped = Math.min(Math.max(original, lower), upper);
          values[entry.idx] = Number(clamped.toFixed(3));
        }
      });

      chart.data.labels = labels;
      chart.data.datasets[0].data = minValues;
      chart.data.datasets[1].data = maxValues;
      chart.data.datasets[2].data = values;
      if (lastHoverMessage) {
        updateHoverSeries(lastHoverMessage, { alignOnly: true });
      } else {
        chart.data.datasets[3].data = labels.map(() => null);
        chart.data.datasets[3].label = 'Max temp (hover, C)';
      }

      const combined = chart.data.datasets
        .flatMap(ds => Array.isArray(ds.data) ? ds.data : [])
        .filter(value => Number.isFinite(value));
      if (combined.length) {
        const minY = Math.min(...combined);
        const maxY = Math.max(...combined);
        const span = Math.max(0.5, maxY - minY);
        const padding = Math.max(0.1, span * 0.08);
        chart.options.scales.y.suggestedMin = 0;
        chart.options.scales.y.suggestedMax = maxY + padding;
      } else {
        chart.options.scales.y.suggestedMin = 0;
        chart.options.scales.y.suggestedMax = undefined;
      }

      chart.update('none');

      statusEl.textContent = `Showing ${rows.length} day${rows.length === 1 ? '' : 's'} (${values.filter(v => Number.isFinite(v)).length} valid, Celsius)`;
      boundsEl.innerHTML = `<strong>Bounds:</strong> ${formatBounds(msg?.meta?.bounds)}`;
      countEl.innerHTML = `<strong>Samples:</strong> ${totalSamples ? totalSamples.toLocaleString() : '—'}`;
      const ts = msg?.timestamp ? new Date(msg.timestamp) : new Date();
      updatedEl.innerHTML = `<strong>Updated:</strong> ${ts.toLocaleTimeString()}`;

      if (!hasReceivedData) {
        hasReceivedData = true;
        if (requestTimer) {
          clearInterval(requestTimer);
          requestTimer = null;
        }
      }
    }

    function handleIncoming(raw) {
      const msg = tryParseMessage(raw);
      if (!msg) return;
      console.log('📨 Chart received:', msg.type, { dataset: msg.dataset, matches: datasetMatches(msg) });
      if (!datasetMatches(msg)) return;
      if (msg.fromComponent && msg.fromComponent === componentId) return;
      if (msg.type === 'hexmap_timeseries') {
        updateChart(msg);
        return;
      }
      if (msg.type === 'hexmap_hover_timeseries') {
        console.log('🟡 Updating hover series with', msg.data?.length, 'rows');
        updateHoverSeries(msg);
      }
    }

    if (bc) {
      bc.onmessage = ev => handleIncoming(ev.data);
    }
    window.addEventListener('message', ev => handleIncoming(ev.data));

    function requestInitial() {
      busSend({
        type: 'hexmap_request_timeseries',
        dataset: DATASET,
        fromComponent: componentId,
        timestamp: Date.now()
      });
    }

    requestInitial();
    requestTimer = setInterval(() => {
      if (!hasReceivedData) {
        requestInitial();
      } else if (requestTimer) {
        clearInterval(requestTimer);
        requestTimer = null;
      }
    }, 2000);
  </script>
</body>
</html>
"""

    html = template.replace("__CHANNEL__", channel_json).replace("__DATASET__", dataset_json)
    return html

