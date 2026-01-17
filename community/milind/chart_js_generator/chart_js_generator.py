@fused.udf
def udf(
  chart: str = """{
    "type": "bar",
    "data": {
      "labels": ["Jan", "Feb", "Mar", "Apr", "May"],
      "datasets": [
        {"label": "Dogs", "data": [50, 60, 70, 180, 190]},
        {"label": "Cats", "data": [100, 200, 300, 400, 500]}
      ]
    },
    "options": { "responsive": true, "maintainAspectRatio": false }
  }""",
  background: str = "transparent",
  version: str = "4.4.1",
  device_pixel_ratio: float = 2.0,
):
  import json

  try:
    dpr = float(device_pixel_ratio)
  except Exception:
    dpr = 2.0
  dpr = max(1.0, min(4.0, dpr))

  if not isinstance(chart, str):
    chart = json.dumps(chart)

  if len(chart) > 200_000:
    raise ValueError("chart config too large (max 200k chars)")

  common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")

  html = f"""<!doctype html>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {{
    --bg: {background};
    --panel: rgba(0,0,0,0.55);
    --text: #fff;
    --muted: rgba(255,255,255,0.75);
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ margin:0; padding:0; width:100%; height:100%; background:var(--bg); font-family: system-ui, -apple-system, sans-serif; }}
  #wrap {{ width:100%; height:100%; padding: 10px; }}
  #card {{ width:100%; height:100%; position: relative; }}
  canvas {{ width:100% !important; height:100% !important; display:block; }}
  #err {{
    position:absolute; inset:0;
    display:none; align-items:center; justify-content:center;
    padding:16px;
    background: var(--panel);
    color: var(--text);
    border-radius: 10px;
    white-space: pre-wrap;
    font-size: 13px;
  }}
  #hint {{
    position:absolute; left:10px; bottom:8px;
    color: var(--muted); font-size: 11px;
    background: rgba(0,0,0,0.25);
    padding: 4px 6px;
    border-radius: 6px;
    pointer-events: none;
  }}
</style>

<div id="wrap">
  <div id="card">
    <div id="err"></div>
    <canvas id="c"></canvas>
    <div id="hint">Chart.js v{version} • dpr {dpr:g}</div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@{version}/dist/chart.umd.min.js"></script>
<script>
  const RAW_JSON = {json.dumps(chart)};
  const DPR = {dpr};

  function showError(msg) {{
    const el = document.getElementById('err');
    el.style.display = 'flex';
    el.textContent = String(msg || 'Unknown error');
    console.error('[chart-udf]', msg);
  }}

  function parseConfig(jsonStr) {{
    const s = String(jsonStr || '').trim();
    if (!s) throw new Error('Empty chart config');
    return JSON.parse(s);
  }}

  function normalize(cfg) {{
    if (!cfg || typeof cfg !== 'object') throw new Error('Chart config must be an object');
    if (!cfg.type) throw new Error('Missing required field: type');
    if (cfg.type === 'donut') cfg.type = 'doughnut';
    cfg.options = cfg.options || {{}};
    cfg.data = cfg.data || {{}};
    cfg.options.responsive = true;
    cfg.options.maintainAspectRatio = false;
    return cfg;
  }}

  const card = document.getElementById('card');
  const canvas = document.getElementById('c');
  let chartInstance = null;

  function resizeCanvasToContainer() {{
    const r = card.getBoundingClientRect();
    const w = Math.max(1, Math.floor(r.width));
    const h = Math.max(1, Math.floor(r.height));
    canvas.width = Math.floor(w * DPR);
    canvas.height = Math.floor(h * DPR);
    const ctx = canvas.getContext('2d');
    ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
    return ctx;
  }}

  function render() {{
    const ctx = resizeCanvasToContainer();
    const cfg = normalize(parseConfig(RAW_JSON));
    if (chartInstance) chartInstance.destroy();
    chartInstance = new Chart(ctx, cfg);
  }}

  try {{
    render();
    const ro = new ResizeObserver(() => {{
      try {{ render(); }} catch (e) {{ showError(e?.message || e); }}
    }});
    ro.observe(card);
    window.addEventListener('orientationchange', () => setTimeout(render, 100));
  }} catch (e) {{
    showError(e?.message || e);
  }}
</script>
"""
  return common.html_to_obj(html)