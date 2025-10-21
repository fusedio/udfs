import json
common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf
def udf(channel: str = "channel_1", sender_id: str = "my_udf"):
    L = fused.api.list('s3://fused-sample/')
    html = dropdown(channel="channel_1",options = L, sender_id="draw_1")
    return html

def map_html(
    channel: str = "channel_1",
    sender_id: str = "map_1",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -98.5,
    center_lat: float = 39.8,
    zoom: float = 3.0,
    style_url: str = "mapbox://styles/mapbox/dark-v10",
) -> str:
    return f"""<!doctype html>
<meta charset="utf-8">
<div id="map" style="position:fixed;inset:0;"></div>
<link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet">
<script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@main/channel.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  mapboxgl.accessToken = {json.dumps(mapbox_token)};
  const map = new mapboxgl.Map({{
    container: 'map',
    style: {json.dumps(style_url)},
    center: [{center_lng}, {center_lat}],
    zoom: {zoom},
    dragRotate: false,
    pitchWithRotate: false
  }});
  enableBoundsMessaging(map, {json.dumps(channel)}, {json.dumps(sender_id)});
}});
</script>
"""

def button_html(channel: str = "channel_1", sender_id: str = "button_1") -> str:
    return f"""<!doctype html>
<meta charset="utf-8">
<button id="btn" style="font-size:1.2rem;padding:0.6rem 1rem;margin:2rem;">Click me</button>
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@main/channel.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const btn = document.getElementById('btn');
  enableButtonMessaging(btn, {json.dumps(channel)}, {json.dumps(sender_id)}, {{ label: 'wowow' }}, 'click');
}});
</script>
"""

def dropdown(
    options: list,
    channel: str = "channel_1",
    sender_id: str = "dropdown_1",
    default_value: str | None = None,
    label: str = "Select an option:",
    placeholder: str = "— select —",
    return_html: bool = False,
):
    import json
    OPTIONS_JS = json.dumps(options, ensure_ascii=False)
    DEFAULT_JS = json.dumps(default_value, ensure_ascii=False)

    html = f"""<!doctype html>
<meta charset="utf-8">
<style>
body {{
  background: #121212;
  color: #eeeeee;
  margin: 0;
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100vh;
}}
.card {{
  background: #1e1e1e;
  padding: 2rem;
  border-radius: 8px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.6);
  font-family: system-ui, -apple-system, sans-serif;
  color: inherit;
  min-width: 280px;
}}
label {{
  display: block;
  margin-bottom: 0.5rem;
  font-weight: 500;
}}
select {{
  font-size: 1rem;
  padding: 0.5rem;
  min-width: 100%;
  border: 2px solid #444;
  border-radius: 4px;
  background: #2a2a2a;
  color: #ffffff;
}}
</style>

<div class="card">
  <label for="dropdown">{label}</label>
  <select id="dropdown"></select>
</div>

<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@main/channel.js"></script>
<script>
(function() {{
  const RAW_OPTIONS = {OPTIONS_JS};
  const DEFAULT_VALUE = {DEFAULT_JS};
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const PLACEHOLDER = {json.dumps(placeholder)};

  function normalize(options) {{
    if (!Array.isArray(options)) return [];
    return options.map((item, i) => {{
      if (['string','number','boolean'].includes(typeof item)) {{
        const s = String(item);
        return {{ value: s, label: s }};
      }}
      if (Array.isArray(item)) {{
        const v = item[0];
        const l = item[1] ?? v;
        return {{ value: String(v), label: String(l) }};
      }}
      if (item && typeof item === 'object') {{
        const val = item.value ?? item.id ?? item.key ?? item.name ?? item.path ?? item.url ?? ('opt_' + i);
        const lbl = item.label ?? item.name ?? item.title ?? item.text ?? val;
        return {{ value: String(val), label: String(lbl) }};
      }}
      const s = String(item);
      return {{ value: s, label: s }};
    }});
  }}

  function renderDropdown() {{
    const opts = normalize(RAW_OPTIONS);
    const sel = document.getElementById('dropdown');
    sel.innerHTML = '';
    const ph = document.createElement('option');
    ph.textContent = PLACEHOLDER;
    ph.disabled = true;
    ph.selected = true;
    sel.appendChild(ph);
    for (const o of opts) {{
      const opt = document.createElement('option');
      opt.value = o.value;
      opt.textContent = o.label;
      sel.appendChild(opt);
    }}
    if (DEFAULT_VALUE) {{
      const found = Array.from(sel.options).find(o => o.value === String(DEFAULT_VALUE));
      if (found) {{
        found.selected = true;
        ph.selected = false;
      }}
    }}
    enableDropdownMessaging(sel, CHANNEL, SENDER);
  }}

  document.addEventListener('DOMContentLoaded', renderDropdown);
}})();
</script>
"""
    return html if return_html else common.html_to_obj(html)


def map_draw_html(
    channel: str = "channel_1",
    sender_id: str = "draw_1",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -74.0,
    center_lat: float = 40.7,
    zoom: float = 12.0,
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    include_bounds: bool = True,
) -> str:
    return f"""<!doctype html>
<meta charset="utf-8">
<link href="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.css" rel="stylesheet">
<script src="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.js"></script>
<link rel="stylesheet" href="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.5.0/mapbox-gl-draw.css">
<script src="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.5.0/mapbox-gl-draw.js"></script>
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@main/channel.js"></script>

<style>
  html, body, #map {{ margin:0; height:100% }}
  #map {{ position:fixed; inset:0 }}
  #send {{
    position:fixed; right:10px; bottom:10px; z-index:10;
    padding:10px 14px; background:#fff; border:1px solid #999; border-radius:6px;
    font:14px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; cursor:pointer;
  }}
</style>

<div id="map"></div>
<button id="send" disabled>Send</button>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  mapboxgl.accessToken = {json.dumps(mapbox_token)};
  const map = new mapboxgl.Map({{
    container: 'map',
    style: {json.dumps(style_url)},
    center: [{center_lng}, {center_lat}],
    zoom: {zoom},
    dragRotate: false,
    pitchWithRotate: false
  }});

  const draw = new MapboxDraw({{
    displayControlsDefault: false,
    controls: {{ polygon:true, point:true, line_string:true, trash:true }},
    defaultMode: 'simple_select'
  }});
  map.addControl(draw, 'bottom-left');

  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const INCLUDE_BOUNDS = {str(include_bounds).lower()};

  const sendBtn = document.getElementById('send');

  // In-memory state only (no auto-send)
  let lastPoint = null;      // {{lat, lng}} or null
  let lastFC = null;         // GeoJSON FeatureCollection or null

  function enableSendIfData() {{
    const hasData = !!(lastFC && lastFC.features && lastFC.features.length);
    sendBtn.disabled = !hasData;
  }}

  function latestPointFrom(fc) {{
    const feats = fc?.features || [];
    const pts = feats.filter(f => f.geometry?.type === 'Point' && Array.isArray(f.geometry.coordinates));
    if (!pts.length) return null;
    const [lng, lat] = pts[pts.length - 1].geometry.coordinates;
    return {{ lat: Number(lat), lng: Number(lng) }};
  }}

  function updateStateFromDraw() {{
    const fc = draw.getAll();
    lastFC = (fc && Array.isArray(fc.features)) ? fc : null;
    lastPoint = lastFC ? latestPointFrom(lastFC) : null;
    enableSendIfData();
  }}

  function hookTrashToClearAll() {{
    const btn = document.querySelector('.mapbox-gl-draw_trash');
    if (!btn) return;
    btn.replaceWith(btn.cloneNode(true));
    const fresh = document.querySelector('.mapbox-gl-draw_trash');
    fresh.addEventListener('click', (e) => {{
      e.preventDefault(); e.stopPropagation();
      try {{ draw.deleteAll(); }} catch (_) {{}}
      lastPoint = null;
      lastFC = null;
      enableSendIfData();
    }});
  }}
  map.on('load', hookTrashToClearAll);

  // Draw events ONLY update state; they DO NOT publish
  ['draw.create','draw.update','draw.combine','draw.uncombine','draw.delete'].forEach(ev => {{
    map.on(ev, updateStateFromDraw);
  }});

  function collectVarsForSend() {{
    // Recompute just before sending to be safe
    updateStateFromDraw();
    if (!lastFC || !lastFC.features || !lastFC.features.length) return null;

    const vars = {{}};
    if (lastPoint) {{
      vars.lat = lastPoint.lat;
      vars.lng = lastPoint.lng;
    }}

    try {{ vars.geojson = JSON.stringify(lastFC); }} catch (_) {{}}

    if (INCLUDE_BOUNDS && map && map.getBounds) {{
      const b = map.getBounds();
      vars.bounds = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(',');
      vars.zoom = map.getZoom();
    }}

    return vars;
  }}

  sendBtn.addEventListener('click', (e) => {{
    e.preventDefault();
    const vars = collectVarsForSend();
    if (!vars) {{
      console.log('Nothing to send; draw a point/shape first.');
      return;
    }}
    publishVars(CHANNEL, SENDER, vars); // type:'vars' with payload=vars (per channel.js)
    console.log('SENT vars →', vars);
  }});
}});
</script>
"""


