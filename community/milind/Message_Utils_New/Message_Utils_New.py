import json
common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf
def udf(channel: str = "channel_40", sender_id: str = "my_udf"):
    html = button_html(channel=channel, sender_id=sender_id)
    return html


# BUTTON ----------------------------------------------------------------------
def button_html(channel="channel_1", sender_id="button_1", buttons=None):
    if buttons is None:
        buttons = [{"label": "Click me", "value": "clicked"}]
    BUTTONS_JS = json.dumps(buttons, ensure_ascii=False)
    return f"""<!doctype html>
<meta charset="utf-8">
<style>
body {{
  background:#121212;color:#eee;margin:0;padding:2rem;
  font-family:system-ui,-apple-system,sans-serif;
}}
button {{
  font-size:1rem;padding:0.5rem 1rem;margin:0.25rem;
  background:#2a2a2a;color:#fff;border:2px solid #444;border-radius:6px;cursor:pointer;
}}
button:hover {{ background:#333; }}
</style>
<div id="container"></div>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const BUTTONS = {BUTTONS_JS};
  const cont = document.getElementById('container');
  for (const b of BUTTONS) {{
    const btn = document.createElement('button');
    btn.textContent = b.label ?? b.value;
    btn.addEventListener('click', () => {{
      window.parent.postMessage({{
        type: 'button',
        payload: {{ key: b.label ?? b.value, value: b.value }},
        origin: SENDER, channel: CHANNEL, ts: Date.now()
      }}, '*');
    }});
    cont.appendChild(btn);
  }}
}});
</script>
"""



# DROPDOWN --------------------------------------------------------------------
def dropdown(
    options: list,
    channel: str = "channel_1",
    sender_id: str = "dropdown_1",
    default_value: str | None = None,
    label: str = "Select an option:",
    placeholder: str = "— select —",
    auto_send_on_load: bool = True,
    return_html: bool = False,
):
    import json
    OPTIONS_JS = json.dumps(options, ensure_ascii=False)
    DEFAULT_JS = json.dumps(default_value, ensure_ascii=False)
    html = f"""<!doctype html>
<meta charset="utf-8">
<style>
body {{ background:#121212;color:#eee;margin:0;width:100%;height:100%;overflow:hidden; }}
.card {{ background:#1e1e1e;padding:2rem;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,.6);font-family:system-ui,-apple-system,sans-serif;min-width:280px; }}
label{{display:block;margin-bottom:.5rem;font-weight:500;}}
select{{ font-size:1rem;padding:.5rem;width:100%;border:2px solid #444;border-radius:4px;background:#2a2a2a;color:#fff; }}
</style>
<div class="card">
  <label for="dropdown">{label}</label>
  <select id="dropdown"></select>
</div>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const RAW = {OPTIONS_JS};
  const DEF = {DEFAULT_JS};
  const CHANNEL = {json.dumps(channel)};
  const SENDER = {json.dumps(sender_id)};
  const PLACE = {json.dumps(placeholder)};
  const AUTO = {str(auto_send_on_load).lower()};
  const sel = document.getElementById('dropdown');

  function normalize(arr){{
    if(!Array.isArray(arr)) return [];
    return arr.map((x,i)=>{{
      if(['string','number','boolean'].includes(typeof x)){{const s=String(x);return{{value:s,label:s}};}}
      if(Array.isArray(x)){{const v=x[0],l=x[1]??v;return{{value:String(v),label:String(l)}};}}
      if(x&&typeof x==='object'){{const v=x.value??x.id??x.key??x.name??x.path??x.url??('opt_'+i);
        const l=x.label??x.name??x.title??x.text??v;return{{value:String(v),label:String(l)}};}}
      const s=String(x);return{{value:s,label:s}};
    }});
  }}

  const opts=normalize(RAW);
  sel.innerHTML='';
  const ph=document.createElement('option');
  ph.textContent=PLACE;ph.disabled=true;ph.selected=true;sel.appendChild(ph);
  for(const o of opts){{const opt=document.createElement('option');opt.value=o.value;opt.textContent=o.label;sel.appendChild(opt);}}

  let initialValue = null;
  if (DEF) {{
    const f=Array.from(sel.options).find(o=>o.value===String(DEF));
    if(f){{ f.selected=true; ph.selected=false; initialValue = f.value; }}
  }} else if (opts.length) {{
    // Select first option visually only if we will auto-send
    if (AUTO) {{
      sel.selectedIndex = 1; // 0 is placeholder
      initialValue = sel.value;
    }}
  }}

  sel.addEventListener('change',e=>{{
    window.parent.postMessage({{
      type:'dropdown',
      payload:{{value:e.target.value}},
      origin:SENDER,channel:CHANNEL,ts:Date.now()
    }},'*');
  }});

  if (AUTO && initialValue !== null) {{
    window.parent.postMessage({{
      type:'dropdown',
      payload:{{value: initialValue}},
      origin:SENDER,channel:CHANNEL,ts:Date.now()
    }},'*');
  }}
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)


# SLIDER ----------------------------------------------------------------------
def slider(
    channel: str = "channel_1",
    sender_id: str = "slider_1",
    label: str = "Select a value:",
    min_value: float = 0,
    max_value: float = 100,
    default_value: float | None = None,
    auto_send_on_load: bool = True,
    return_html: bool = False,
):
    if default_value is None:
        default_value = (min_value + max_value) / 2
    html = f"""<!doctype html>
<meta charset="utf-8">
<style>
body {{
  background:#121212;color:#eee;margin:0;height:100vh;display:flex;align-items:center;justify-content:center;
  font:14px system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;
}}
.card {{ background:#1e1e1e;padding:1.5rem 2rem;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,.6);width:320px; }}
label{{display:block;margin-bottom:.5rem;font-weight:500;}}
input[type=range]{{ width:100%;height:6px;background:#2a2a2a;border:2px solid #444;border-radius:4px;outline:none; }}
input[type=range]::-webkit-slider-thumb{{ -webkit-appearance:none;width:18px;height:18px;border-radius:50%;background:#eee;border:2px solid #444;cursor:pointer; }}
.val{{margin-top:.75rem;font-size:1rem;text-align:center;color:#ccc;}}
</style>
<div class="card">
  <label for="rng">{label}</label>
  <input id="rng" type="range" min="{min_value}" max="{max_value}" step="1" value="{default_value}">
  <div id="val" class="val">{default_value}</div>
</div>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const AUTO = {str(auto_send_on_load).lower()};
  const rng = document.getElementById('rng');
  const val = document.getElementById('val');
  let v = Number(rng.value);

  function post(valToSend) {{
    window.parent.postMessage({{
      type:'slider',
      payload:{{value: valToSend}},
      origin:SENDER,channel:CHANNEL,ts:Date.now()
    }},'*');
  }}

  if (AUTO) {{
    post(v);
  }}

  rng.addEventListener('input', () => {{
    v = Number(rng.value);
    val.textContent = v;
  }});

  const send = () => post(v);
  rng.addEventListener('mouseup', send);
  rng.addEventListener('touchend', send);
}});
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
    return_html: bool = False,
):
    html = f"""<!doctype html>
<meta charset="utf-8">
<link href="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.css" rel="stylesheet">
<script src="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.js"></script>
<link rel="stylesheet" href="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.5.0/mapbox-gl-draw.css">
<script src="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.5.0/mapbox-gl-draw.js"></script>

<style>
  html, body, #map {{ margin:0; height:100% }}
  #map {{ position:fixed; inset:0 }}
  #send {{
    position:fixed; right:10px; bottom:10px; z-index:10;
    padding:10px 14px; background:#fff; border:1px solid #999; border-radius:6px;
    font:14px/1.2 system-ui,-apple-system, Segoe UI, Roboto, Helvetica, Arial; cursor:pointer;
  }}
</style>

<div id="map"></div>
<button id="send" disabled>Send</button>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  mapboxgl.accessToken = {json.dumps(mapbox_token)};
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const INCLUDE_BOUNDS = {str(include_bounds).lower()};

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

  const sendBtn = document.getElementById('send');
  let lastFC = null;
  let lastPoint = null;

  function latestPointFrom(fc) {{
    const feats = fc?.features || [];
    const pts = feats.filter(f => f.geometry?.type === 'Point' && Array.isArray(f.geometry.coordinates));
    if (!pts.length) return null;
    const [lng, lat] = pts[pts.length - 1].geometry.coordinates;
    return {{ lat: Number(lat), lng: Number(lng) }};
  }}

  function refreshState() {{
    const fc = draw.getAll();
    lastFC = (fc && Array.isArray(fc.features)) ? fc : null;
    lastPoint = lastFC ? latestPointFrom(lastFC) : null;
    sendBtn.disabled = !(lastFC && lastFC.features && lastFC.features.length);
  }}

  function hookTrashToClearAll() {{
    const btn = document.querySelector('.mapbox-gl-draw_trash');
    if (!btn) return;
    btn.replaceWith(btn.cloneNode(true));
    const fresh = document.querySelector('.mapbox-gl-draw_trash');
    fresh.addEventListener('click', (e) => {{
      e.preventDefault(); e.stopPropagation();
      try {{ draw.deleteAll(); }} catch (_){{
      }}
      lastFC = null; lastPoint = null; sendBtn.disabled = true;
    }});
  }}
  map.on('load', hookTrashToClearAll);

  ['draw.create','draw.update','draw.combine','draw.uncombine','draw.delete']
    .forEach(ev => map.on(ev, refreshState));

  function collectVars() {{
    refreshState();
    if (!lastFC || !lastFC.features || !lastFC.features.length) return null;
    const vars = {{}};
    if (lastPoint) {{ vars.lat = lastPoint.lat; vars.lng = lastPoint.lng; }}
    try {{ vars.geojson = JSON.stringify(lastFC); }} catch (_){{
    }}
    if (INCLUDE_BOUNDS && map && map.getBounds) {{
      const b = map.getBounds();
      vars.bounds = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(',');
      vars.zoom = map.getZoom();
    }}
    return vars;
  }}

  sendBtn.addEventListener('click', () => {{
    const vars = collectVars();
    if (!vars) return;
    window.parent.postMessage({{
      type: 'vars',
      payload: {{ vars }},
      origin: SENDER,
      channel: CHANNEL,
      ts: Date.now()
    }}, '*');
  }});
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)
