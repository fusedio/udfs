import json
common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

  
@fused.udf(cache_max_age=0)
def udf(channel: str = "channel_40"):

    html = button("Click me", channel=channel)
    return html


# BUTTON ----------------------------------------------------------------------
def button(
    label: str,
    *,
    key: str | None = None,
    help: str | None = None,
    on_click=None,
    disabled: bool = False,
    type: str = "primary",  # "primary" | "secondary"
    use_container_width: bool = False,
    # Internal params
    channel: str | None = None,
    sender_id: str | None = None,
    return_html: bool = False,
):
    """
    Streamlit-style button with neon accent.
    - Primary: #e8ff59 (bright yellow-green)
    - Secondary: dark gray minimal style
    """
    import json

    if key is None:
        key = f"button_{label.replace(' ', '_').lower()}"
    if channel is None:
        channel = f"channel_{key}"
    if sender_id is None:
        sender_id = key

    VALUE_JS = json.dumps(str(label))
    LABEL_JS = json.dumps(str(label))

    is_primary = type == "primary"

    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  html, body {{
    height:100%; margin:0; padding:0;
    background:#000; color:#fafafa;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
    display:flex; align-items:center; justify-content:center;
    padding:1rem;
  }}
  .container {{
    {"width:100%; max-width:500px;" if use_container_width else ""}
  }}
  button {{
    padding:0.45rem 0.9rem;
    border:1px solid {"transparent" if is_primary else "#333"};
    border-radius:0.3rem;
    background:{"#e8ff59" if is_primary else "#2a2a2a"};
    color:{"#000" if is_primary else "#fafafa"};
    font-size:0.9rem;
    font-weight:500;
    cursor:pointer;
    transition:all 0.18s ease;
    {"width:100%;" if use_container_width else ""}
    {"opacity:0.6; cursor:not-allowed;" if disabled else ""}
  }}
  button:hover:not(:disabled) {{
    background:{"#f0ff7a" if is_primary else "#3a3a3a"};
    border-color:{"transparent" if is_primary else "#444"};
  }}
  button:active:not(:disabled) {{
    transform:translateY(1px);
  }}
  button:focus {{
    outline:none;
    box-shadow:0 0 0 0.15rem {"rgba(232,255,89,0.4)" if is_primary else "rgba(250,250,250,0.1)"};
  }}
</style>
<div class="container">
  <button id="btn" {"disabled" if disabled else ""}></button>
</div>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const LABEL   = {LABEL_JS};
  const VALUE   = {VALUE_JS};
  const btn = document.getElementById('btn');
  btn.textContent = LABEL;
  
  btn.addEventListener('click', () => {{
    if (btn.disabled) return;
    window.parent.postMessage({{
      type: 'button',
      payload: {{ value: VALUE, label: LABEL }},
      origin: SENDER, channel: CHANNEL, ts: Date.now()
    }}, '*');
  }});
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)

# SELECTBOX --------------------------------------------------------------------
def selectbox(
    label: str,
    options,
    *,
    index: int | None = 0,
    format_func=None,
    placeholder: str | None = None,
    channel: str = "channel_1",
    sender_id: str = "selectbox_1",
    auto_send_on_load: bool = True,
    return_html: bool = False,
):
    import json

    if not isinstance(options, (list, tuple)):
        options = list(options)
    values = list(options)
    labels = [str(format_func(v)) if callable(format_func) else str(v) for v in values]

    opts = [{"value": str(values[i]), "label": labels[i]} for i in range(len(values))]
    OPTIONS_JS = json.dumps(opts, ensure_ascii=False)
    INDEX_JS = "null" if index is None else json.dumps(int(index))
    PLACE_JS = json.dumps(placeholder or "Select an option…")

    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  html, body {{
    height:100%; margin:0; padding:0;
    background:#121212; color:#eee;
    font-family:system-ui,-apple-system,sans-serif;
    display:flex; flex-direction:column;
    align-items:center; justify-content:center;
  }}
  label {{
    display:block;
    font-weight:500;
    font-size:clamp(14px, 1.6vmin, 18px);
    margin-bottom:.5rem;
  }}
  select {{
    width:80%; max-width:400px;
    font-size:clamp(14px, 1.6vmin, 18px);
    padding:.5rem;
    border:2px solid #444;
    border-radius:6px;
    background:#2a2a2a;
    color:#fff;
    outline:none;
  }}
</style>

<label for="sb">{label}</label>
<select id="sb"></select>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  const OPTIONS = {OPTIONS_JS};
  const INDEX   = {INDEX_JS};
  const PLACE   = {PLACE_JS};
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const AUTO    = {str(auto_send_on_load).lower()};
  const sel     = document.getElementById('sb');

  sel.innerHTML = '';
  const ph = document.createElement('option');
  ph.textContent = PLACE;
  ph.disabled = true;
  ph.selected = (INDEX === null);
  sel.appendChild(ph);

  for (const o of OPTIONS) {{
    const opt = document.createElement('option');
    opt.value = o.value;
    opt.textContent = o.label;
    sel.appendChild(opt);
  }}

  let initialValue = null;
  if (INDEX !== null) {{
    const targetIndex = Math.max(0, Math.min(OPTIONS.length - 1, Number(INDEX)));
    sel.selectedIndex = targetIndex + 1; // +1 for placeholder
    initialValue = OPTIONS[targetIndex]?.value ?? null;
  }}

  function post(val) {{
    window.parent.postMessage({{
      type:'selectbox',
      payload:{{ value: val }},
      origin:SENDER, channel:CHANNEL, ts:Date.now()
    }}, '*');
  }}

  sel.addEventListener('change', e => post(e.target.value));

  if (AUTO && initialValue !== null) {{
    queueMicrotask(() => post(initialValue));
  }}
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)




# SLIDER ----------------------------------------------------------------------
def slider(
    label: str = None,
    min_value=None,
    max_value=None,
    value=None,
    step=None,
    format: str | None = None,
    *,
    channel: str = "channel_1",
    sender_id: str = "slider_1",
    auto_send_on_load: bool = True,
    return_html: bool = False,
    send: str = "end",           # "end" | "continuous"
    debounce_ms: int = 64,
):
    import json

    is_range = isinstance(value, (list, tuple)) and len(value) == 2

    if min_value is None or max_value is None:
        if is_range:
            if min_value is None: min_value = value[0]
            if max_value is None: max_value = value[1]
        else:
            if min_value is None: min_value = 0
            if max_value is None: max_value = 100

    if value is None:
        value = (min_value, max_value) if is_range else min_value

    if step is None:
        nums = [min_value, max_value] + (list(value) if is_range else [value])
        step = 1 if all(isinstance(x, int) for x in nums) else 0.01

    fmt = format or "{val}"

    VAL_JS  = json.dumps(list(value) if is_range else value)
    MIN_JS  = json.dumps(min_value)
    MAX_JS  = json.dumps(max_value)
    STEP_JS = json.dumps(step)
    FMT_JS  = json.dumps(fmt)
    SEND_JS = json.dumps(send)
    DB_JS   = json.dumps(max(0, int(debounce_ms)))

    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {{
    --primary: #e8ff59;
    --primary-hover: #f0ff7a;
    --bg: #121212;
    --track-bg: #2a2a2a;
    --text: #eee;
    --text-muted: #999;
    --border: #3a3a3a;
  }}
  html, body {{
    height:100%; margin:0; padding:0;
    background:var(--bg); color:var(--text);
    font-family:system-ui,-apple-system,sans-serif;
    display:flex; flex-direction:column; align-items:center; justify-content:center;
    gap:.75rem;
  }}
  .label {{ 
    width:min(92vw, 520px);
    font-weight:500; 
    font-size:14px; 
    margin-bottom:.5rem;
  }}
  .slider-container {{
    width:min(92vw, 520px);
    display:flex; 
    gap:.5rem; 
    align-items:center;
  }}
  .slider-wrap {{ 
    position:relative; 
    display:flex; 
    align-items:center; 
    flex:1;
    height:20px;
  }}
  .track {{
    position:absolute; 
    left:0; right:0; 
    height:8px; 
    border-radius:999px;
    background:var(--track-bg);
  }}
  .fill {{
    position:absolute; 
    height:8px; 
    border-radius:999px;
    background:var(--primary);
  }}
  .value {{ 
    min-width:48px; 
    text-align:left; 
    color:var(--text-muted); 
    font-size:12px;
    font-variant-numeric:tabular-nums;
  }}
  input[type=range] {{
    appearance:none; -webkit-appearance:none;
    position:relative; 
    width:100%; 
    height:20px; 
    margin:0; 
    background:transparent; 
    outline:none;
    cursor:pointer;
  }}
  input[type=range]::-webkit-slider-thumb {{
    -webkit-appearance:none; 
    appearance:none;
    width:16px; 
    height:16px; 
    border-radius:50%;
    background:#fff; 
    border:1px solid var(--primary);
    box-shadow:1px 1px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
    cursor:pointer;
    transition:background-color 200ms;
  }}
  input[type=range]::-webkit-slider-thumb:hover {{
    background:var(--primary-hover);
  }}
  input[type=range]::-moz-range-thumb {{
    width:16px; 
    height:16px; 
    border-radius:50%;
    background:#fff; 
    border:1px solid var(--primary);
    box-shadow:1px 1px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
    cursor:pointer;
    transition:background-color 200ms;
  }}
  input[type=range]::-moz-range-thumb:hover {{
    background:var(--primary-hover);
  }}
  .tooltip {{
    position:absolute;
    bottom:100%;
    left:50%;
    transform:translateX(-50%);
    background:#fff;
    color:#000;
    padding:4px 10px;
    border-radius:6px;
    font-size:13px;
    white-space:nowrap;
    opacity:0;
    visibility:hidden;
    transition:opacity 150ms, visibility 150ms;
    margin-bottom:8px;
    pointer-events:none;
    box-shadow:0 2px 8px rgba(0,0,0,0.15);
  }}
  .thumb-container {{
    position:absolute;
    width:16px;
    height:20px;
    pointer-events:none;
  }}
  .thumb-container.show-tooltip .tooltip {{
    opacity:1;
    visibility:visible;
  }}
</style>

<div class="label">{label}</div>

<!-- Single -->
<div id="single" class="slider-container" style="display:none">
  <div class="slider-wrap">
    <div class="track"></div>
    <div class="fill" id="fill"></div>
    <div class="thumb-container" id="thumb-container">
      <div class="tooltip" id="tooltip"></div>
    </div>
    <input id="rng" type="range" />
  </div>
  <div class="value" id="valtxt"></div>
</div>

<!-- Range -->
<div id="range" class="slider-container" style="display:none">
  <div class="slider-wrap">
    <div class="track"></div>
    <div class="fill" id="fillr"></div>
    <div class="thumb-container" id="thumb-container0">
      <div class="tooltip" id="tooltip0"></div>
    </div>
    <div class="thumb-container" id="thumb-container1">
      <div class="tooltip" id="tooltip1"></div>
    </div>
    <input id="rng0" type="range" />
    <input id="rng1" type="range" />
  </div>
  <div class="value" id="valtxt"></div>
</div>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const MIN = {MIN_JS};
  const MAX = {MAX_JS};
  const STEP = {STEP_JS};
  const INIT = {VAL_JS};
  const FMT  = {FMT_JS};
  const AUTO = {str(auto_send_on_load).lower()};
  const SEND = {SEND_JS};
  const DEBOUNCE = {DB_JS};
  const isRange = Array.isArray(INIT);
  const valtxt = document.getElementById('valtxt');

  function fmt(v){{
    if (FMT.includes("{{val}}")) return FMT.replace("{{val}}", String(v));
    const m = FMT.match(/^%0?\\.(\\d+)f$/);
    if (m) return Number(v).toFixed(Number(m[1]));
    return String(v);
  }}

  function post(val){{
    window.parent.postMessage({{
      type:'slider',
      payload:{{ value: val }},
      origin:SENDER, channel:CHANNEL, ts:Date.now()
    }}, '*');
  }}

  function setFillSingle(input, fillEl, thumbContainer){{
    const p = (input.value - input.min) / (input.max - input.min);
    fillEl.style.left = "0";
    fillEl.style.width = (Math.max(0, Math.min(1, p)) * 100).toFixed(4) + "%";
    if(thumbContainer){{
      thumbContainer.style.left = (p * 100).toFixed(4) + "%";
      thumbContainer.style.transform = "translateX(-50%)";
    }}
  }}
  function setFillRange(i0, i1, fillEl, tc0, tc1){{
    const p0 = (i0.value - i0.min) / (i0.max - i0.min);
    const p1 = (i1.value - i1.min) / (i1.max - i1.min);
    const left = Math.min(p0, p1);
    const right = Math.max(p0, p1);
    fillEl.style.left = (left * 100).toFixed(4) + "%";
    fillEl.style.width = ((right - left) * 100).toFixed(4) + "%";
    if(tc0){{
      tc0.style.left = (p0 * 100).toFixed(4) + "%";
      tc0.style.transform = "translateX(-50%)";
    }}
    if(tc1){{
      tc1.style.left = (p1 * 100).toFixed(4) + "%";
      tc1.style.transform = "translateX(-50%)";
    }}
  }}

  function debounced(fn, ms){{
    let t=null, lastArgs=null;
    return (...a)=>{{lastArgs=a;clearTimeout(t);t=setTimeout(()=>fn(...lastArgs),ms);}};
  }}

  if (!isRange){{
    const wrap=document.getElementById('single');
    const fill=document.getElementById('fill');
    const rng=document.getElementById('rng');
    const thumbContainer=document.getElementById('thumb-container');
    const tooltip=document.getElementById('tooltip');
    wrap.style.display='flex';
    rng.min=MIN; rng.max=MAX; rng.step=STEP; rng.value=INIT;
    valtxt.textContent=fmt(INIT); 
    tooltip.textContent=fmt(INIT);
    setFillSingle(rng,fill,thumbContainer);
    const sendNow=()=>post(Number(rng.value));
    const sendDeb=debounced(sendNow,DEBOUNCE);

    rng.addEventListener('input',()=>{{
      valtxt.textContent=fmt(rng.value);
      tooltip.textContent=fmt(rng.value);
      setFillSingle(rng,fill,thumbContainer);
      if(SEND==="continuous")sendDeb();
    }});
    rng.addEventListener('mouseenter',()=>thumbContainer.classList.add('show-tooltip'));
    rng.addEventListener('mouseleave',()=>thumbContainer.classList.remove('show-tooltip'));
    rng.addEventListener('focus',()=>thumbContainer.classList.add('show-tooltip'));
    rng.addEventListener('blur',()=>thumbContainer.classList.remove('show-tooltip'));
    if(SEND==="end"){{
      rng.addEventListener('mouseup',sendNow);
      rng.addEventListener('touchend',sendNow);
    }}
    if(AUTO)queueMicrotask(sendNow);
  }} else {{
    const wrap=document.getElementById('range');
    const fill=document.getElementById('fillr');
    const r0=document.getElementById('rng0');
    const r1=document.getElementById('rng1');
    const tc0=document.getElementById('thumb-container0');
    const tc1=document.getElementById('thumb-container1');
    const tt0=document.getElementById('tooltip0');
    const tt1=document.getElementById('tooltip1');
    wrap.style.display='flex';
    r0.min=MIN;r0.max=MAX;r0.step=STEP;
    r1.min=MIN;r1.max=MAX;r1.step=STEP;
    r0.value=INIT[0];r1.value=INIT[1];
    const sendNow=()=>post([Number(r0.value),Number(r1.value)]);
    const sendDeb=debounced(sendNow,DEBOUNCE);
    function clamp(){{if(Number(r0.value)>Number(r1.value))r0.value=r1.value;}}
    function show(){{
      valtxt.textContent=fmt(r0.value)+" – "+fmt(r1.value);
      tt0.textContent=fmt(r0.value);
      tt1.textContent=fmt(r1.value);
      setFillRange(r0,r1,fill,tc0,tc1);
    }}
    r0.addEventListener('input',()=>{{clamp();show();if(SEND==="continuous")sendDeb();}});
    r1.addEventListener('input',()=>{{clamp();show();if(SEND==="continuous")sendDeb();}});
    r0.addEventListener('mouseenter',()=>tc0.classList.add('show-tooltip'));
    r0.addEventListener('mouseleave',()=>tc0.classList.remove('show-tooltip'));
    r0.addEventListener('focus',()=>tc0.classList.add('show-tooltip'));
    r0.addEventListener('blur',()=>tc0.classList.remove('show-tooltip'));
    r1.addEventListener('mouseenter',()=>tc1.classList.add('show-tooltip'));
    r1.addEventListener('mouseleave',()=>tc1.classList.remove('show-tooltip'));
    r1.addEventListener('focus',()=>tc1.classList.add('show-tooltip'));
    r1.addEventListener('blur',()=>tc1.classList.remove('show-tooltip'));
    if(SEND==="end"){{r0.addEventListener('mouseup',sendNow);r1.addEventListener('mouseup',sendNow);
                     r0.addEventListener('touchend',sendNow);r1.addEventListener('touchend',sendNow);}}
    show(); if(AUTO)queueMicrotask(sendNow);
  }}
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

# Text Input ----------------------------------------------------------------------

def text_input(
    label: str,
    *,
    placeholder: str = "Type something...",
    button_label: str = "Send",
    disabled: bool = False,
    channel: str = "channel_text",
    sender_id: str = "text_input_1",
    return_html: bool = False,
):
    import json

    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  html, body {{
    height:100%; margin:0; padding:0;
    background:#000; color:#fafafa;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
    display:flex; flex-direction:column; align-items:center; justify-content:center;
    gap:.75rem;
  }}
  label {{
    width:min(92vw, 520px);
    font-weight:500;
    font-size:clamp(14px, 1.6vmin, 18px);
  }}
  .row {{
    width:min(92vw, 520px);
    display:flex; gap:.5rem; align-items:center;
  }}
  input[type="text"] {{
    flex:1;
    box-sizing:border-box;
    padding:0.5rem 0.75rem;
    background:#1b1b1b;
    color:#fafafa;
    border:1px solid #333;
    border-radius:0.3rem;
    font-size:0.95rem;
    outline:none;
    {"opacity:.6; cursor:not-allowed;" if disabled else ""}
  }}
  input[type="text"]:hover:not(:disabled) {{
    border-color:#444;
  }}
  input[type="text"]:focus {{
    border-color:#555;
    background:#1f1f1f;
  }}
  ::placeholder {{
    color:#777;
    opacity:1;
  }}
  button {{
    padding:0.5rem 1rem;
    background:#2a2a2a;
    border:1px solid #444;
    border-radius:0.3rem;
    color:#fafafa;
    font-size:0.9rem;
    cursor:pointer;
    transition:background .15s, border-color .15s;
    {"opacity:.6; cursor:not-allowed;" if disabled else ""}
  }}
  button:hover:not(:disabled) {{
    background:#333;
    border-color:#555;
  }}
  button:active:not(:disabled) {{
    background:#3a3a3a;
  }}
</style>

<label for="txt">{label}</label>
<div class="row">
  <input id="txt" type="text" placeholder="{placeholder}" {("disabled" if disabled else "")}/>
  <button id="send" {("disabled" if disabled else "")}>{button_label}</button>
</div>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  const CHANNEL = {json.dumps(channel)};
  const SENDER  = {json.dumps(sender_id)};
  const input = document.getElementById('txt');
  const btn   = document.getElementById('send');

  btn.addEventListener('click', () => {{
    if (btn.disabled) return;
    const value = input.value.trim();
    if (!value) return;
    window.parent.postMessage({{
      type: 'text_input',
      payload: {{ value }},
      origin: SENDER, channel: CHANNEL, ts: Date.now()
    }}, '*');
  }});

  input.addEventListener('keydown', (e) => {{
    if (e.key === 'Enter' && !btn.disabled) {{
      btn.click();
    }}
  }});
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)
