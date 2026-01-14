import json
from typing import Any

common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(parameter: str = "name"):
    # html = button("Click me", parameter=parameter)
    L = ['A','B','C','D']
    html = selectbox("This is a dropdown", parameter=parameter, options = L)
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
    size: str = "medium",  # "small" | "medium" | "large"
    # Internal params
    parameter: str | None = None,
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
    if parameter is None:
        parameter = f"channel_{key}"
    if sender_id is None:
        sender_id = key

    VALUE_JS = json.dumps(str(label))
    LABEL_JS = json.dumps(str(label))

    is_primary = type == "primary"
    
    # Size configurations
    size_configs = {
        "small": {"padding": "0.35rem 0.7rem", "font_size": "13px", "height": "28px"},
        "medium": {"padding": "0.5rem 1rem", "font_size": "14px", "height": "36px"},
        "large": {"padding": "0.65rem 1.25rem", "font_size": "15px", "height": "44px"},
    }
    size_config = size_configs.get(size, size_configs["medium"])

    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {{
    --primary: #e8ff59;
    --primary-hover: #f0ff7a;
    --bg: #000;
    --text: #fafafa;
    --secondary-bg: #2a2a2a;
    --secondary-hover: #3a3a3a;
    --border: #333;
  }}
  * {{
    box-sizing: border-box;
  }}
  html, body {{
    height:100%; 
    width:100%;
    margin:0; 
    padding:0;
    background:var(--bg); 
    color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
    display:flex; 
    align-items:center; 
    justify-content:center;
    padding:clamp(0rem, 0vw, 0rem);
  }}
  .container {{
    width:100%;
    height:100%;
    max-width:{("100%" if use_container_width else "min(500px, 100%)")};
    display:flex;
    object-fit:contain;
  }}
  button {{
    width:{"100%" if use_container_width else "auto"};
    min-width:{"auto" if use_container_width else "max(80px, fit-content)"};
    width:100%;
    padding:0#{size_config["padding"]};
    # height:{size_config["height"]};
    border:1px solid {"transparent" if is_primary else "var(--border)"};
    border-radius:6px;
    background:{"var(--primary)" if is_primary else "var(--secondary-bg)"};
    color:{"#000" if is_primary else "var(--text)"};
    font-size:{size_config["font_size"]};
    font-weight:500;
    cursor:pointer;
    transition:all 150ms ease;
    box-shadow:1px 1px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
    {"opacity:0.6; cursor:not-allowed;" if disabled else ""}
  }}
  button:hover:not(:disabled) {{
    background:{"var(--primary-hover)" if is_primary else "var(--secondary-hover)"};
    border-color:{"transparent" if is_primary else "#444"};
    box-shadow:2px 2px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  button:active:not(:disabled) {{
    transform:translateY(1px);
    box-shadow:1px 1px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  button:focus {{
    outline:none;
    box-shadow:0 0 0 2px {"rgba(232,255,89,0.4)" if is_primary else "rgba(250,250,250,0.15)"}, 1px 1px 0px 0px rgba(0,0,0,0.3);
  }}
  @media (max-width: 480px) {{
    button {{
      font-size:max(12px, {size_config["font_size"]});
      padding:clamp(0.35rem, 1.5vw, {size_config["padding"].split()[0]}) clamp(0.7rem, 3vw, {size_config["padding"].split()[1]});
    }}
  }}
</style>
<div class="container">
  <button id="btn" {"disabled" if disabled else ""}></button>
</div>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const PARAMETER = {json.dumps(parameter)};
  const SENDER  = {json.dumps(sender_id)};
  const LABEL   = {LABEL_JS};
  const VALUE   = {VALUE_JS};
  const btn = document.getElementById('btn');
  btn.textContent = LABEL;
  
  btn.addEventListener('click', () => {{
    if (btn.disabled) return;
    window.parent.postMessage({{
      type: 'param',
      parameter: PARAMETER,
      values: {{ value: VALUE, label: LABEL }},
      origin: SENDER,
      ts: Date.now()
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
    format_func=None,
    placeholder: Any = None,
    parameter: str = "channel_1",
    sender_id: str = "selectbox_1",
    auto_send_on_load: bool = True,
    return_html: bool = False,
):
    import json
    
    if not isinstance(options, (list, tuple)):
        options = list(options)
    
    if not options:
        raise ValueError("options cannot be empty")
    
    values = list(options)
    
    # Safe format_func with error handling
    labels = []
    for v in values:
        try:
            labels.append(str(format_func(v)) if callable(format_func) else str(v))
        except Exception as e:
            raise ValueError(f"format_func failed for value {v}: {e}")
    
    opts = [{"value": str(values[i]), "label": labels[i]} for i in range(len(values))]
    
    # Determine placeholder behavior: allow text or selecting an existing option
    placeholder_label = "Select an option…"
    placeholder_is_option = False
    resolved_index = None

    if placeholder is not None:
        try:
            idx = values.index(placeholder)
            placeholder_is_option = True
            placeholder_label = labels[idx]
            resolved_index = idx
        except ValueError:
            # Placeholder not found in options - use as disabled prompt text
            placeholder_label = str(placeholder) if not isinstance(placeholder, str) else placeholder
            print(f"[selectbox] Warning: placeholder value {repr(placeholder)} not found in options list.")

    OPTIONS_JS = json.dumps(opts, ensure_ascii=False)
    # If no placeholder selection and no explicit index, default to first item
    if resolved_index is None:
        resolved_index = 0

    INDEX_JS = "null" if resolved_index is None else json.dumps(int(resolved_index))
    PLACE_JS = json.dumps(placeholder_label)
    PLACE_IS_OPTION_JS = "true" if placeholder_is_option else "false"
    
    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    padding: 20px;
    min-height: 100%;
    background: transparent;
    color: #f3f4f6;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }}
  label {{
    font-size: 14px;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    color: rgba(255, 255, 255, 0.72);
  }}
  label:empty {{
    display: none;
  }}
  .select-wrapper {{
    position: relative;
    width: 100%;
    min-width: 220px;
  }}
  select {{
    width: 100%;
    padding: 0.65rem 1rem;
    border-radius: 12px;
    border: 1px solid rgba(255, 255, 255, 0.24);
    background: rgba(24, 24, 24, 0.45);
    color: #f9fafb;
    font-size: 14px;
    appearance: none;
    -webkit-appearance: none;
    -moz-appearance: none;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    transition: border-color 140ms ease, box-shadow 140ms ease, background 140ms ease;
    cursor: pointer;
  }}
  select:hover {{
    background: rgba(36, 36, 36, 0.6);
    border-color: rgba(255, 255, 255, 0.38);
    box-shadow: 0 12px 30px rgba(15, 23, 42, 0.22);
  }}
  select:focus-visible {{
    outline: 2px solid rgba(232, 255, 89, 0.5);
    outline-offset: 2px;
    border-color: rgba(232, 255, 89, 0.65);
    background: rgba(42, 42, 42, 0.65);
  }}
  select option {{
    color: #0f172a;
    background: #f8fafc;
  }}
  select option:disabled {{
    color: rgba(15, 23, 42, 0.6);
  }}
  .chevron {{
    pointer-events: none;
    position: absolute;
    right: 14px;
    top: 50%;
    transform: translateY(-50%);
    color: rgba(248, 250, 252, 0.7);
    font-size: 12px;
  }}
  @media (max-width: 640px) {{
    body {{
      padding: 16px;
      gap: 10px;
    }}
    label {{
      font-size: 12px;
      letter-spacing: 0.05em;
    }}
    select {{
      padding: 0.55rem 0.9rem;
      font-size: clamp(12px, 3.6vw, 14px);
    }}
  }}
</style>
</head>
<body>
<label for="sb" id="lbl">{label}</label>
<div class="select-wrapper">
  <select id="sb" aria-labelledby="lbl" aria-label="{label or 'Select box'}"></select>
  <span class="chevron">▾</span>
</div>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const OPTIONS = {OPTIONS_JS};
  const INDEX   = {INDEX_JS};
  const PLACE   = {PLACE_JS};
  const PLACE_IS_OPTION = {PLACE_IS_OPTION_JS};
  const PARAMETER = {json.dumps(parameter)};
  const SENDER  = {json.dumps(sender_id)};
  const AUTO    = {str(auto_send_on_load).lower()};
  const sel     = document.getElementById('sb');
  
  sel.innerHTML = '';
  
  if (!PLACE_IS_OPTION) {{
    const ph = document.createElement('option');
    ph.textContent = PLACE;
    ph.disabled = true;
    ph.selected = (INDEX === null);
    ph.value = '';
    sel.appendChild(ph);
  }}
  
  for (const o of OPTIONS) {{
    const opt = document.createElement('option');
    opt.value = o.value;
    opt.textContent = o.label;
    sel.appendChild(opt);
  }}
  
  let initialValue = null;
  if (INDEX !== null) {{
    const targetIndex = Math.max(0, Math.min(OPTIONS.length - 1, Number(INDEX)));
    sel.selectedIndex = targetIndex + (PLACE_IS_OPTION ? 0 : 1);
    initialValue = OPTIONS[targetIndex]?.value ?? null;
  }} else if (!PLACE_IS_OPTION) {{
    sel.selectedIndex = 0;
  }}
  
  function post(val) {{
    if (val === '') return;
    window.parent.postMessage({{
      type: 'param',
      parameter: PARAMETER,
      values: val,
      origin: SENDER,
      ts: Date.now()
    }}, '*');
  }}
  
  sel.addEventListener('change', e => {{
    requestAnimationFrame(() => post(e.target.value));
  }});
  
  if (AUTO && initialValue !== null) {{
    queueMicrotask(() => post(initialValue));
  }}
}});
</script>
</body>
</html>
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
    parameter: str = "channel_1",
    sender_id: str = "slider_1",
    auto_send_on_load: bool = True,
    return_html: bool = False,
    send: str = "end",  # "end" | "continuous"
    debounce_ms: int = 64,
):
    import json

    is_range = isinstance(value, (list, tuple)) and len(value) == 2

    if min_value is None or max_value is None:
        if is_range:
            if min_value is None:
                min_value = value[0]
            if max_value is None:
                max_value = value[1]
        else:
            if min_value is None:
                min_value = 0
            if max_value is None:
                max_value = 100

    if value is None:
        value = (min_value, max_value) if is_range else min_value

    if step is None:
        nums = [min_value, max_value] + (list(value) if is_range else [value])
        step = 1 if all(isinstance(x, int) for x in nums) else 0.01

    fmt = format or "{val}"

    VAL_JS = json.dumps(list(value) if is_range else value)
    MIN_JS = json.dumps(min_value)
    MAX_JS = json.dumps(max_value)
    STEP_JS = json.dumps(step)
    FMT_JS = json.dumps(fmt)
    SEND_JS = json.dumps(send)
    DB_JS = json.dumps(max(0, int(debounce_ms)))

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ {{ box-sizing: border-box; }} }}
  body {{
    margin: 0;
    padding: 24px 22px;
    min-height: 100%;
    background: transparent;
    color: #f4f4f5;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
  }}
  .label {{
    width: 100%;
    max-width: 480px;
    font-size: 13px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: rgba(255, 255, 255, 0.7);
  }}
  .label:empty {{ {{ display: none; }} }}
  .slider-container {{
    width: 100%;
    max-width: 480px;
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 18px 20px;
    border-radius: 16px;
    border: 1px solid rgba(255, 255, 255, 0.08);
    background: #000;
  }}
  .slider-wrap {{
    position: relative;
    flex: 1;
    display: flex;
    align-items: center;
    height: 36px;
  }}
  .track {{
    position: absolute;
    top: 50%;
    left: 12px;
    right: 12px;
    height: 18px;
    transform: translateY(-50%);
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.12);
    overflow: hidden;
  }}
  .fill {{
    position: absolute;
    top: 50%;
    left: 12px;
    height: 18px;
    transform: translateY(-50%);
    border-radius: 999px;
    background: #e8ff59;
    box-shadow: 0 8px 20px rgba(232, 255, 89, 0.25);
    width: 0;
  }}
  .value {{
    min-width: 76px;
    text-align: right;
    color: rgba(255, 255, 255, 0.64);
    font-size: 13px;
    font-variant-numeric: tabular-nums;
  }}
  input[type=range] {{
    appearance: none;
    -webkit-appearance: none;
    width: 100%;
    background: transparent;
    margin: 0;
    height: 36px;
    position: absolute;
    left: 0;
    right: 0;
    top: 50%;
    transform: translateY(-50%);
    cursor: pointer;
    outline: none;
  }}
  input[type=range]::-webkit-slider-thumb {{
    -webkit-appearance: none;
    appearance: none;
    width: 18px;
    height: 18px;
    border-radius: 50%;
    background: #ffffff;
    border: 2px solid rgba(0, 0, 0, 0.4);
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.25);
    transition: transform 140ms ease, box-shadow 140ms ease;
  }}
  input[type=range]::-webkit-slider-thumb:hover {{
    transform: scale(1.06);
    box-shadow: 0 10px 24px rgba(0, 0, 0, 0.32);
  }}
  input[type=range]::-moz-range-thumb {{
    width: 18px;
    height: 18px;
    border-radius: 50%;
    background: #ffffff;
    border: 2px solid rgba(0, 0, 0, 0.4);
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.25);
    transition: transform 140ms ease, box-shadow 140ms ease;
  }}
  input[type=range]::-moz-range-thumb:hover {{
    transform: scale(1.06);
    box-shadow: 0 10px 24px rgba(0, 0, 0, 0.32);
  }}
  .thumb-container {{
    position: absolute;
    top: 50%;
    left: 12px;
    transform: translate(-50%, -50%);
    pointer-events: none;
    width: 0;
    height: 0;
  }}
  .tooltip {{
    position: absolute;
    bottom: calc(100% + 8px);
    left: 50%;
    transform: translateX(-50%) scale(0.94);
    background: rgba(20, 20, 20, 0.92);
    color: #f4f4f5;
    border: 1px solid rgba(255, 255, 255, 0.16);
    border-radius: 8px;
    padding: 6px 12px;
    font-size: 12px;
    letter-spacing: 0.04em;
    font-variant-numeric: tabular-nums;
    opacity: 0;
    pointer-events: none;
    transition: opacity 140ms ease, transform 140ms ease;
    box-shadow: 0 12px 22px rgba(15, 23, 42, 0.25);
  }}
  .thumb-container.show-tooltip .tooltip {{ {{ opacity: 1; transform: translateX(-50%) scale(1); }} }}
  @media (max-width: 640px) {{
    body {{
      padding: 18px 16px;
      gap: 12px;
    }}
    .slider-container {{
      padding: 16px;
      gap: 12px;
    }}
    .value {{
      min-width: 64px;
      font-size: clamp(12px, 3.4vw, 13px);
    }}
    input[type=range]::-webkit-slider-thumb,
    input[type=range]::-moz-range-thumb {{ {{
      width: 16px;
      height: 16px;
    }} }}
  }}
</style>
</head>
<body>
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
  const PARAMETER = {json.dumps(parameter)};
  const SENDER = {json.dumps(sender_id)};
  const MIN = {MIN_JS};
  const MAX = {MAX_JS};
  const STEP = {STEP_JS};
  const INIT = {VAL_JS};
  const FMT = {FMT_JS};
  const AUTO = {str(auto_send_on_load).lower()};
  const SEND = {SEND_JS};
  const DEBOUNCE = {DB_JS};
  const isRange = Array.isArray(INIT);
  const valtxt = document.getElementById('valtxt');

  function fmt(v) {{
    if (FMT.includes("{{{{val}}}}")) return FMT.replace("{{{{val}}}}", String(v));
    const m = FMT.match(/^%0?\\.(\\d+)f$/);
    if (m) return Number(v).toFixed(Number(m[1]));
    return String(v);
  }}

  function post(val) {{
    window.parent.postMessage({{
      type: Array.isArray(val) ? 'range' : 'param',
      parameter: PARAMETER,
      values: val,
      origin: SENDER,
      ts: Date.now()
    }}, '*');
  }}

  function setFillSingle(input, fillEl, thumbContainer, trackEl) {{
    const min = Number(input.min);
    const max = Number(input.max);
    const value = Number(input.value);
    const percent = Math.max(0, Math.min(1, (value - min) / ((max - min) || 1)));
    const wrapRect = trackEl.parentElement.getBoundingClientRect();
    const trackRect = trackEl.getBoundingClientRect();
    const trackLeft = trackRect.left - wrapRect.left;
    const trackWidth = trackRect.width;
    const cursor = trackLeft + percent * trackWidth;
    fillEl.style.left = trackLeft + 'px';
    fillEl.style.width = (percent * trackWidth) + 'px';
    if (thumbContainer) {{
      thumbContainer.style.left = cursor + 'px';
      thumbContainer.style.transform = 'translate(-50%, -50%)';
    }}
  }}

  function setFillRange(i0, i1, fillEl, tc0, tc1, trackEl) {{
    const min = Number(i0.min);
    const max = Number(i0.max);
    const range = (max - min) || 1;
    const p0 = Math.max(0, Math.min(1, (Number(i0.value) - min) / range));
    const p1 = Math.max(0, Math.min(1, (Number(i1.value) - min) / range));
    const wrapRect = trackEl.parentElement.getBoundingClientRect();
    const trackRect = trackEl.getBoundingClientRect();
    const trackLeft = trackRect.left - wrapRect.left;
    const trackWidth = trackRect.width;
    const leftP = Math.min(p0, p1);
    const rightP = Math.max(p0, p1);
    const start = trackLeft + leftP * trackWidth;
    const cursor0 = trackLeft + p0 * trackWidth;
    const cursor1 = trackLeft + p1 * trackWidth;
    fillEl.style.left = start + 'px';
    fillEl.style.width = Math.max(0, (rightP - leftP) * trackWidth) + 'px';
    if (tc0) {{
      tc0.style.left = cursor0 + 'px';
      tc0.style.transform = 'translate(-50%, -50%)';
    }}
    if (tc1) {{
      tc1.style.left = cursor1 + 'px';
      tc1.style.transform = 'translate(-50%, -50%)';
    }}
  }}

  function debounced(fn, ms) {{
    let t = null, lastArgs = null;
    return (...a) => {{
      lastArgs = a;
      clearTimeout(t);
      t = setTimeout(() => fn(...lastArgs), ms);
    }};
  }}

  if (!isRange) {{
    const wrap = document.getElementById('single');
    const fill = document.getElementById('fill');
    const rng = document.getElementById('rng');
    const thumbContainer = document.getElementById('thumb-container');
    const tooltip = document.getElementById('tooltip');
    const track = wrap.querySelector('.track');
    wrap.style.display = 'flex';
    rng.min = MIN; rng.max = MAX; rng.step = STEP; rng.value = INIT;
    valtxt.textContent = fmt(INIT);
    tooltip.textContent = fmt(INIT);
    const refreshSingleLayout = () => setFillSingle(rng, fill, thumbContainer, track);
    refreshSingleLayout();
    requestAnimationFrame(refreshSingleLayout);
    if (typeof ResizeObserver === 'function') {{
      const ro = new ResizeObserver(refreshSingleLayout);
      ro.observe(wrap);
    }} else {{
      window.addEventListener('resize', refreshSingleLayout);
    }}
    const sendNow = () => post(Number(rng.value));
    const sendDeb = debounced(sendNow, DEBOUNCE);
    let rafId = null;

    rng.addEventListener('input', () => {{
      if (rafId) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(() => {{
        valtxt.textContent = fmt(rng.value);
        tooltip.textContent = fmt(rng.value);
        setFillSingle(rng, fill, thumbContainer, track);
        rafId = null;
      }});
      if (SEND === "continuous") sendDeb();
    }});
    rng.addEventListener('mouseenter', () => thumbContainer.classList.add('show-tooltip'));
    rng.addEventListener('mouseleave', () => thumbContainer.classList.remove('show-tooltip'));
    rng.addEventListener('focus', () => thumbContainer.classList.add('show-tooltip'));
    rng.addEventListener('blur', () => thumbContainer.classList.remove('show-tooltip'));
    if (SEND === "end") {{
      rng.addEventListener('mouseup', sendNow);
      rng.addEventListener('touchend', sendNow);
    }}
    if (AUTO) queueMicrotask(sendNow);
  }} else {{
    const wrap = document.getElementById('range');
    const fill = document.getElementById('fillr');
    const r0 = document.getElementById('rng0');
    const r1 = document.getElementById('rng1');
    const tc0 = document.getElementById('thumb-container0');
    const tc1 = document.getElementById('thumb-container1');
    const tt0 = document.getElementById('tooltip0');
    const tt1 = document.getElementById('tooltip1');
    const track = wrap.querySelector('.track');
    wrap.style.display = 'flex';
    r0.min = MIN; r0.max = MAX; r0.step = STEP;
    r1.min = MIN; r1.max = MAX; r1.step = STEP;
    r0.value = INIT[0]; r1.value = INIT[1];
    const sendNow = () => post([Number(r0.value), Number(r1.value)]);
    const sendDeb = debounced(sendNow, DEBOUNCE);
    let rafId = null;
    function clamp() {{ if (Number(r0.value) > Number(r1.value)) r0.value = r1.value; }}
    function show() {{
      valtxt.textContent = fmt(r0.value) + " – " + fmt(r1.value);
      tt0.textContent = fmt(r0.value);
      tt1.textContent = fmt(r1.value);
      setFillRange(r0, r1, fill, tc0, tc1, track);
    }}
    r0.addEventListener('input', () => {{
      if (rafId) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(() => {{ clamp(); show(); rafId = null; }});
      if (SEND === "continuous") sendDeb();
    }});
    r1.addEventListener('input', () => {{
      if (rafId) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(() => {{ clamp(); show(); rafId = null; }});
      if (SEND === "continuous") sendDeb();
    }});
    r0.addEventListener('mouseenter', () => tc0.classList.add('show-tooltip'));
    r0.addEventListener('mouseleave', () => tc0.classList.remove('show-tooltip'));
    r0.addEventListener('focus', () => tc0.classList.add('show-tooltip'));
    r0.addEventListener('blur', () => tc0.classList.remove('show-tooltip'));
    r1.addEventListener('mouseenter', () => tc1.classList.add('show-tooltip'));
    r1.addEventListener('mouseleave', () => tc1.classList.remove('show-tooltip'));
    r1.addEventListener('focus', () => tc1.classList.add('show-tooltip'));
    r1.addEventListener('blur', () => tc1.classList.remove('show-tooltip'));
    if (SEND === "end") {{
      r0.addEventListener('mouseup', sendNow);
      r1.addEventListener('mouseup', sendNow);
      r0.addEventListener('touchend', sendNow);
      r1.addEventListener('touchend', sendNow);
    }}
    const refreshRangeLayout = () => show();
    refreshRangeLayout();
    requestAnimationFrame(refreshRangeLayout);
    if (typeof ResizeObserver === 'function') {{
      const ro = new ResizeObserver(refreshRangeLayout);
      ro.observe(wrap);
    }} else {{
      window.addEventListener('resize', refreshRangeLayout);
    }}
    if (AUTO) queueMicrotask(sendNow);
  }}
}});
</script>
</html>
"""
    return html if return_html else common.html_to_obj(html)

    
def map_draw_html(
    parameter: str = "channel_1",
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
  const PARAMETER = {json.dumps(parameter)};
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
      type: 'param',
      parameter: PARAMETER,
      values: vars,
      origin: SENDER,
      ts: Date.now()
    }}, '*');
  }});
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)


def map_bounds(
    parameter: str = "channel_1",
    sender_id: str = "map_bounds_1",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -74.0,
    center_lat: float = 40.7,
    zoom: float = 12.0,
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    button_label: str = "Send View",
    return_html: bool = False,
):
    import json

    html = f"""<!doctype html>
<meta charset="utf-8">
<link href="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.css" rel="stylesheet">
<script src="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.js"></script>
<link href="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-geocoder/v5.0.2/mapbox-gl-geocoder.css" rel="stylesheet">
<script src="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-geocoder/v5.0.2/mapbox-gl-geocoder.min.js"></script>

<style>
  html, body, #map {{ margin:0; height:100% }}
  #map {{ position:fixed; inset:0 }}
  #send {{
    position:fixed; right:10px; bottom:40px; z-index:10;
    padding:10px 14px; background:#fff; border:1px solid #999; border-radius:6px;
    font:14px/1.2 system-ui,-apple-system, Segoe UI, Roboto, Helvetica, Arial;
    cursor:pointer;
    transition: background 150ms ease, border-color 150ms ease, transform 150ms ease;
    box-shadow:0 4px 10px rgba(0,0,0,0.15);
  }}
  #send:disabled {{
    opacity:0.6; cursor:not-allowed;
  }}
  #send:not(:disabled):hover {{
    background:#f3f4f6;
    border-color:#6b7280;
    transform:translateY(-1px);
    box-shadow:0 6px 16px rgba(0,0,0,0.22), 0 0 0 3px rgba(255,255,255,0.18);
  }}
  #send:not(:disabled):active {{
    background:#e4e4e4;
    transform:translateY(0);
  }}
  .mapboxgl-ctrl-geocoder {{
    min-width:240px;
    background:rgba(30,30,30,0.95);
    color:#fff;
    border:1px solid rgba(255,255,255,0.15);
  }}
  .mapboxgl-ctrl-geocoder input,
  .mapboxgl-ctrl-geocoder input[type="text"],
  .mapboxgl-ctrl-geocoder--input {{
    color:#fff !important;
    background:transparent;
  }}
  .mapboxgl-ctrl-geocoder input::placeholder {{
    color:rgba(255,255,255,0.5);
  }}
  .mapboxgl-ctrl-geocoder--icon-search {{
    fill:rgba(255,255,255,0.6);
  }}
  .mapboxgl-ctrl-geocoder--icon-close {{
    fill:rgba(255,255,255,0.6);
  }}
  .mapboxgl-ctrl-geocoder--button {{
    background:transparent;
  }}
  .mapboxgl-ctrl-geocoder .suggestions {{
    background:rgba(30,30,30,0.95);
    border:1px solid rgba(255,255,255,0.15);
  }}
  .mapboxgl-ctrl-geocoder .suggestions > li > a {{
    color:#fff;
  }}
  .mapboxgl-ctrl-geocoder .suggestions > .active > a,
  .mapboxgl-ctrl-geocoder .suggestions > li > a:hover {{
    background:rgba(255,255,255,0.1);
    color:#fff;
  }}
  /* Full-width top banner for sent bounds */
  #toast {{
    position:fixed; top:0; left:0; right:0;
    background:rgba(18,18,18,0.92); color:#fff;
    padding:7px 12px;
    padding-right:56px; /* leave room for search icon */
    border-bottom:1px solid rgba(255,255,255,0.12);
    opacity:0; pointer-events:none; z-index:100;
    transition:opacity 0.2s ease;
  }}
  #toast.show {{ opacity:1; pointer-events:auto; }}
  #toast-content {{
    font-family:'SF Mono',Consolas,monospace; font-size:12px;
    user-select:all; cursor:text;
    white-space:nowrap;
    overflow-x:auto; overflow-y:hidden;
    -webkit-overflow-scrolling:touch;
    scrollbar-width:thin;
    display:block;
  }}

  /* Keep the search icon accessible without pushing it down */
  .mapboxgl-ctrl-top-right {{
    top:15px !important;
    right:6px !important;
    z-index:110;
  }}
</style>

<div id="map"></div>
<div id="toast"><span id="toast-content"></span></div>
<button id="send" disabled></button>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  mapboxgl.accessToken = {json.dumps(mapbox_token)};
  const PARAMETER = {json.dumps(parameter)};
  const SENDER  = {json.dumps(sender_id)};
  const BUTTON_LABEL = {json.dumps(button_label)};

  const map = new mapboxgl.Map({{
    container: 'map',
    style: {json.dumps(style_url)},
    center: [{center_lng}, {center_lat}],
    zoom: {zoom},
    dragRotate: false,
    pitchWithRotate: false
  }});

  const geocoder = new MapboxGeocoder({{
    accessToken: mapboxgl.accessToken,
    mapboxgl: mapboxgl,
    marker: false,
    collapsed: true,
    placeholder: 'Search location...'
  }});
  map.addControl(geocoder, 'top-right');

  const sendBtn = document.getElementById('send');
  sendBtn.textContent = BUTTON_LABEL;
  let lastPayload = null;

  function captureView() {{
    if (!map || typeof map.getBounds !== 'function') return null;
    const bounds = map.getBounds();
    if (!bounds) return null;
    const numbers = [
      Number(bounds.getWest()),
      Number(bounds.getSouth()),
      Number(bounds.getEast()),
      Number(bounds.getNorth())
    ];
    if (numbers.some(v => !Number.isFinite(v))) return null;
    return numbers;
  }}

  function refresh() {{
    const payload = captureView();
    if (!payload) return;
    lastPayload = payload;
    sendBtn.disabled = false;
  }}

  map.on('load', refresh);
  ['moveend','zoomend','rotateend','pitchend'].forEach(evt => map.on(evt, refresh));

  const toast = document.getElementById('toast');
  const toastContent = document.getElementById('toast-content');
  let toastTimeout = null;

  function showToast(content) {{
    if (toastTimeout) clearTimeout(toastTimeout);
    toast.classList.remove('show');
    toastContent.textContent = JSON.stringify(content);
    toast.classList.add('show');
    toastTimeout = setTimeout(() => toast.classList.remove('show'), 7000);
  }}


  sendBtn.addEventListener('click', () => {{
    if (sendBtn.disabled) return;
    if (!lastPayload) refresh();
    if (!lastPayload) return;
    window.parent.postMessage({{
      type: 'param',
      parameter: PARAMETER,
      values: {{
        west: lastPayload[0],
        south: lastPayload[1],
        east: lastPayload[2],
        north: lastPayload[3],
      }},
      origin: SENDER,
      ts: Date.now()
    }}, '*');
    showToast({{
      west: lastPayload[0],
      south: lastPayload[1],
      east: lastPayload[2],
      north: lastPayload[3],
    }});
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
    parameter: str = "channel_text",
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
  const PARAMETER = {json.dumps(parameter)};
  const SENDER  = {json.dumps(sender_id)};
  const input = document.getElementById('txt');
  const btn   = document.getElementById('send');

  btn.addEventListener('click', () => {{
    if (btn.disabled) return;
    const value = input.value.trim();
    if (!value) return;
    window.parent.postMessage({{
      type: 'param',
      parameter: PARAMETER,
      values: value,
      origin: SENDER,
      ts: Date.now()
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


def date_input(
    label: str,
    *,
    value: str | None = None,  # ISO format "YYYY-MM-DD" or None
    min_date: str | None = None,
    max_date: str | None = None,
    parameter: str = "channel_date",
    sender_id: str = "date_input_1",
    auto_send_on_load: bool = True,
    return_html: bool = False,
):
    import json
    from datetime import datetime, timedelta
    
    # Default to today if no value provided
    if value is None:
        value = datetime.now().strftime("%Y-%m-%d")
    
    # Default min/max to +/- 10 years if not provided
    if min_date is None:
        min_date = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
    if max_date is None:
        max_date = (datetime.now() + timedelta(days=3650)).strftime("%Y-%m-%d")
    
    VALUE_JS = json.dumps(value)
    MIN_JS = json.dumps(min_date)
    MAX_JS = json.dumps(max_date)
    
    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {{
    --primary: #e8ff59;
    --primary-hover: #f0ff7a;
    --bg: #121212;
    --text: #eee;
    --text-muted: #999;
    --border: #333;
    --input-bg: #1b1b1b;
    --input-hover: #2a2a2a;
    --primary-dim: rgba(232, 255, 89, 0.1);
  }}
  * {{
    box-sizing: border-box;
  }}
  html, body {{
    height:100vh; margin:0; padding:0;
    background:var(--bg); color:var(--text);
    font-family:system-ui,-apple-system,sans-serif;
    display:flex; flex-direction:column; align-items:center; justify-content:center;
    gap:0rem;
  }}
  label {{
    width:90vw;
    color:#ddd;
    font-size:min(25vh, 25px); 
    margin-bottom:min(10vh, 10px);
    text-align:left;
  }}
  .container {{
    width:90vw;
    display:flex;
    flex-direction:column;
    gap:0;
  }}
  input[type="date"] {{
    width:100%;
    font-size:min(15vh, 15px);
    padding:min(7.5vh, 7.5px) min(10vh, 10px);
    border:1px solid var(--border);
    border-radius:min(7.5vh, 7.5px);
    background:var(--input-bg);
    color:var(--text);
    outline:none;
    cursor:pointer;
    transition:all 150ms ease;
    box-shadow:1px 1px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
    color-scheme: dark;
  }}
  input[type="date"]:hover {{
    background:var(--input-hover);
    border-color:#444;
    box-shadow:2px 2px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  input[type="date"]:focus {{
    border-color:var(--primary);
    background:var(--input-hover);
    box-shadow:0 0 0 2px var(--primary-dim), 1px 1px 0px 0px rgba(0,0,0,0.3);
  }}
  input[type="date"]::-webkit-calendar-picker-indicator {{
    filter: invert(1);
    cursor: pointer;
  }}
</style>

<label for="dt">{label}</label>
<div class="container">
  <input id="dt" type="date" />
</div>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  const PARAMETER = {json.dumps(parameter)};
  const SENDER  = {json.dumps(sender_id)};
  const VALUE   = {VALUE_JS};
  const MIN     = {MIN_JS};
  const MAX     = {MAX_JS};
  const AUTO    = {str(auto_send_on_load).lower()};
  
  const input = document.getElementById('dt');
  input.value = VALUE;
  input.min = MIN;
  input.max = MAX;
  
  function post(val) {{
    window.parent.postMessage({{
      type: 'param',
      parameter: PARAMETER,
      values: val,
      origin: SENDER,
      ts: Date.now()
    }}, '*');
  }}
  
  input.addEventListener('change', (e) => {{
    post(e.target.value);
  }});
  
  if (AUTO) {{
    queueMicrotask(() => post(VALUE));
  }}
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)


def sql_input(
    label: str = "SQL Query",
    *,
    value: str = "",
    placeholder: str = "SELECT * FROM table...",
    button_label: str = "Run Query",
    height: str = "min(40vh, 300px)",
    parameter: str = "channel_sql",
    sender_id: str = "sql_input_1",
    disabled: bool = False,
    return_html: bool = False,
):
    import json

    VALUE_JS = json.dumps(value)
    PLACEHOLDER_JS = json.dumps(placeholder)

    html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/theme/material-darker.min.css">

<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/sql/sql.min.js"></script>

<style>
  :root {{
    --primary: #e8ff59;
    --bg: #121212;
    --text: #eee;
    --text-muted: #999;
    --border: #333;
    --input-bg: #1b1b1b;
    --button-bg: #2a2a2a;
    --button-hover: #3a3a3a;
  }}
  * {{
    box-sizing: border-box;
  }}
  html, body {{
    height: 100vh;
    margin: 0;
    padding: 0;
    background: var(--bg);
    color: var(--text);
    font-family: system-ui, -apple-system, sans-serif;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 0;
  }}
  label {{
    width: 90vw;
    color: #ddd;
    font-size: min(25vh, 25px);
    margin-bottom: min(10vh, 10px);
    text-align: left;
  }}
  .container {{
    width: 90vw;
    display: flex;
    flex-direction: column;
    gap: min(10vh, 10px);
  }}
  .editor-wrapper {{
    width: 100%;
    border: 1px solid var(--border);
    border-radius: min(7.5vh, 7.5px);
    overflow: hidden;
    box-shadow:
      1px 1px 0px 0px rgba(0,0,0,0.3),
      0px 0px 2px 0px rgba(0,0,0,0.2);
    transition: all 150ms ease;
  }}
  .editor-wrapper:focus-within {{
    border-color: var(--primary);
    box-shadow:
      inset 0 0 0 2px rgba(232,255,89,0.65),
      0 0 0 2px rgba(232,255,89,0.15),
      1px 1px 0px 0px rgba(0,0,0,0.3);
  }}

  .CodeMirror {{
    height: {height};
    font-size: min(15vh, 15px);
    font-family: "Consolas","Monaco","Courier New",monospace;
    background: var(--input-bg);
    color: var(--text);
  }}
  .CodeMirror.cm-has-placeholder-init .CodeMirror-lines {{
    /* muted look before user types for placeholder mode */
    color: var(--text-muted);
    font-style: italic;
  }}

  .CodeMirror-cursor {{
    border-left: 2px solid var(--primary);
  }}
  .CodeMirror-selected {{
    background: rgba(232, 255, 89, 0.2);
  }}
  .CodeMirror-gutters {{
    background: var(--input-bg);
    border-right: 1px solid var(--border);
  }}
  .CodeMirror-linenumber {{
    color: var(--text-muted);
  }}

  button {{
    width: 100%;
    padding: min(7.5vh, 10px) min(10vh, 16px);
    border: 1px solid var(--border);
    border-radius: min(7.5vh, 7.5px);
    background: var(--button-bg);
    color: var(--text);
    font-size: min(15vh, 15px);
    font-weight: 500;
    cursor: pointer;
    transition: all 150ms ease;
    box-shadow:
      1px 1px 0px 0px rgba(0,0,0,0.3),
      0px 0px 2px 0px rgba(0,0,0,0.2);
    {"opacity: 0.6; cursor: not-allowed;" if disabled else ""}
  }}
  button:hover:not(:disabled) {{
    background: var(--button-hover);
    border-color: #444;
    box-shadow:
      2px 2px 0px 0px rgba(0,0,0,0.3),
      0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  button:active:not(:disabled) {{
    transform: translateY(1px);
    box-shadow:
      1px 1px 0px 0px rgba(0,0,0,0.3),
      0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  button:focus {{
    outline: none;
    box-shadow:
      0 0 0 2px rgba(232,255,89,0.1),
      1px 1px 0px 0px rgba(0,0,0,0.3);
  }}
</style>

<label for="sql-editor">{label}</label>
<div class="container">
  <div class="editor-wrapper">
    <!-- Note: textarea placeholder attr is now only cosmetic before CM mounts.
         After mount we control text ourselves. -->
    <textarea id="sql-editor" placeholder="{placeholder}"></textarea>
  </div>
  <button id="run-btn" {("disabled" if disabled else "")}>{button_label}</button>
</div>

<script>
document.addEventListener('DOMContentLoaded', () => {{
  const PARAMETER   = {json.dumps(parameter)};
  const SENDER      = {json.dumps(sender_id)};
  const VALUE       = {VALUE_JS};
  const PLACEHOLDER = {PLACEHOLDER_JS};

  const textarea = document.getElementById('sql-editor');
  const btn = document.getElementById('run-btn');

  // init CodeMirror
  const editor = CodeMirror.fromTextArea(textarea, {{
    mode: 'text/x-sql',
    theme: 'material-darker',
    lineNumbers: true,
    lineWrapping: true,
    indentUnit: 2,
    tabSize: 2,
    indentWithTabs: false,
    autofocus: true,
    extraKeys: {{
      'Ctrl-Enter': () => btn.click(),
      'Cmd-Enter': () => btn.click(),
    }}
  }});

  // If user passed a real value, use it.
  // Else, inject the placeholder text AS REAL CONTENT.
  if (VALUE && VALUE.trim() !== "") {{
    editor.setValue(VALUE);
  }} else {{
    editor.setValue(PLACEHOLDER);
    editor.getWrapperElement().classList.add('cm-has-placeholder-init');
  }}

  // As soon as they type something different, remove "placeholder style"
  editor.on('change', () => {{
    const wrap = editor.getWrapperElement();
    if (wrap.classList.contains('cm-has-placeholder-init')) {{
      // If user modified text away from the exact placeholder OR added newlines, etc.,
      // drop the muted styling forever.
      if (editor.getValue() !== PLACEHOLDER) {{
        wrap.classList.remove('cm-has-placeholder-init');
      }}
    }}
  }});

  function post() {{
    const query = editor.getValue().trim();
    if (!query) return;
    window.parent.postMessage({{
      type: 'param',
      parameter: PARAMETER,
      values: query,
      origin: SENDER,
      ts: Date.now()
    }}, '*');
  }}

  btn.addEventListener('click', () => {{
    if (!btn.disabled) {{
      post();
    }}
  }});

  // Refresh on initial layout + on resize so vh height stays sane
  const refresh = () => editor.refresh();
  const ro = new ResizeObserver(refresh);
  ro.observe(document.body);

  setTimeout(refresh, 100);
}});
</script>
"""
    return html if return_html else common.html_to_obj(html)
