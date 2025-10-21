import json
common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(channel: str = "channel_m"):
    html = iframe_receiver_html(
        channel=channel,
        base_url="https://unstable.udf.ai/fsh_5Q8mjn124whyR3tuAXzaKQ/run?dtype_out_vector=html",
        mapping={"value": "dataset"},  # from dropdown
    )
    return common.html_to_obj(html)


def iframe_receiver_html(
    channel: str,
    base_url: str,
    mapping: dict | None = None,
    show_url: bool = True,
    height: str = "100vh",
) -> str:
    """
    Reusable helper to render a 'receiver' iframe that listens for fused-channel messages
    and loads a constructed URL.

    Args:
        channel: Broadcast channel name (e.g. 'channel_1')
        base_url: Base URL to load, e.g. a fused UDF run endpoint
        mapping: Dict mapping incoming vars to query params, e.g. {'lat':'center_lat'}
        show_url: Whether to display constructed URL text above the iframe
        height: CSS height for iframe (default 100vh)
    """
    import json
    mapping = mapping or {}
    show_bar = "block" if show_url else "none"

    return f"""<!doctype html>
<meta charset="utf-8">
<title>Receiver</title>
<style>
  body {{ margin:0; font:14px system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial; background:#fff; color:#111 }}
  #bar {{ display:{show_bar}; padding:10px 12px; border-bottom:1px solid #eee; background:#fafafa }}
  #chan {{ color:#475569 }}
  #url a {{ color:inherit; text-decoration:none }}
  iframe {{ width:100%; height:{height}; border:0; display:block }}
  #loading {{
    position:fixed; top:50%; left:50%; transform:translate(-50%,-50%);
    background:rgba(255,255,255,0.9); padding:10px 20px; border-radius:6px;
    border:1px solid #ddd; font-weight:500; font-size:14px; color:#333;
    display:none; z-index:10;
  }}
</style>

<div id="bar">
  <div>Channel: <code id="chan"></code></div>
  <div>Constructed URL: <span id="url">(waiting…)</span></div>
</div>
<div id="loading">Loading…</div>
<iframe id="viewer"></iframe>

<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@main/channel.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const CHANNEL = {json.dumps(channel)};
  const BASE = {json.dumps(base_url)};
  const MAP  = {json.dumps(mapping)};
  const chanEl = document.getElementById('chan');
  const urlEl  = document.getElementById('url');
  const frame  = document.getElementById('viewer');
  const loading = document.getElementById('loading');
  chanEl.textContent = CHANNEL;

  const isScalar = v => (['string','number','boolean'].includes(typeof v));

  function pickVars(msg) {{
    if (!msg || !msg.payload) return {{}};
    if (msg.payload.vars && typeof msg.payload.vars === 'object') return msg.payload.vars;
    return msg.payload;
  }}

  function filterAndRename(obj) {{
    const out = {{}};
    for (const [srcKey, dstKey] of Object.entries(MAP)) {{
      if (obj[srcKey] != null) out[dstKey] = obj[srcKey];
    }}
    return out;
  }}

  function buildUrl(params) {{
    const u = new URL(BASE);
    for (const [k,v] of Object.entries(params || {{}})) {{
      if (isScalar(v)) u.searchParams.set(k, v);
    }}
    return u.toString();
  }}

  function render(url) {{
    const a = document.createElement('a');
    a.href = url; a.target = '_blank'; a.rel = 'noreferrer';
    a.textContent = url;
    urlEl.replaceChildren(a);
    loading.style.display = 'block';
    frame.src = url;
  }}

  frame.addEventListener('load', () => {{
    loading.style.display = 'none';
  }});

  enableMsgListener(CHANNEL, (msg) => {{
    const raw = pickVars(msg);
    const mapped = filterAndRename(raw);
    if (Object.keys(mapped).length === 0) return;
    const url = buildUrl(mapped);
    render(url);
  }});
}});
</script>
"""
