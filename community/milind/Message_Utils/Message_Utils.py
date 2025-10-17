import json
common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf
def udf(channel: str = "channel_1", sender_id: str = "my_udf"):
    # html = dropdown(channel, sender_id)
    L = fused.api.list('s3://fused-sample/')
    html = map_draw_html(
        channel="channel_1",
        sender_id="draw_1"
    )
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
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@4f1bd37/channel.js"></script>

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
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@4f1bd37/channel.js"></script>
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
    default_value: str = None,
    label: str = "Select an option:",
    return_html: bool = False
) -> str:
    def truncate_path(path, max_len=35):
        if len(path) <= max_len:
            return path
        return path[:15] + '...' + path[-(max_len-13):]
    
    if options and isinstance(options[0], dict):
        option_tags = "\n".join([
            f'<option value="{opt["value"]}" data-type="{opt["value"].split(".")[-1]}" {"selected" if default_value == opt["value"] else ""}>{truncate_path(opt["label"])}</option>'
            for opt in options
        ])
    else:
        option_tags = "\n".join([
            f'<option value="{opt}" data-type="{opt.split(".")[-1]}" {"selected" if default_value == opt else ""}>{truncate_path(opt)}</option>'
            for opt in options
        ])

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
}}
select {{ 
    font-size: 1rem; 
    padding: 0.5rem; 
    min-width: 200px; 
    border: 2px solid #444;
    border-radius: 4px;
    background: #2a2a2a;
    color: #ffffff;
}}
.toggle {{
    display: flex;
    gap: 1rem;
    margin-bottom: 1rem;
}}
.toggle label {{
    cursor: pointer;
}}
</style>
<div class="card">
  <div class="toggle">
    <label><input type="radio" name="fileType" value="csv" checked> CSV</label>
    <label><input type="radio" name="fileType" value="parquet"> Parquet</label>
  </div>
  <label for="dropdown" style="display:block;margin-bottom:0.5rem;font-weight:500;">{label}</label>
  <select id="dropdown">
    {option_tags}
  </select>
</div>
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@4f1bd37/channel.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const dropdown = document.getElementById('dropdown');
  const allOptions = Array.from(dropdown.options);
  const radios = document.querySelectorAll('input[name="fileType"]');
  
  function filterOptions(type) {{
    dropdown.innerHTML = '';
    allOptions.forEach(opt => {{
      if (opt.dataset.type === type) {{
        dropdown.appendChild(opt.cloneNode(true));
      }}
    }});
  }}
  
  radios.forEach(radio => {{
    radio.addEventListener('change', (e) => {{
      filterOptions(e.target.value);
    }});
  }});
  
  filterOptions('csv');
  enableDropdownMessaging(dropdown, {json.dumps(channel)}, {json.dumps(sender_id)});
}});
</script>
"""
    if return_html:
        return html
    else:
        return common.html_to_obj(html)


def map_draw_html(
    channel: str = "channel_1",
    sender_id: str = "draw_1",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -74.0,
    center_lat: float = 40.7,
    zoom: float = 12.0,
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    include_bounds: bool = True
) -> str:
    """
    Publishes messages on `channel` with:
      type: 'shape'
      payload: { geojson: <FeatureCollection>, bounds?: [w,s,e,n], zoom?: number }
    Uses your channel.js's enableMessaging for consistent behavior.
    """
    return f"""<!doctype html>
<meta charset="utf-8">
<link href="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.css" rel="stylesheet">
<script src="https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.js"></script>
<link rel="stylesheet" href="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.5.0/mapbox-gl-draw.css">
<script src="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-draw/v1.5.0/mapbox-gl-draw.js"></script>
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@4f1bd37/channel.js"></script>

<style>
  html, body, #map {{ margin: 0; height: 100% }}
  #map {{ position: fixed; inset: 0 }}
</style>

<div id="map"></div>

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
    controls: {{ polygon: true, point: true, line_string: true, trash: true }},
    defaultMode: 'draw_polygon'
  }});
  map.addControl(draw, 'bottom-left');

  // Debounced handler via rAF to avoid spam on rapid edits
  let ticking = false;
  const debounced = (fn) => {{
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(() => {{ ticking = false; fn(); }});
  }};

  // We wire through your enableMessaging helper so all senders behave consistently
  const getPayload = () => {{
    const payload = {{ geojson: draw.getAll() }};
    {"const b = map.getBounds(); payload.bounds = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; payload.zoom = map.getZoom();" if include_bounds else ""}
    return payload;
  }};

  const on  = (m, h) => {{
    const start = () => {{
      // initial emit so receivers can react even before any drawing
      h();
      // hook into mapbox-draw events (they are emitted on the map)
      ['draw.create','draw.update','draw.delete','draw.combine','draw.uncombine'].forEach(ev => {{
        m.on(ev, () => debounced(h));
      }});
    }};
    (m.loaded && m.loaded()) ? start() : m.once('load', start);
  }};
  const off = (m, h) => {{
    ['draw.create','draw.update','draw.delete','draw.combine','draw.uncombine'].forEach(ev => m.off(ev, h));
  }};

  // This will publish {{ type: 'shape', payload: getPayload() }} on the channel
  enableMessaging({{
    source: map,
    channel: {json.dumps(channel)},
    sender: {json.dumps(sender_id)},
    type: 'shape',
    on,
    off,
    getPayload
  }});
}});
</script>
"""
