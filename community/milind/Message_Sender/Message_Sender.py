import json, fused

@fused.udf
def udf(channel: str = "bounds-demo", sender_id: str = "my_udf"):
    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    html = map_html(channel, sender_id)
    return common.html_to_obj(html)


def map_html(
    channel: str = "bounds-demo",
    sender_id: str = "my_udf",
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
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@48b6d9a/channel.js"></script>


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


def button_html(channel: str = "button-demo", sender_id: str = "button-udf") -> str:
    return f"""<!doctype html>
<meta charset="utf-8">
<button id="btn" style="font-size:1.2rem;padding:0.6rem 1rem;margin:2rem;">Click me</button>
<script src="https://cdn.jsdelivr.net/gh/milind-soni/fused-channel@48b6d9a/channel.js"></script>
<script>
document.addEventListener('DOMContentLoaded', () => {{
  const btn = document.getElementById('btn');
  enableButtonMessaging(btn, {json.dumps(channel)}, {json.dumps(sender_id)}, {{ label: 'wowow' }}, 'click');
}});
</script>
"""
