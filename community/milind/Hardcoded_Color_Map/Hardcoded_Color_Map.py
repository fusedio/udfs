@fused.udf(cache_max_age=0)
def udf(
    token: str = "UDF_Overture_Maps_Example",
    host: str = "https://www.fused.io",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -122.4194,
    center_lat: float = 37.7749,
    zoom: float = 16,
    minzoom: int = 0,
    maxzoom: int = 15,
    layer_id: str = "vector-fill",
    source_layer: str = "udf",
    fill_color: str = "#35AF6D",
    fill_opacity: float = 0.55,
    outline_color: str = "#0b0b0b",
):
    from jinja2 import Template
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Vector Fill - Hardcoded Color</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=false"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <style>html,body{margin:0;height:100%}#map{position:absolute;inset:0}</style>
</head>
<body>
  <div id="map"></div>
  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const STYLE_URL    = {{ style_url | tojson }};
    const HOST         = {{ host | tojson }};
    const TOKEN        = {{ token | tojson }};
    const CENTER       = [{{ center_lng }}, {{ center_lat }}];
    const ZOOM         = {{ zoom }};
    const MINZOOM      = {{ minzoom }};
    const MAXZOOM      = {{ maxzoom }};
    const LAYER_ID     = {{ layer_id | tojson }};
    const SOURCE_LAYER = {{ source_layer | tojson }};
    const FILL_COLOR   = {{ fill_color | tojson }};
    const FILL_OPAC    = {{ fill_opacity }};
    const OUTLINE_COL  = {{ outline_color | tojson }};

    function tilesFromToken(tok) {
      return `${HOST.replace(/\/+$/,'')}/server/v1/realtime-shared/${tok}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt`;
    }

    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ container:'map', style:STYLE_URL, center:CENTER, zoom:ZOOM });

    map.on('load', () => {
      map.addSource('xyz', { type:'vector', tiles:[tilesFromToken(TOKEN)], minzoom:MINZOOM, maxzoom:MAXZOOM });

      map.addLayer({
        id: LAYER_ID,
        type: 'fill',
        source: 'xyz',
        'source-layer': SOURCE_LAYER,
        filter: ['==', ['geometry-type'], 'Polygon'],
        paint: { 'fill-color': FILL_COLOR, 'fill-opacity': FILL_OPAC }
      });

      map.addLayer({
        id: LAYER_ID + '-outline',
        type: 'line',
        source: 'xyz',
        'source-layer': SOURCE_LAYER,
        filter: ['==', ['geometry-type'], 'Polygon'],
        paint: { 'line-color': OUTLINE_COL, 'line-width': 0.5, 'line-opacity': 0.9 }
      });
    });
  </script>
</body>
</html>
""").render(
        token=token, host=host, mapbox_token=mapbox_token, style_url=style_url,
        center_lng=center_lng, center_lat=center_lat, zoom=zoom,
        minzoom=minzoom, maxzoom=maxzoom, layer_id=layer_id,
        source_layer=source_layer, fill_color=fill_color, fill_opacity=fill_opacity,
        outline_color=outline_color,
    )
    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)