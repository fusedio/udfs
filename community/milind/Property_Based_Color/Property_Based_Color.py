@fused.udf(cache_max_age=0)
def udf(
    token: str = "UDF_DSM_Zonal_Stats",
    host: str = "https://www.fused.io",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -122.4178,
    center_lat: float = 37.7449,
    zoom: float = 17,
    minzoom: int = 0, 
    maxzoom: int = 15,
    layer_id: str = "vector-choro",
    source_layer: str = "udf",
    value_attr: str = "stats",       # attribute to color by
    domain_min: float = 0.0,
    domain_mid: float = 50.0,
    domain_max: float = 100.0,
    color_min: str = "#2b65a0",
    color_mid: str = "#35af6d",
    color_max: str = "#e8ff59",
    fill_opacity: float = 0.65,
    outline_color: str = "#111111",
):
    from jinja2 import Template
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Vector Fill - Property Based</title>
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
    const ATTR         = {{ value_attr | tojson }};
    const DMIN         = {{ domain_min }};
    const DMID         = {{ domain_mid }};
    const DMAX         = {{ domain_max }};
    const CMIN         = {{ color_min | tojson }};
    const CMID         = {{ color_mid | tojson }};
    const CMAX         = {{ color_max | tojson }};
    const FILL_OPAC    = {{ fill_opacity }};
    const OUTLINE_COL  = {{ outline_color | tojson }};

    function tilesFromToken(tok) {
      return `${HOST.replace(/\/+$/,'')}/server/v1/realtime-shared/${tok}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt`;
    }

    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ container:'map', style:STYLE_URL, center:CENTER, zoom:ZOOM });

    function valueExpr() {
      return ['to-number', ['get', ATTR]];
    }

    map.on('load', () => {
      map.addSource('xyz', { type:'vector', tiles:[tilesFromToken(TOKEN)], minzoom:MINZOOM, maxzoom:MAXZOOM });

      map.addLayer({
        id: LAYER_ID,
        type: 'fill',
        source: 'xyz',
        'source-layer': SOURCE_LAYER,
        filter: ['==', ['geometry-type'], 'Polygon'],
        paint: {
          'fill-color': [
            'interpolate', ['linear'], ['coalesce', valueExpr(), DMIN],
            DMIN, CMIN,
            DMID, CMID,
            DMAX, CMAX
          ],
          'fill-opacity': FILL_OPAC
        }
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
        minzoom=minzoom, maxzoom=maxzoom, layer_id=layer_id, source_layer=source_layer,
        value_attr=value_attr, domain_min=domain_min, domain_mid=domain_mid, domain_max=domain_max,
        color_min=color_min, color_mid=color_mid, color_max=color_max,
        fill_opacity=fill_opacity, outline_color=outline_color,
    )
    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)