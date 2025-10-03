@fused.udf(cache_max_age=0)
def udf(
    token: str = "UDF_Overture_Maps_Example",
    host: str = "https://www.fused.io",  # change to https://www.fused.io if needed
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -121.16450354933122,
    center_lat: float = 38.44272969483187,
    zoom: float = 8.59,
    minzoom: int = 6,
    maxzoom: int = 14,
    layer_id: str = "fused-vector-layer",
    source_layer: str = "udf"
):

    from jinja2 import Template

    """
    Minimal Mapbox map (no input UI) that loads Fused XYZ vector tiles (MVT) from `token`.
    Tiles URL: {host}/server/v1/realtime-shared/{token}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt
    """
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Simple XYZ (MVT) Loader</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <style>
    html, body { margin:0; height:100%; }
    #map { position:absolute; inset:0; }
  </style>
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

    function tilesFromToken(tok) {
      return `${HOST}/server/v1/realtime-shared/${tok}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt`;
    }

    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({
      container: 'map',
      style: STYLE_URL,
      center: CENTER,
      zoom: ZOOM
    });

    function addVectorTiles(tok) {
      const tilesUrl = tilesFromToken(tok);
      if (map.getLayer(LAYER_ID)) map.removeLayer(LAYER_ID);
      if (map.getSource('xyz'))   map.removeSource('xyz');

      map.addSource('xyz', {
        type: 'vector',
        tiles: [tilesUrl],
        minzoom: MINZOOM,
        maxzoom: MAXZOOM
      });

      map.addLayer({
        id: LAYER_ID,
        type: 'line',
        source: 'xyz',
        'source-layer': SOURCE_LAYER,
        layout: { 'line-join': 'round', 'line-cap': 'round' },
        paint: { 'line-color': '#35AF6D', 'line-width': 2, 'line-opacity': 0.8 }
      });
    }

    map.on('load', () => {
      addVectorTiles(TOKEN);
    });
  </script>
</body>
</html>
""").render(
        token=token,
        host=host,
        mapbox_token=mapbox_token,
        style_url=style_url,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        minzoom=minzoom,
        maxzoom=maxzoom,
        layer_id=layer_id,
        source_layer=source_layer,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
