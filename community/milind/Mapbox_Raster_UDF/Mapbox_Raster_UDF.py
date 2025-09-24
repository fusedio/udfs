@fused.udf(cache_max_age=0)
def udf(
    token: str = "UDF_CDLs_Tile_Example",      
    host: str = "https://www.fused.io",    # or https://www.fused.io
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -121.16450354933122,
    center_lat: float = 38.44272969483187,
    zoom: float = 8.59,
    minzoom: int = 6,
    maxzoom: int = 14,
    layer_id_raster: str = "fused-raster-layer",
    raster_tile_size: int = 256,
    raster_opacity: float = 0.95
):
    from jinja2 import Template

    """
    Mapbox map that loads **raster** XYZ tiles from a Fused UDF token.

    Tiles URL:
      {host}/server/v1/realtime-shared/{token}/run/tiles/{z}/{x}/{y}?dtype_out_raster=png
    """
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Raster XYZ Loader</title>
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
    const LID_RAS      = {{ layer_id_raster | tojson }};
    const R_TILE_SIZE  = {{ raster_tile_size }};
    const R_OPACITY    = {{ raster_opacity }};

    function buildRaster(host, tok){
      return host.replace(/\/+$/,'') + `/server/v1/realtime-shared/${tok}/run/tiles/{z}/{x}/{y}?dtype_out_raster=png`;
    }

    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({
      container: 'map',
      style: STYLE_URL,
      center: CENTER,
      zoom: ZOOM
    });

    function addRaster(url){
      if (map.getLayer(LID_RAS)) map.removeLayer(LID_RAS);
      if (map.getSource('xyz'))  map.removeSource('xyz');

      map.addSource('xyz', {
        type: 'raster',
        tiles: [url],
        tileSize: R_TILE_SIZE,
        minzoom: MINZOOM,
        maxzoom: MAXZOOM
      });

      map.addLayer({
        id: LID_RAS,
        type: 'raster',
        source: 'xyz',
        paint: {
          'raster-opacity': R_OPACITY,
          'raster-fade-duration': 0
        }
      });
    }

    map.on('load', () => {
      const url = buildRaster(HOST, TOKEN);
      addRaster(url);
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
        layer_id_raster=layer_id_raster,
        raster_tile_size=raster_tile_size,
        raster_opacity=raster_opacity,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
