@fused.udf(cache_max_age=0)
def udf(
    # Fused vector tiles (MVT)
    token: str = "UDF_Overture_Maps_Example",
    host: str = "https://unstable.fused.io",   # or https://www.fused.io
    source_layer: str = "udf",

    # MapLibre basemap + view
    style_url: str = "https://demotiles.maplibre.org/style.json",
    center_lng: float = -121.16450354933122,
    center_lat: float = 38.44272969483187,
    zoom: float = 2,
    minzoom: int = 6,
    maxzoom: int = 14,

    # Simple styling
    layer_id: str = "fused-vector-layer",
    fill_color: str = "#35AF6D",
    fill_opacity: float = 0.35,
):
    from jinja2 import Template

    """
    Minimal MapLibre map (no UI) that loads Fused XYZ vector tiles (MVT) from `token`.

    Tiles URL:
      {host}/server/v1/realtime-shared/{token}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt
    """
    html = Template(r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Simple XYZ (MVT) Loader — MapLibre</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.css"/>
  <script src="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.js"></script>
  <style>
    html, body { margin:0; height:100%; }
    #map { position:absolute; inset:0; }
  </style>
</head>
<body>
  <div id="map"></div>

  <script>
    const STYLE_URL    = {{ style_url    | tojson }};
    const HOST         = {{ host         | tojson }};
    const TOKEN        = {{ token        | tojson }};
    const SOURCE_LAYER = {{ source_layer | tojson }};
    const LAYER_ID     = {{ layer_id     | tojson }};
    const CENTER       = [{{ center_lng }}, {{ center_lat }}];
    const ZOOM         = {{ zoom }};
    const MINZOOM      = {{ minzoom }};
    const MAXZOOM      = {{ maxzoom }};
    const FILL_COLOR   = {{ fill_color   | tojson }};
    const FILL_OPACITY = {{ fill_opacity }};

    function tilesFromToken(tok) {
      return `${HOST.replace(/\/+$/,'')}/server/v1/realtime-shared/${tok}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt`;
    }

    const map = new maplibregl.Map({
      container: 'map',
      style: STYLE_URL,
      center: CENTER,
      zoom: ZOOM
    });

    map.addControl(new maplibregl.NavigationControl({showZoom:true, showCompass:true}));

    map.on('load', () => {
      const tilesUrl = tilesFromToken(TOKEN);

      // Source
      map.addSource('fused_xyz', {
        type: 'vector',
        tiles: [tilesUrl],
        minzoom: MINZOOM,
        maxzoom: MAXZOOM
      });

      // Fill layer (simple)
      map.addLayer({
        id: LAYER_ID,
        type: 'fill',
        source: 'fused_xyz',
        'source-layer': SOURCE_LAYER,
        paint: {
          'fill-color': FILL_COLOR,
          'fill-opacity': FILL_OPACITY
        }
      });

      // Optional thin outline for visibility
      map.addLayer({
        id: LAYER_ID + '-outline',
        type: 'line',
        source: 'fused_xyz',
        'source-layer': SOURCE_LAYER,
        paint: {
          'line-color': '#0b0b0b',
          'line-width': 0.5,
          'line-opacity': 0.6
        }
      });
    });
  </script>
</body>
</html>
""").render(
        token=token,
        host=host,
        source_layer=source_layer,
        style_url=style_url,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        minzoom=minzoom,
        maxzoom=maxzoom,
        layer_id=layer_id,
        fill_color=fill_color,
        fill_opacity=fill_opacity,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
