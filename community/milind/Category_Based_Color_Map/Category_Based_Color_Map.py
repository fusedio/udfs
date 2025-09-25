@fused.udf(cache_max_age=0)
def udf(
    token: str = "UDF_FEMA_Buildings_US",
    host: str = "https://unstable.fused.io",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -74.0110,   # longitude
    center_lat: float = 40.7133,  
    zoom: float = 14,
    minzoom: int = 0,
    maxzoom: int = 15,
    source_layer: str = "udf",
    layer_id: str = "fema-categories",
    attr_name: str = "OCC_CLS",
    # Category domain (from legacy JSON)
    domain: list = None,
    # CARTOColors Bold palette (7 colors)
    colors: list = None,
    fill_opacity: float = 0.65,
    outline_color: str = "#0b0b0b"
):
    from jinja2 import Template

    if domain is None:
        domain = [
            "Assembly",
            "Commercial",
            "Utility and Misc",
            "Residential",
            "Industrial",
            "Education",
            "Government",
        ]
    if colors is None:
        colors = ["#7F3C8D", "#11A579", "#3969AC", "#F2B701", "#E73F74", "#80BA5A", "#E68310"]

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Category Colors (Mapbox GL)</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <style>html,body{margin:0;height:100%}#map{position:absolute;inset:0}</style>
</head>
<body>
  <div id="map"></div>
  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const STYLE_URL    = {{ style_url    | tojson }};
    const HOST         = {{ host         | tojson }};
    const TOKEN        = {{ token        | tojson }};
    const CENTER       = [{{ center_lng }}, {{ center_lat }}];
    const ZOOM         = {{ zoom }};
    const MINZOOM      = {{ minzoom }};
    const MAXZOOM      = {{ maxzoom }};
    const SOURCE_LAYER = {{ source_layer | tojson }};
    const LAYER_ID     = {{ layer_id     | tojson }};
    const ATTR_NAME    = {{ attr_name    | tojson }};
    const DOMAIN       = {{ domain       | tojson }};
    const COLORS       = {{ colors       | tojson }};
    const FILL_OPAC    = {{ fill_opacity }};
    const OUTLINE_COL  = {{ outline_color | tojson }};

    function tilesFromToken(h, tok) {
      return h.replace(/\/+$/,'') + `/server/v1/realtime-shared/${tok}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt`;
    }

    function matchByCategory(attr, domain, colors, fallbackColor) {
      // ['match', ['get', attr], domain[0], colors[0], domain[1], colors[1], ..., fallback]
      const expr = ['match', ['get', attr]];
      for (let i = 0; i < Math.min(domain.length, colors.length); i++) {
        expr.push(domain[i], colors[i]);
      }
      expr.push(fallbackColor);
      return expr;
    }

    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({
      container: 'map',
      style: STYLE_URL,
      center: CENTER,
      zoom: ZOOM
    });

    map.on('load', () => {
      // Vector MVT from the FEMA UDF token
      map.addSource('fema', {
        type: 'vector',
        tiles: [tilesFromToken(HOST, TOKEN)],
        minzoom: MINZOOM,
        maxzoom: MAXZOOM
      });

      // Polygons fill by category
      map.addLayer({
        id: LAYER_ID,
        type: 'fill',
        source: 'fema',
        'source-layer': SOURCE_LAYER,
        filter: ['==', ['geometry-type'], 'Polygon'],
        paint: {
          'fill-color': matchByCategory(ATTR_NAME, DOMAIN, COLORS, '#BBBBBB'),
          'fill-opacity': FILL_OPAC
        }
      });

      // Thin polygon outline
      map.addLayer({
        id: LAYER_ID + '-outline',
        type: 'line',
        source: 'fema',
        'source-layer': SOURCE_LAYER,
        filter: ['==', ['geometry-type'], 'Polygon'],
        paint: { 'line-color': OUTLINE_COL, 'line-width': 0.5, 'line-opacity': 0.9 }
      });

      // Optional: hover cursor over polygons
      map.on('mouseenter', LAYER_ID, () => map.getCanvas().style.cursor = 'pointer');
      map.on('mouseleave', LAYER_ID, () => map.getCanvas().style.cursor = '');
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
        source_layer=source_layer,
        layer_id=layer_id,
        attr_name=attr_name,
        domain=domain,
        colors=colors,
        fill_opacity=fill_opacity,
        outline_color=outline_color,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)