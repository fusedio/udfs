import fused
from jinja2 import Template

@fused.udf(cache_max_age=0)
def udf(
    # Fused tiles (points)
    token: str = "UDF_Airports_visualizer", # using community udf. Use fsh_*** style token for private UDF otherwise
    host: str = "https://www.fused.io",
    source_layer: str = "udf",

    # View
    center_lng: float = 12.550343, # hardcoded but could be dynamically computed based on the data
    center_lat: float = 55.665957,
    zoom: float = 6.0,
    minzoom: int = 0,
    maxzoom: int = 14,

    # Circle styling
    circle_opacity: float = 0.9,
    circle_radius_min: float = 2.0,
    circle_radius_max: float = 8.0,

    # Dark basemap (Carto Dark Matter raster tiles)
    dark_tiles: list = None,
    basemap_attribution: str = "© OpenStreetMap contributors, © CARTO"
):
    """
    MapLibre + dark basemap + Fused MVT points

    - Basemap: Carto Dark Matter raster tiles (no key).
    - Data: auto-fetched MVT via XYZ from your Fused token (MapLibre can't read Parquet directly).
      {host}/server/v1/realtime-shared/{token}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt
    """
    if dark_tiles is None:
        dark_tiles = [
            "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
            "https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
            "https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        ]

    html = Template(r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>MapLibre • Dark Basemap + Fused Points</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.css"/>
  <script src="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.js"></script>
  <style>html,body,#map{height:100%;margin:0;background:#000}</style>
</head>
<body>
  <div id="map"></div>
  <script>
    const HOST         = {{ host | tojson }};
    const TOKEN        = {{ token | tojson }};
    const SOURCE_LAYER = {{ source_layer | tojson }};
    const CENTER       = [{{ center_lng }}, {{ center_lat }}];
    const ZOOM         = {{ zoom }};
    const MINZOOM      = {{ minzoom }};
    const MAXZOOM      = {{ maxzoom }};
    const RMIN         = {{ circle_radius_min }};
    const RMAX         = {{ circle_radius_max }};
    const OPAC         = {{ circle_opacity }};
    const DARK_TILES   = {{ dark_tiles | tojson }};
    const ATTRIB       = {{ basemap_attribution | tojson }};

    const tilesURL = `${HOST.replace(/\/+$/,'')}/server/v1/realtime-shared/${TOKEN}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt`;

    // Inline dark style using raster tiles
    const DARK_STYLE = {
      version: 8,
      sources: {
        "carto-dark": {
          type: "raster",
          tiles: DARK_TILES,
          tileSize: 256,
          attribution: ATTRIB,
          maxzoom: 19
        }
      },
      layers: [
        { id: "carto-dark", type: "raster", source: "carto-dark" }
      ]
    };

    const map = new maplibregl.Map({
      container: 'map',
      style: DARK_STYLE,
      center: CENTER,
      zoom: ZOOM
    });

    map.addControl(new maplibregl.NavigationControl({showCompass:true, showZoom:true}));

    map.on('load', () => {
      // Fused MVT source (auto-fetch XYZ)
      map.addSource('fused', {
        type: 'vector',
        tiles: [tilesURL],
        minzoom: MINZOOM,
        maxzoom: MAXZOOM
      });

      // Points as circles (filter to Point geometry)
      map.addLayer({
        id: 'fused-points',
        type: 'circle',
        source: 'fused',
        'source-layer': SOURCE_LAYER,
        filter: ['==', ['geometry-type'], 'Point'],
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 0, RMIN, 14, RMAX],
          'circle-color': [
            'case',
            ['has', 'stats'],
            ['interpolate', ['linear'], ['coalesce', ['get','stats'], 0],
              0,  '#88C0D0',
              50, '#A3BE8C',
              100,'#EBCB8B',
              200,'#D08770'
            ],
            '#35AF6D'
          ],
          'circle-opacity': OPAC,
          'circle-stroke-width': 0.5,
          'circle-stroke-color': '#0b0b0b'
        }
      });

      // Click popup
      map.on('click', 'fused-points', e => {
        const f = e.features && e.features[0];
        if (!f) return;
        const html = '<pre style="margin:0;white-space:pre-wrap;">' +
                     JSON.stringify(f.properties || {}, null, 2) + '</pre>';
        new maplibregl.Popup({closeButton:true})
          .setLngLat(e.lngLat)
          .setHTML(html)
          .addTo(map);
      });

      map.on('mouseenter', 'fused-points', () => map.getCanvas().style.cursor = 'pointer');
      map.on('mouseleave', 'fused-points', () => map.getCanvas().style.cursor = '');
    });
  </script>
</body>
</html>
""").render(
        token=token,
        host=host,
        source_layer=source_layer,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        minzoom=minzoom,
        maxzoom=maxzoom,
        circle_radius_min=circle_radius_min,
        circle_radius_max=circle_radius_max,
        circle_opacity=circle_opacity,
        dark_tiles=dark_tiles,
        basemap_attribution=basemap_attribution,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
