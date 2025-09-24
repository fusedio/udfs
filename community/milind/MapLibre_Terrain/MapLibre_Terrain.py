import fused
from jinja2 import Template

@fused.udf(cache_max_age=0)
def udf(
    # Camera (defaults from your example; Innsbruck area)
    center_lng: float = 11.39085,
    center_lat: float = 47.27574,
    zoom: float = 12,
    pitch: float = 70,
    bearing: float = 0,

    # Sources
    osm_tiles: str = "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
    osm_maxzoom: int = 19,
    terrain_tiles: str = "https://demotiles.maplibre.org/terrain-tiles/tiles.json",
    hillshade_tiles: str = "https://demotiles.maplibre.org/terrain-tiles/tiles.json",

    # Terrain
    exaggeration: float = 1.0,

    # Map limits
    max_zoom: int = 18,
    max_pitch: int = 85,
):
    """
    MapLibre GL JS 3D terrain UDF.
    - OSM raster basemap
    - Raster DEM for terrain + hillshade
    - Navigation & Terrain controls
    """
    html = Template(r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>3D Terrain (MapLibre)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.css"/>
  <script src="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.js"></script>
  <style>
    html, body { height: 100%; margin: 0; padding: 0; }
    #map { height: 100%; }
  </style>
</head>
<body>
  <div id="map"></div>
  <script>
    const map = new maplibregl.Map({
      container: 'map',
      zoom: {{ zoom }},
      center: [{{ center_lng }}, {{ center_lat }}],
      pitch: {{ pitch }},
      bearing: {{ bearing }},
      hash: true,
      style: {
        "version": 8,
        "sources": {
          "osm": {
            "type": "raster",
            "tiles": [{{ osm_tiles | tojson }}],
            "tileSize": 256,
            "attribution": "© OpenStreetMap Contributors",
            "maxzoom": {{ osm_maxzoom }}
          },
          "terrainSource": {
            "type": "raster-dem",
            "url": {{ terrain_tiles | tojson }},
            "tileSize": 256
          },
          "hillshadeSource": {
            "type": "raster-dem",
            "url": {{ hillshade_tiles | tojson }},
            "tileSize": 256
          }
        },
        "layers": [
          { "id": "osm", "type": "raster", "source": "osm" },
          {
            "id": "hills",
            "type": "hillshade",
            "source": "hillshadeSource",
            "layout": { "visibility": "visible" },
            "paint": { "hillshade-shadow-color": "#473B24" }
          }
        ],
        "terrain": {
          "source": "terrainSource",
          "exaggeration": {{ exaggeration }}
        },
        "sky": {}
      },
      maxZoom: {{ max_zoom }},
      maxPitch: {{ max_pitch }}
    });

    map.addControl(new maplibregl.NavigationControl({
      visualizePitch: true,
      showZoom: true,
      showCompass: true
    }));

    map.addControl(new maplibregl.TerrainControl({
      source: 'terrainSource',
      exaggeration: {{ exaggeration }}
    }));
  </script>
</body>
</html>
""").render(
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        pitch=pitch,
        bearing=bearing,
        osm_tiles=osm_tiles,
        osm_maxzoom=osm_maxzoom,
        terrain_tiles=terrain_tiles,
        hillshade_tiles=hillshade_tiles,
        exaggeration=exaggeration,
        max_zoom=max_zoom,
        max_pitch=max_pitch,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
