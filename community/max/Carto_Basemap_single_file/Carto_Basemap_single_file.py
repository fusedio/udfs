import fused
from jinja2 import Template
import json
import pandas as pd
import geopandas as gpd

@fused.udf(cache_max_age=0)
def udf(
    # View – these defaults will be overwritten by the computed center below
    center_lng: float = 0.0,
    center_lat: float = 0.0,
    zoom: float = 12,
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
    MapLibre + dark basemap + points loaded via @blue_moth token (single parquet read).
    - Data: fetched with fused.run() using the shared token.
    - Center/zoom: automatically calculated from the data bounds.
    """
    if dark_tiles is None:
        dark_tiles = [
            "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
            "https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
            "https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        ]

    # ----------------------------------------------------------------------
    # 1️⃣ Load data via @airbnb_listing token (single parquet read)
    # ----------------------------------------------------------------------
    # The shared token for the @airbnb_listing UDF
    data = fused.run("fsh_2W2nFDuy4C2YqEXz5SCx3c")

    # The @blue_moth UDF may return a GeoDataFrame or a plain DataFrame.
    # Ensure we end up with a GeoDataFrame named `gdf`.
    if isinstance(data, gpd.GeoDataFrame):
        gdf = data
    elif isinstance(data, pd.DataFrame):
        # Attempt to construct geometry from typical lat/lon column names
        if "latitude" in data.columns and "longitude" in data.columns:
            gdf = gpd.GeoDataFrame(
                data,
                geometry=gpd.points_from_xy(data["longitude"], data["latitude"]),
                crs="EPSG:4326",
            )
        else:
            raise RuntimeError(
                "Unable to locate latitude/longitude columns in the data returned by @blue_moth."
            )
    else:
        raise RuntimeError(
            "Unexpected data type returned by @blue_moth token. Expected DataFrame or GeoDataFrame."
        )

    # Ensure we are in WGS84 for the map
    gdf = gdf.to_crs("EPSG:4326")
    print(gdf.T)  # Debug: view schema / sample rows

    # ----------------------------------------------------------------------
    # 2️⃣ Compute map centre from data bounds
    # ----------------------------------------------------------------------
    minx, miny, maxx, maxy = gdf.total_bounds  # (lon_min, lat_min, lon_max, lat_max)
    center_lng = (minx + maxx) / 2.0
    center_lat = (miny + maxy) / 2.0

    # ----------------------------------------------------------------------
    # 3️⃣ Convert GeoDataFrame to GeoJSON for MapLibre
    # ----------------------------------------------------------------------
    geojson_str = gdf.to_json()
    geojson = json.loads(geojson_str)  # ensure a proper Python dict for Jinja2 tojson filter

    # ----------------------------------------------------------------------
    # 4️⃣ Render the HTML/JS map
    # ----------------------------------------------------------------------
    html = Template(r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>MapLibre • Dark Basemap + Parquet Points</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.css"/>
  <script src="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.js"></script>
  <style>html,body,#map{height:100%;margin:0;background:#000}</style>
</head>
<body>
  <div id="map"></div>
  <script>
    const DARK_TILES   = {{ dark_tiles | tojson }};
    const ATTRIB       = {{ basemap_attribution | tojson }};
    const CENTER       = [{{ center_lng }}, {{ center_lat }}];
    const ZOOM         = {{ zoom }};
    const MINZOOM      = {{ minzoom }};
    const MAXZOOM      = {{ maxzoom }};
    const RMIN         = {{ circle_radius_min }};
    const RMAX         = {{ circle_radius_max }};
    const OPAC         = {{ circle_opacity }};

    // Inline dark raster basemap style
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
      // Add GeoJSON source with the points from the parquet file
      map.addSource('points', {
        type: 'geojson',
        data: {{ geojson | tojson }}
      });

      // Render points as circles
      map.addLayer({
        id: 'points-circles',
        type: 'circle',
        source: 'points',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 0, RMIN, 14, RMAX],
          'circle-color': '#35AF6D',
          'circle-opacity': OPAC,
          'circle-stroke-width': 0.5,
          'circle-stroke-color': '#0b0b0b'
        }
      });

      // Popup on click
      map.on('click', 'points-circles', e => {
        const f = e.features && e.features[0];
        if (!f) return;
        const html = '<pre style="margin:0;white-space:pre-wrap;">' +
                     JSON.stringify(f.properties || {}, null, 2) + '</pre>';
        new maplibregl.Popup({closeButton:true})
          .setLngLat(e.lngLat)
          .setHTML(html)
          .addTo(map);
      });

      map.on('mouseenter', 'points-circles', () => map.getCanvas().style.cursor = 'pointer');
      map.on('mouseleave', 'points-circles', () => map.getCanvas().style.cursor = '');
    });
  </script>
</body>
</html>
""").render(
        dark_tiles=dark_tiles,
        basemap_attribution=basemap_attribution,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        minzoom=minzoom,
        maxzoom=maxzoom,
        circle_radius_min=circle_radius_min,
        circle_radius_max=circle_radius_max,
        circle_opacity=circle_opacity,
        geojson=geojson
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)