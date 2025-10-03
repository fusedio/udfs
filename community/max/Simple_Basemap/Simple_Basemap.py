import fused
from jinja2 import Template
import json
import pandas as pd
import geopandas as gpd

@fused.udf()
def udf(
    # View – these defaults will be overwritten by the computed center below
    center_lng: float = 0.0,
    center_lat: float = 0.0,
    zoom: float = 10,
    minzoom: int = 0,
    maxzoom: int = 14,

    # Circle styling
    circle_opacity: float = 0.9,
    circle_radius_min: float = 1.0,   # reduced size
    circle_radius_max: float = 4.0,   # reduced size

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
    # 1️⃣ Load data via @blue_moth token (single parquet read)
    # ----------------------------------------------------------------------
    # Replace this with shared token of the UDF you want to pull data from
    data = fused.run("UDF_Airbnb_Point_Location_Dataset")

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
    # 2️⃣ Compute map centre, bounds and bedrooms range from data
    # ----------------------------------------------------------------------
    minx, miny, maxx, maxy = gdf.total_bounds  # (lon_min, lat_min, lon_max, lat_max)
    center_lng = (minx + maxx) / 2.0
    center_lat = (miny + maxy) / 2.0

    # Bedrooms range for colour ramp
    bedrooms_min = float(gdf["bedrooms"].min())
    bedrooms_max = float(gdf["bedrooms"].max())

    # ----------------------------------------------------------------------
    # 3️⃣ Convert GeoDataFrame to GeoJSON for MapLibre
    # ----------------------------------------------------------------------
    geojson_str = gdf.to_json()
    geojson = json.loads(geojson_str)  # ensure proper dict for Jinja2

    # ----------------------------------------------------------------------
    # 4️⃣ Render the HTML/JS map (with layer name toggle, fit-bounds button and legend)
    # ----------------------------------------------------------------------
    layer_name = "Airbnb Listings SF"
    html = Template(r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>MapLibre • Dark Basemap + Parquet Points</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.css"/>
  <script src="https://unpkg.com/maplibre-gl@5.7.3/dist/maplibre-gl.js"></script>
  <style>
    html,body,#map{height:100%;margin:0;background:#000}
    #controls{position:absolute;top:10px;left:10px;z-index:1;background:rgba(255,255,255,0.9);padding:5px;border-radius:4px;}
    #controls button{margin:2px;}
    #controls label{margin-left:4px;font-weight:normal;}
    #legend{position:absolute;bottom:10px;left:10px;z-index:1;background:rgba(255,255,255,0.9);padding:5px;border-radius:4px;font-size:12px;}
    #legend .gradient{width:150px;height:10px;background:linear-gradient(to right, #440154, #fde724);}
    #legend .labels{display:flex;justify-content:space-between;}
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="controls">
    <input type="checkbox" id="layerToggle" checked>
    <label for="layerToggle">{{ layer_name }}</label>
    <button id="fitBtn">Fit Data</button>
  </div>
  <div id="legend">
    <div class="gradient"></div>
    <div class="labels"><span>{{ bedrooms_min }}</span><span>{{ bedrooms_max }}</span></div>
  </div>
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
    const DATA_BOUNDS  = [[{{ minx }}, {{ miny }}], [{{ maxx }}, {{ maxy }}]];
    const GEOJSON      = {{ geojson | tojson }};

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
        data: GEOJSON
      });

      // Render points as circles, coloured by bedrooms (viridis ramp)
      map.addLayer({
        id: 'points-circles',
        type: 'circle',
        source: 'points',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 0, RMIN, 14, RMAX],
          'circle-color': [
            'interpolate',
            ['linear'],
            ['get', 'bedrooms'],
            {{ bedrooms_min }}, '#440154',
            {{ bedrooms_max }}, '#fde724'
          ],
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

      // ---------- UI Controls ----------
      const layerToggle = document.getElementById('layerToggle');
      const fitBtn = document.getElementById('fitBtn');

      layerToggle.addEventListener('change', () => {
        const visibility = layerToggle.checked ? 'visible' : 'none';
        map.setLayoutProperty('points-circles', 'visibility', visibility);
      });

      fitBtn.addEventListener('click', () => {
        map.fitBounds(DATA_BOUNDS, {padding: 30});
      });
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
        minx=minx,
        miny=miny,
        maxx=maxx,
        maxy=maxy,
        geojson=geojson,
        layer_name=layer_name,
        bedrooms_min=bedrooms_min,
        bedrooms_max=bedrooms_max
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)