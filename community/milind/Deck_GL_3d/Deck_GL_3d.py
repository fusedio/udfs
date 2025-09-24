import fused
from jinja2 import Template

@fused.udf(cache_max_age=0)
def udf(
    token: str = "UDF_DSM_Zonal_Stats",
    host: str = "https://www.fused.io",  # or https://staging.fused.io
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -122.4194,  # SF
    center_lat: float = 37.7749,
    zoom: float = 16.0,
    pitch: float = 60.0,
    bearing: float = -20.0,
    minzoom: int = 0,
    maxzoom: int = 15,
    elevation_scale: float = 1.0,   # stats -> height multiplier
):
    """
    Minimal Deck.gl 3D vector map (no raster). Requires server to serve MVT:
      {host}/.../{token}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt
    """
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Deck.gl 3D Vector (Minimal)</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/deck.gl@latest/dist.min.js"></script>
  <style>
    html,body{margin:0;height:100%} #map{position:absolute;inset:0}
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
    const PITCH        = {{ pitch }};
    const BEARING      = {{ bearing }};
    const MIN_ZOOM     = {{ minzoom }};
    const MAX_ZOOM     = {{ maxzoom }};
    const ELEV_SCALE   = {{ elevation_scale }};

    const mvtURL = (h,t) => h.replace(/\/+$/,'') +
      `/server/v1/realtime-shared/${t}/run/tiles/{z}/{x}/{y}?dtype_out_vector=mvt`;

    mapboxgl.accessToken = MAPBOX_TOKEN;
    if (mapboxgl.setTelemetryEnabled) { try { mapboxgl.setTelemetryEnabled(false); } catch(e){} }

    const map = new mapboxgl.Map({
      container: 'map',
      style: STYLE_URL,
      center: CENTER,
      zoom: ZOOM,
      pitch: PITCH,
      bearing: BEARING
    });

    const overlay = new deck.MapboxOverlay({
      layers: []
    });

    map.on('load', () => {
      map.addControl(overlay);

      const vector3d = new deck.MVTLayer({
        id: 'vector-3d',
        data: mvtURL(HOST, TOKEN),
        minZoom: MIN_ZOOM,
        maxZoom: MAX_ZOOM,
        pickable: true,
        extruded: true,
        wireframe: false,
        getElevation: f => {
          const s = f.properties && f.properties.stats;
          return (Number.isFinite(s) ? Number(s) : 0) * ELEV_SCALE;
        },
        getFillColor: f => {
          const s = f.properties && f.properties.stats;
          if (!Number.isFinite(s)) return [160,160,160,60];
          const t = Math.max(0, Math.min(1, s/100));
          return [Math.floor(30+200*t), Math.floor(120+60*(1-t)), Math.floor(90+30*(1-t)), 200];
        },
        getLineColor: [0,0,0,120],
        lineWidthMinPixels: 1,
        onClick: info => {
          if (!info || !info.coordinate || !info.object) return;
          new mapboxgl.Popup({closeButton:true})
            .setLngLat(info.coordinate)
            .setHTML('<pre style="margin:0;white-space:pre-wrap;">' +
                     JSON.stringify(info.object.properties||{}, null, 2) + '</pre>')
            .addTo(map);
        }
      });

      // Simple lighting so the extrusion looks 3D
      const ambient = new deck.AmbientLight({intensity: 1.0});
      const dir     = new deck.DirectionalLight({intensity: 1.0, direction: [-1,-2,-1]});
      overlay.setProps({ layers: [vector3d], effects: [new deck.LightingEffect({ambientLight: ambient, dirLight: dir})]});
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
        pitch=pitch,
        bearing=bearing,
        minzoom=minzoom,
        maxzoom=maxzoom,
        elevation_scale=elevation_scale,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
