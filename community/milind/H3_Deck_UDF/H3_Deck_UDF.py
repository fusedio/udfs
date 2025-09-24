
@fused.udf(cache_max_age=0)
def udf(
    # H3 data (each row: {hex: '8f...', count: number})
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_DuckDB_H3_SF/run/file?format=json",

    # Mapbox + camera
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -122.417759,
    center_lat: float = 37.776452,
    zoom: float = 11.0,
    pitch: float = 50.0,
    bearing: float = -10.0,

    # Layer tuning
    elevation_scale: float = 20.0,     # count * elevation_scale
    max_count_for_color: float = 500.0, # for [255, (1 - count/max)*255, 0]
    wireframe: bool = False,
):

    from jinja2 import Template

    """
    Deck.gl 3D H3HexagonLayer over Mapbox, using your JSON (hex,count) data.
    - Elevation: count * elevation_scale
    - Color: [255, (1 - count/max_count_for_color)*255, 0]
    - Popup shows hex & count
    """
    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Deck.gl 3D H3 (Fixed)</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>

  <!-- Load h3-js BEFORE deck.gl -->
  <script src="https://unpkg.com/h3-js@latest/dist/h3-js.umd.js"></script>
  <!-- Then load deck.gl -->
  <script src="https://unpkg.com/deck.gl@9.0.0/dist.min.js"></script>

  <style>
    html, body { margin:0; height:100%; }
    #map { position:absolute; inset:0; }
    .deck-tooltip {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
      font-size: 12px;
      padding: 6px 8px;
      background: rgba(0,0,0,0.7);
      color: #fff;
      border-radius: 6px;
      max-width: 260px;
      pointer-events: none;
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <script>
    // Verify h3-js is loaded properly
    if (typeof h3 === 'undefined') {
      console.error('h3-js library not loaded!');
      throw new Error('h3-js library must be loaded before deck.gl');
    }

    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const STYLE_URL    = {{ style_url    | tojson }};
    const DATA_URL     = {{ data_url     | tojson }};

    const INITIAL_VIEW_STATE = {
      longitude: {{ center_lng }},
      latitude:  {{ center_lat }},
      zoom:      {{ zoom }},
      pitch:     {{ pitch }},
      bearing:   {{ bearing }}
    };

    const ELEVATION_SCALE     = {{ elevation_scale }};
    const MAX_COUNT_FOR_COLOR = {{ max_count_for_color }};
    const WIREFRAME           = {{ wireframe | tojson }};

    mapboxgl.accessToken = MAPBOX_TOKEN;
    if (mapboxgl.setTelemetryEnabled) { try { mapboxgl.setTelemetryEnabled(false); } catch(e){} }

    const map = new mapboxgl.Map({
      container: 'map',
      style: STYLE_URL,
      center: [INITIAL_VIEW_STATE.longitude, INITIAL_VIEW_STATE.latitude],
      zoom: INITIAL_VIEW_STATE.zoom,
      pitch: INITIAL_VIEW_STATE.pitch,
      bearing: INITIAL_VIEW_STATE.bearing
    });

    const overlay = new deck.MapboxOverlay({
      interleaved: true,
      layers: []
    });

    map.on('load', () => {
      map.addControl(overlay);

      const ambient = new deck.AmbientLight({intensity: 1.0});
      const dir     = new deck.DirectionalLight({intensity: 0.9, direction: [-1, -2, -1]});
      const effects = [ new deck.LightingEffect({ambientLight: ambient, dirLight: dir}) ];

      const h3Layer = new deck.H3HexagonLayer({
        id: 'h3-3d',
        data: DATA_URL,
        pickable: true,
        extruded: true,
        wireframe: WIREFRAME,
        material: { ambient: 0.35, diffuse: 0.6, shininess: 32, specularColor: [255,255,255] },

        getHexagon: d => d.hex,
        getElevation: d => (d && Number.isFinite(d.count) ? d.count : 0) * ELEVATION_SCALE,
        getFillColor: d => {
          const count = (d && Number.isFinite(d.count)) ? d.count : 0;
          const g = Math.max(0, Math.min(255, (1 - (count / MAX_COUNT_FOR_COLOR)) * 255));
          return [255, g, 0];
        },
        filled: true,
        stroked: true,
        getLineColor: [255, 255, 255],
        getLineWidth: 2,
        lineWidthUnits: 'pixels',

        onClick: info => {
          if (!info || !info.coordinate || !info.object) return;
          const props = { hex: info.object.hex, count: info.object.count };
          new mapboxgl.Popup({closeButton:true})
            .setLngLat(info.coordinate)
            .setHTML('<pre style="margin:0;white-space:pre-wrap;">' +
                      JSON.stringify(props, null, 2) + '</pre>')
            .addTo(map);
        }
      });

      overlay.setProps({ layers: [h3Layer], effects });
    });
  </script>
</body>
</html>
""").render(
    data_url=data_url,
    mapbox_token=mapbox_token,
    style_url=style_url,
    center_lng=center_lng,
    center_lat=center_lat,
    zoom=zoom,
    pitch=pitch,
    bearing=bearing,
    elevation_scale=elevation_scale,
    max_count_for_color=max_count_for_color,
    wireframe=wireframe,
)

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)
