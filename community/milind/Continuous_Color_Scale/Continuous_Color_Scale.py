@fused.udf(cache_max_age=0)
def udf(
    # JSON rows: {hex: "8a2a1072b59ffff", count: number}
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_DuckDB_H3_SF/run/file?format=json",

    # Mapbox view
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -122.417759,
    center_lat: float = 37.776452,
    zoom: float = 11.0,
    pitch: float = 45.0,
    bearing: float = 0.0,

    # H3 layer
    elevation_scale: float = 10.0,   # count * elevation_scale
    wireframe: bool = False,

    # Continuous color scale
    domain_min: float = 0.0,
    domain_max: float = 600.0,
    null_color: tuple = (184, 184, 184),  # for missing/NaN
):
    from jinja2 import Template
    import fused

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>H3 Continuous Color Scale</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>

  <!-- Load h3-js BEFORE deck.gl -->
  <script src="https://unpkg.com/h3-js@latest/dist/h3-js.umd.js"></script>
  <!-- Then deck.gl -->
  <script src="https://unpkg.com/deck.gl@9.0.0/dist.min.js"></script>

  <style>
    html, body { margin:0; height:100%; }
    #map { position:absolute; inset:0; }
  </style>
</head>
<body>
  <div id="map"></div>
  <script>
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

    const ELEVATION_SCALE = {{ elevation_scale }};
    const WIREFRAME       = {{ wireframe | tojson }};
    const DOMAIN_MIN      = {{ domain_min }};
    const DOMAIN_MAX      = {{ domain_max }};
    const NULL_COLOR      = {{ null_color | tojson }};

    // Continuous color scale (yellow → red)
    // t in [0,1] -> [R,G,B]
    function ramp(t) {
      t = Math.max(0, Math.min(1, t));
      // two-stop gradient: bright yellow -> vivid red
      const stops = [
        [253, 231, 37],   // bright yellow
        [215, 48, 39]     // red
      ];
      const n = stops.length - 1;
      const p = t * n;
      const i = Math.floor(p);
      const f = p - i;
      const a = stops[i], b = stops[Math.min(i+1, n)];
      return [
        Math.round(a[0] + (b[0]-a[0])*f),
        Math.round(a[1] + (b[1]-a[1])*f),
        Math.round(a[2] + (b[2]-a[2])*f)
      ];
    }

    function colorForCount(count) {
      if (!Number.isFinite(count)) return [NULL_COLOR[0], NULL_COLOR[1], NULL_COLOR[2]];
      if (DOMAIN_MAX === DOMAIN_MIN) return ramp(1.0);
      const t = (count - DOMAIN_MIN) / (DOMAIN_MAX - DOMAIN_MIN);
      return ramp(t);
    }

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

      const h3Layer = new deck.H3HexagonLayer({
        id: 'h3-continuous',
        data: DATA_URL,
        pickable: true,
        extruded: true,
        wireframe: WIREFRAME,
        material: { ambient: 0.35, diffuse: 0.6, shininess: 32, specularColor: [255,255,255] },

        getHexagon: d => d.hex,  // hex string
        getElevation: d => {
          const c = (d && Number.isFinite(d.count)) ? d.count : 0;
          return c * ELEVATION_SCALE;
        },
        getFillColor: d => {
          const c = (d && Number.isFinite(d.count)) ? d.count : NaN;
          const rgb = colorForCount(c);
          return [rgb[0], rgb[1], rgb[2], 210]; // add alpha
        },
        stroked: true,
        getLineColor: [30, 30, 30],
        getLineWidth: 1,
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

      overlay.setProps({ layers: [h3Layer] });
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
        wireframe=wireframe,
        domain_min=domain_min,
        domain_max=domain_max,
        null_color=list(null_color),
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/351515e/public/common/")
    return common.html_to_obj(html)