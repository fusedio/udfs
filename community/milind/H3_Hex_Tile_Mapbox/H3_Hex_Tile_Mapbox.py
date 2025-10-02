import fused
import json

@fused.udf(cache_max_age=0)
def udf(
    tile_url_template: str = "https://www.fused.io/server/v1/realtime-shared/UDF_Ookla_Download_Speeds/run/tiles/{z}/{x}/{y}?dtype_out_vector=json"
):
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>H3 XYZ Workbench-style Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>
  <style>
    html, body, #map {{ margin: 0; height: 100%; width: 100%; }}
    #hud {{ position: absolute; top: 8px; left: 8px; z-index: 5; color: #fff; background: rgba(0,0,0,.65); padding: 8px 12px; border-radius: 6px; font: 12px/1.5 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }}
    #hud b {{ color: #4fc3f7; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="hud"><b>Tiles:</b> <span id="note">ready</span></div>

  <script>
    const MAPBOX_TOKEN = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA";
    const STYLE_URL = "mapbox://styles/mapbox/dark-v10";
    // XYZ template returning JSON array [{{hex, metric}}] or FeatureCollection
    const TPL = {json.dumps(tile_url_template)};

    const {{ TileLayer, H3HexagonLayer, MapboxOverlay }} = deck;
    const {{ colorContinuous }} = deck.carto;

    const $note = () => document.getElementById('note');
    function setNote(t){{ const n=$note(); if(n) n.textContent=t; }}

    // Convert incoming hex (unsafe JSON numbers) → hex string safely
    function toH3String(hex) {{
      try {{
        if (typeof hex === 'string') {{
          // If the string is decimal digits, convert via BigInt → hex
          if (/^\\d+$/.test(hex)) {{
            return BigInt(hex).toString(16);
          }}
          return hex.toLowerCase();
        }}
        if (typeof hex === 'number') return BigInt(hex).toString(16);
        if (typeof hex === 'bigint') return hex.toString(16);
        if (Array.isArray(hex) && hex.length === 2) {{
          // Accept [high32, low32] or [low32, high32] – choose valid one
          const a = (BigInt(hex[0]) << 32n) | BigInt(hex[1]);
          const b = (BigInt(hex[1]) << 32n) | BigInt(hex[0]);
          const aStr = a.toString(16), bStr = b.toString(16);
          if (typeof h3 !== 'undefined' && h3.isValidCell) {{
            if (h3.isValidCell(aStr)) return aStr;
            if (h3.isValidCell(bStr)) return bStr;
          }}
          return aStr; // fallback
        }}
      }} catch(_e) {{}}
      return null;
    }}

    // Normalize any tile JSON into a plain array of {{hex, metric, ...}}
    function normalize(raw){{
      const arr = Array.isArray(raw) ? raw : (Array.isArray(raw?.data) ? raw.data : (raw?.features || []));
      const items = arr.map(d => d?.properties ? {{...d.properties}} : {{...d}});
      return items.map(p => {{
        let hex = p.hex ?? p.h3 ?? p.id;
        const metric = p.metric ?? p.value;
        const h = toH3String(hex);
        // Place spreads first so computed hex string and metric override originals
        return h ? {{ ...p, hex: h, metric }} : null;
      }}).filter(Boolean);
    }}

    // Color function like workbench presets
    const getFillColor = colorContinuous({{
      attr: 'metric',
      domain: [100000, 0],
      colors: 'TealGrn',
      nullColor: [184,184,184]
    }});

    // Mapbox init
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({{ container:'map', style:STYLE_URL, center:[-98.5,39.5], zoom:3 }});

    const overlay = new MapboxOverlay({{
      // Use a separate WebGL2 context to avoid basemap incompatibility
      interleaved: false,
      layers: [
        new TileLayer({{
          id: 'hex-tiles',
          data: TPL,
          tileSize: 256,
          minZoom: 0,
          maxZoom: 19,
          getTileData: async ({{index, signal}}) => {{
            const {{x,y,z}} = index;
            const url = TPL.replace('{{' + 'z}}', z).replace('{{' + 'x}}', x).replace('{{' + 'y}}', y);
            try {{
              const res = await fetch(url, {{cache:'no-cache', signal}});
              if (!res.ok) throw new Error(String(res.status));
              // Parse as text to preserve large integers, then coerce hex fields to strings
              const text = await res.text();
              const textWithHexStrings = text.replace(/"hex"\\s*:\\s*(\\d+)/g, (_m, d) => `"hex":"${{d}}"`);
              const data = JSON.parse(textWithHexStrings);
              const out = normalize(data);
              setNote(`z${{z}} (${{x}},${{y}}) → ${{out.length}}`);
              return out;
            }} catch(e){{
              setNote(`error z${{z}} (${{x}},${{y}})`);
              return [];
            }}
          }},
          renderSubLayers: props => new H3HexagonLayer({{
            id: `${{props.id}}-h3`,
            data: props.data,
            pickable: true,
            stroked: false,
            filled: true,
            extruded: false,
            coverage: 0.9,
            lineWidthMinPixels: 1,
            getHexagon: d => d.hex,
            getFillColor,
            getLineColor: [200,200,200]
          }})
        }})
      ]
    }});
    map.addControl(overlay);

    // Tooltip
    map.on('mousemove', (e)=>{{
      const info = overlay.pickObject({{x:e.point.x, y:e.point.y, radius:4}});
      if(info?.object){{
        map.getCanvas().style.cursor='pointer';
        const p = info.object;
        setNote(`hex ${{p.hex}}${{p.metric!=null?` • ${{Number(p.metric).toFixed(2)}}`:''}}`);
      }} else {{
        map.getCanvas().style.cursor='';
      }}
    }});
  </script>
</body>
</html>"""
    common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")
    return common.html_to_obj(html)
