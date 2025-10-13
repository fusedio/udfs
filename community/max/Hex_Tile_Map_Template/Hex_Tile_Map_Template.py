DEFAULT_CONFIG = r"""{
  "tileLayer": {
    "@@type": "TileLayer",
    "maxZoom": 12
  },
  "hexLayer": {
    "@@type": "H3HexagonLayer",
    "filled": true,
    "pickable": true,
    "extruded": false,
    "getHexagon": "@@=properties.hex",
    "getFillColor": {
      "@@function": "colorContinuous",
      "attr": "POP",
      "domain": [
        5000,
        0
      ],
      "steps": 20,
      "colors": "Magenta"
    }
  } 
}"""

@fused.udf(cache_max_age=0)
def udf(
    tile_url_template: str = "https://www.fused.io/server/v1/realtime-shared/fsh_3LSmFZyaUuDIt6FB7PFBGs/run/tiles/{z}/{x}/{y}?dtype_out_vector=json",
    config_json: str = DEFAULT_CONFIG,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    # Center the map on California (approximate geographic center)
    center_lng: float = -119.4179,
    center_lat: float = 36.7783,
    # Adjust zoom to show the state clearly
    zoom: float = 5,
    # List of column names to show in the tooltip (default: use the colored attribute)
    tooltip_columns: list = None,
):
    from jinja2 import Template

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>H3 XYZ Workbench-style Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <!-- Mapbox GL -->
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <!-- Load h3-js FIRST, then deck.gl + geo-layers (+ carto for color ramps) -->
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>

  <style>
    html, body, #map { margin: 0; height: 100%; width: 100%; }
    #hud { position: absolute; top: 8px; left: 8px; z-index: 5; color: #fff; background: rgba(0,0,0,.65);
           padding: 8px 12px; border-radius: 6px; font: 12px/1.5 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }
    #hud b { color: #4fc3f7; }
    /* Tooltip styling */
    #tooltip {
      position: absolute;
      pointer-events: none;
      background: rgba(0,0,0,0.7);
      color: #fff;
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 12px;
      display: none;
      z-index: 6;
    }
    /* Legend / toolbar styling */
    #legend {
      position: absolute;
      bottom: 8px;
      left: 8px;
      z-index: 5;
      background: rgba(0,0,0,.65);
      padding: 4px 6px;
      border-radius: 4px;
      font: 12px/1.5 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      color: #fff;
      display: flex;
      flex-direction: column;
      align-items: center;
    }
    #legend .gradient {
      width: 120px;
      height: 12px;
      margin-bottom: 4px;
    }
    #legend .labels {
      width: 100%;
      display: flex;
      justify-content: space-between;
      font-size: 10px;
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="hud"><b>Tiles:</b> <span id="note">ready</span></div>
  <div id="tooltip"></div>
  <div id="legend">
    <div class="gradient"></div>
    <div class="labels"><span id="maxLabel"></span> <span id="minLabel"></span></div>
  </div>

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const STYLE_URL    = "mapbox://styles/mapbox/dark-v10";
    const TPL          = {{ tile_url_template | tojson }};
    const CONFIG       = JSON.parse({{ config_json | tojson }});
    const TOOLTIP_COLUMNS = {{ tooltip_columns | default([]) | tojson }};

    const { TileLayer, PolygonLayer, MapboxOverlay } = deck;
    const H3HexagonLayer = deck.H3HexagonLayer || (deck.GeoLayers && deck.GeoLayers.H3HexagonLayer);
    const { colorContinuous } = deck.carto;

    const $note = () => document.getElementById('note');
    const $tooltip = () => document.getElementById('tooltip');

    function setNote(t){ const n=$note(); if(n) n.textContent=t; }

    // ----- helpers for @@function / @@= support -----
    function evalExpression(expr, object) {
      if (typeof expr === 'string' && expr.startsWith('@@=')) {
        const code = expr.slice(3);
        try {
          const fn = new Function('object', `
            const properties = object?.properties || object || {};
            return (${code});
          `);
          return fn(object);
        } catch (e) { console.error('@@= eval error:', expr, e); return null; }
      }
      return expr;
    }

    function hasProp({ property, present, absent }) {
      return (object) => {
        const props = object?.properties || object || {};
        if (property in props && props[property] !== null && props[property] !== undefined) {
          if (typeof present === 'function') return present(object);
          if (typeof present === 'string' && present.startsWith('@@=')) return evalExpression(present, object);
          return present;
        }
        return typeof absent === 'function' ? absent(object) : absent;
      };
    }

    function processColorContinuous(cfg) {
      // Expand 2-element domain into steps array (matching client behavior)
      let domain = cfg.domain;
      if (domain && domain.length === 2) {
        const [min, max] = domain;
        const steps = cfg.steps ?? 20;
        const stepSize = (max - min) / (steps - 1);
        domain = Array.from({ length: steps }, (_, i) => min + stepSize * i);
      }

      return {
        attr: cfg.attr,
        domain: domain,
        colors: cfg.colors || 'TealGrn',
        nullColor: cfg.nullColor || [184,184,184]
      };
    }

    function parseHexLayerConfig(config) {
      const out = {};
      for (const [k, v] of Object.entries(config || {})) {
        if (k === '@@type') continue;
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          if (v['@@function'] === 'colorContinuous') {
            out[k] = colorContinuous(processColorContinuous(v));
          } else if (v['@@function'] === 'hasProp') {
            out[k] = hasProp(v);
          } else {
            out[k] = v;
          }
        } else if (typeof v === 'string' && v.startsWith('@@=')) {
          out[k] = (obj) => evalExpression(v, obj);
        } else {
          out[k] = v;
        }
      }
      return out;
    }

    // ----- H3 ID safety (handles string/number/bigint/[hi,lo]) -----
    function toH3String(hex) {
      try {
        if (hex == null) return null;
        if (typeof hex === 'string') {
          const s = hex.startsWith('0x') ? hex.slice(2) : hex;
          return (/^\d+$/.test(s) ? BigInt(s).toString(16) : s.toLowerCase());
        }
        if (typeof hex === 'number') return BigInt(Math.trunc(hex)).toString(16);
        if (typeof hex === 'bigint') return hex.toString(16);
        if (Array.isArray(hex) && hex.length === 2) {
          const a = (BigInt(hex[0]) << 32n) | BigInt(hex[1]);
          const b = (BigInt(hex[1]) << 32n) | BigInt(hex[0]);
          const sa = a.toString(16), sb = b.toString(16);
          if (h3.isValidCell?.(sa)) return sa;
          if (h3.isValidCell?.(sb)) return sb;
          return sa;
        }
      } catch(_) {}
      return null;
    }

    // Normalize any tile JSON into data rows, and add both top-level + properties for compatibility
    function normalize(raw){
      const arr = Array.isArray(raw)
        ? raw
        : (Array.isArray(raw?.data) ? raw.data
        : (Array.isArray(raw?.features) ? raw?.features : []));
      const rows = arr.map(d => d?.properties ? {...d.properties} : {...d});
      return rows.map(p => {
        const hexRaw = p.hex ?? p.h3 ?? p.index ?? p.id;
        const metric = p.metric ?? p.value ?? p.count ?? p.pct ?? p.area ?? p.total ?? p.val;
        const hex = toH3String(hexRaw);
        if (!hex) return null;
        const props = { ...p, hex, metric };
        return { ...props, properties: { ...props } };
      }).filter(Boolean);
    }

    // Mapbox init
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ container:'map', style:STYLE_URL, center:[{{ center_lng }}, {{ center_lat }}], zoom: {{ zoom }} });

    // Fetch & parse tile with big-int protection for "hex|h3|index"
    async function getTileData({ index, signal }) {
      const {x,y,z} = index;
      const url = TPL.replace('{z}', z).replace('{x}', x).replace('{y}', y);
      try {
        const res = await fetch(url, { signal });
        if (!res.ok) throw new Error(String(res.status));
        let text = await res.text();
        text = text.replace(/\"(hex|h3|index)\"\s*:\s*(\d+)/gi, (_m, k, d) => `"${k}":"${d}"`);
        const data = JSON.parse(text);
        const out = normalize(data);
        setNote(`z${z} (${x},${y}) → ${out.length}`);
        return out;
      } catch (e) {
        const s = String(e?.name||e);
        if (!/Abort/i.test(s)) {
          console.error('tile error', e);
          setNote(`error z${z} (${x},${y})`);
        }
        return [];
      }
    }

    // Build layers from CONFIG
    const tileCfg = CONFIG.tileLayer || {};
    const hexCfg  = parseHexLayerConfig(CONFIG.hexLayer || {});

    // Extract the attribute name from color config for hover display (fallback)
    const colorAttr = CONFIG.hexLayer?.getFillColor?.attr || 'metric';

    // ---- Legend setup -------------------------------------------------
    const legendMin = document.getElementById('minLabel');
    const legendMax = document.getElementById('maxLabel');
    const gradientDiv = document.querySelector('#legend .gradient');
    const domainVals = (CONFIG.hexLayer?.getFillColor?.domain) || [0,0];
    if (Array.isArray(domainVals) && domainVals.length===2) {
        legendMax.textContent = domainVals[0];
        legendMin.textContent = domainVals[1];
    }
    // Simple magenta-to-white gradient (you can customize the colours)
    if (gradientDiv) {
        gradientDiv.style.background = 'linear-gradient(to right, #ff00ff, #ffffff)';
    }

    const overlay = new MapboxOverlay({
      interleaved: false,
      layers: [
        new TileLayer({
          id: 'hex-tiles',
          data: TPL,
          tileSize: tileCfg.tileSize ?? 256,
          minZoom:   tileCfg.minZoom   ?? 0,
          maxZoom:   tileCfg.maxZoom   ?? 19,
          pickable:  tileCfg.pickable  ?? true,
          maxRequests: 6,
          getTileData,
          renderSubLayers: (props) => {
            const data = props.data || [];
            if (!data.length) return null;

            if (H3HexagonLayer) {
              return new H3HexagonLayer({
                id: `${props.id}-h3`,
                data,
                pickable: true, stroked: false, filled: true, extruded: false,
                coverage: 0.9, lineWidthMinPixels: 1,
                getHexagon: d => d.hex,
                ...hexCfg
              });
            }

            // Fallback: PolygonLayer
            const polys = data.map(d => {
              const ring = h3.cellToBoundary(d.hex, true).map(([lat,lng]) => [lng, lat]);
              return { ...d, polygon: ring };
            });
            return new PolygonLayer({
              id: `${props.id}-poly-fallback`,
              data: polys,
              pickable: true, stroked: true, filled: true, extruded: false,
              getPolygon: d => d.polygon,
              getFillColor: [184,184,184,220],
              getLineColor: [200,200,200,255],
              lineWidthMinPixels: 1
            });
          }
        })
      ]
    });
    map.addControl(overlay);

    // HUD hover – construct tooltip from requested columns
    map.on('mousemove', (e)=>{ 
      const info = overlay.pickObject({x:e.point.x, y:e.point.y, radius:4});
      if(info?.object){
        map.getCanvas().style.cursor='pointer';
        const p = info.object;

        // Build tooltip content
        const lines = [`hex ${p.hex}`];
        if (TOOLTIP_COLUMNS && TOOLTIP_COLUMNS.length) {
          TOOLTIP_COLUMNS.forEach(col => {
            if (p[col] !== undefined) {
              const val = p[col];
              lines.push(`${col}: ${Number(val).toFixed(2)}`);
            }
          });
        } else {
          const val = p[colorAttr] ?? p.metric;
          if (val != null) lines.push(`${colorAttr}: ${Number(val).toFixed(2)}`);
        }
        const tooltipText = lines.join(' • ');
        const tt = $tooltip();
        tt.innerHTML = tooltipText;
        tt.style.left = `${e.point.x + 10}px`;
        tt.style.top  = `${e.point.y + 10}px`;
        tt.style.display = 'block';
        setNote(tooltipText);
      } else {
        map.getCanvas().style.cursor='';
        $tooltip().style.display = 'none';
        const lastZ = map.getZoom().toFixed(1);
        setNote(`zoom: ${lastZ}`);
      }
    });
  </script>
</body>
</html>
""").render(
        tile_url_template=tile_url_template,
        config_json=config_json,
        mapbox_token=mapbox_token,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
        tooltip_columns=tooltip_columns,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/f430c25/public/common/")
    return common.html_to_obj(html)