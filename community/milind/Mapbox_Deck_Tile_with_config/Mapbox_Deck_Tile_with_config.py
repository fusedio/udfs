import fused

DEFAULT_CONFIG = r"""{
  "tileLayer": {
    "@@type": "TileLayer",
    "minZoom": 0,
    "maxZoom": 19,
    "tileSize": 256,
    "pickable": true
  },
  "vectorLayer": {
    "@@type": "GeoJsonLayer",
    "stroked": true,
    "filled": true,
    "pickable": true,
    "extruded": false,
    "opacity": 1,
    "lineWidthMinPixels": 1,
    "getLineColor": [200, 200, 200],
    "getFillColor": {
      "@@function": "colorContinuous",
      "attr": "metric",
      "domain": [100000, 0],
      "colors": "OrYel",
      "nullColor": [184, 184, 184]
    }
  }
}"""


@fused.udf(cache_max_age=0)
def udf(
    url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_Overture_Maps_Example/run/tiles/{z}/{x}/{y}?dtype_out_vector=json",
    config_json: str = DEFAULT_CONFIG,
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    center_lng: float = -98.5,
    center_lat: float = 39.5,
    zoom: float = 3
):
    from jinja2 import Template

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Geometry Tile/File Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <!-- Mapbox GL -->
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>

  <!-- deck.gl & carto color ramps -->
  <script src="https://unpkg.com/deck.gl@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/geo-layers@9.1.3/dist.min.js"></script>
  <script src="https://unpkg.com/@deck.gl/carto@9.1.3/dist.min.js"></script>
  <!-- WKT parser for convenience -->
  <script src="https://unpkg.com/wellknown@0.5.0/wellknown.min.js"></script>

  <style>
    html, body, #map { margin: 0; height: 100%; width: 100%; }
    #hud { position: absolute; top: 8px; left: 8px; z-index: 5; color: #fff; background: rgba(0,0,0,.65);
           padding: 8px 12px; border-radius: 6px; font: 12px/1.5 system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }
    #hud b { color: #4fc3f7; }
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="hud"><b>Status:</b> <span id="note">ready</span></div>

  <script>
    const MAPBOX_TOKEN = {{ mapbox_token | tojson }};
    const STYLE_URL    = "mapbox://styles/mapbox/dark-v10";
    const INPUT_URL    = {{ url | tojson }};
    const CONFIG       = JSON.parse({{ config_json | tojson }});

    const { TileLayer, GeoJsonLayer, MapboxOverlay } = deck;
    const { colorContinuous } = deck.carto;

    const $note = () => document.getElementById('note');
    function setNote(t){ const n=$note(); if(n) n.textContent=t; }

    // ---- Helpers for expressions/functions ----
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

    function processColorContinuous(cfg) {
      // If domain has only 2 entries, pass through unchanged; deck.carto will handle it
      return {
        attr: cfg.attr,
        domain: cfg.domain,
        colors: cfg.colors || 'TealGrn',
        nullColor: cfg.nullColor || [184,184,184]
      };
    }

    function parseVectorLayerConfig(config) {
      const out = {};
      for (const [k, v] of Object.entries(config || {})) {
        if (k === '@@type') continue;
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          if (v['@@function'] === 'colorContinuous') {
            out[k] = colorContinuous(processColorContinuous(v));
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

    // ---- Geometry normalization ----
    function asFeature(obj) {
      // Already a Feature
      if (obj && obj.type === 'Feature' && obj.geometry) return obj;

      // GeoJSON geometry in 'geometry'
      if (obj && obj.geometry && obj.geometry.type && obj.geometry.coordinates) {
        return { type: 'Feature', properties: { ...obj }, geometry: obj.geometry };
      }

      // WKT string in 'wkt'
      if (obj && typeof obj.wkt === 'string' && window.wellknown) {
        try {
          const geom = window.wellknown.parse(obj.wkt);
          if (geom) return { type: 'Feature', properties: { ...obj }, geometry: geom };
        } catch {}
      }

      // Point from lon/lat variants
      const lon = obj?.longitude ?? obj?.lon ?? obj?.lng ?? obj?.x;
      const lat = obj?.latitude  ?? obj?.lat ?? obj?.y;
      if (typeof lon === 'number' && typeof lat === 'number') {
        return { type: 'Feature', properties: { ...obj }, geometry: { type: 'Point', coordinates: [lon, lat] } };
      }

      // Polygon from coordinates array (lon/lat order)
      if (Array.isArray(obj?.coordinates)) {
        const c = obj.coordinates;
        if (Array.isArray(c[0]) && Array.isArray(c[0][0]) && typeof c[0][0][0] === 'number') {
          return { type: 'Feature', properties: { ...obj }, geometry: { type: 'Polygon', coordinates: c } };
        }
        if (Array.isArray(c) && typeof c[0] === 'number' && c.length === 2) {
          // single point [lon, lat]
          return { type: 'Feature', properties: { ...obj }, geometry: { type: 'Point', coordinates: c } };
        }
      }

      // If nothing matched, return null to drop
      return null;
    }

    function toFeatures(raw){
      // If FeatureCollection
      if (raw?.type === 'FeatureCollection' && Array.isArray(raw.features)) return raw.features;
      // If array of features or rows
      const arr = Array.isArray(raw) ? raw : (Array.isArray(raw?.data) ? raw.data : (raw?.features || []));
      const feats = arr.map(d => asFeature(d?.type === 'Feature' ? d : (d?.properties ? { ...d, properties: d.properties } : { ...d })))
                      .filter(Boolean);
      return feats;
    }

    // Mapbox init
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const map = new mapboxgl.Map({ container:'map', style:STYLE_URL, center:[{{ center_lng }}, {{ center_lat }}], zoom: {{ zoom }} });

    const tileCfg = CONFIG.tileLayer || {};
    const vecCfg  = parseVectorLayerConfig(CONFIG.vectorLayer || {});
    const isTile  = /\{z\}|\{x\}|\{y\}/.test(INPUT_URL);

    const overlay = new MapboxOverlay({ interleaved: false, layers: [] });
    map.addControl(overlay);

    async function fetchAndNormalize(url, signal) {
      const res = await fetch(url, { cache:'no-cache', signal });
      if (!res.ok) throw new Error(String(res.status));
      const text = await res.text();
      let data;
      try { data = JSON.parse(text); } catch { data = []; }
      const feats = toFeatures(data).map(f => {
        const p = f.properties || {};
        // normalize metric/value-like fields to 'metric'
        const metric = p.metric ?? p.value ?? p.count ?? p.cnt ?? p.total ?? p.pct ?? p.area ?? p.val;
        return { ...f, properties: { ...p, metric } };
      });
      return feats;
    }

    if (isTile) {
      overlay.setProps({
        layers: [
          new TileLayer({
            id: 'geom-tiles',
            data: INPUT_URL,
            tileSize: tileCfg.tileSize ?? 256,
            minZoom:   tileCfg.minZoom   ?? 0,
            maxZoom:   tileCfg.maxZoom   ?? 19,
            pickable:  tileCfg.pickable  ?? true,
            getTileData: async ({ index, signal }) => {
              const {x,y,z} = index;
              const url = INPUT_URL.replace('{z}', z).replace('{x}', x).replace('{y}', y);
              try {
                const feats = await fetchAndNormalize(url, signal);
                setNote(`z${z} (${x},${y}) → ${feats.length}`);
                return feats;
              } catch (e) {
                const s = String(e?.name||e);
                if (!/Abort/i.test(s)) setNote(`error z${z} (${x},${y})`);
                return [];
              }
            },
            renderSubLayers: (props) => {
              const data = props.data || [];
              if (!data.length) return null;
              return new GeoJsonLayer({
                id: `${props.id}-geojson`,
                data: { type: 'FeatureCollection', features: data },
                // default styling
                pickable: true, stroked: true, filled: true, extruded: false,
                lineWidthMinPixels: 1,
                // apply config overrides
                ...vecCfg
              });
            }
          })
        ]
      });
    } else {
      // File mode
      (async () => {
        try {
          const feats = await fetchAndNormalize(INPUT_URL);
          setNote(`file → ${feats.length}`);
          const layer = new GeoJsonLayer({
            id: 'file-geojson',
            data: { type: 'FeatureCollection', features: feats },
            pickable: true, stroked: true, filled: true, extruded: false,
            lineWidthMinPixels: 1,
            ...vecCfg
          });
          overlay.setProps({ layers: [layer] });
        } catch (e) {
          console.error('file load error', e);
          setNote('file load error');
        }
      })();
    }
  </script>
</body>
</html>
""").render(
        url=url,
        config_json=config_json,
        mapbox_token=mapbox_token,
        center_lng=center_lng,
        center_lat=center_lat,
        zoom=zoom,
    )

    common = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")
    return common.html_to_obj(html)


