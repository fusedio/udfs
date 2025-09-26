@fused.udf(cache_max_age=0)
def udf(
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_DuckDB_H3_SF/run/file?format=json",
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
    style_url: str = "mapbox://styles/mapbox/dark-v10",
    center_lng: float = -122.417759,
    center_lat: float = 37.776452,
    zoom: float = 11.0,
    pitch: float = 50.0,
    bearing: float = -10.0,
    elevation_scale: float = 10.0,
    max_count_for_color: float = 500.0,
    wireframe: bool = False,
):
    import fused
    from jinja2 import Template

    html = Template(r"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>H3 Hex Viewer</title>
  <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no"/>

  <link href="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.css" rel="stylesheet"/>
  <script src="https://api.mapbox.com/mapbox-gl-js/v3.2.0/mapbox-gl.js"></script>

  <!-- h3-js BEFORE deck.gl -->
  <script src="https://unpkg.com/h3-js@4.1.0/dist/h3-js.umd.js"></script>
  <!-- deck.gl -->
  <script src="https://unpkg.com/deck.gl@9.0.0/dist.min.js"></script>

  <style>
    html, body { margin:0; height:100%; }
    #map { position:absolute; inset:0; }
  </style>
</head>
<body>
  <div id="map"></div>
  <script>
    if (typeof h3 === 'undefined') throw new Error('h3-js not loaded');
    if (typeof deck === 'undefined') throw new Error('deck.gl not loaded');

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
    try { mapboxgl.setTelemetryEnabled(false); } catch(e) {}

    const map = new mapboxgl.Map({
      container: 'map',
      style: STYLE_URL,
      center: [INITIAL_VIEW_STATE.longitude, INITIAL_VIEW_STATE.latitude],
      zoom: INITIAL_VIEW_STATE.zoom,
      pitch: INITIAL_VIEW_STATE.pitch,
      bearing: INITIAL_VIEW_STATE.bearing
    });

    const overlay = new deck.MapboxOverlay({ interleaved: true, layers: [] });

    function toHexStringFromArray(arr) {
      if (!Array.isArray(arr)) return null;
      if (arr.length === 2 && arr.every(x => Number.isFinite(x) && x >= 0 && x <= 0xffffffff)) {
        const hi = BigInt(Math.trunc(arr[0])) << 32n;
        const lo = BigInt(Math.trunc(arr[1])) & 0xffffffffn;
        return (hi | lo).toString(16);
      }
      if (arr.every(x => Number.isFinite(x) && x >= 0 && x <= 255)) {
        const hex = arr.map(b => Math.trunc(b).toString(16).padStart(2, '0')).join('');
        return hex.replace(/^0+/, '') || '0';
      }
      return null;
    }

    function normalizeHex(v) {
      if (typeof v === 'string') {
        if (/^[0-9]+$/.test(v)) { try { return BigInt(v).toString(16); } catch { return v; } }
        if (v.startsWith('0x') || v.startsWith('0X')) return v.slice(2);
        return v;
      }
      if (typeof v === 'number' && Number.isFinite(v)) return Math.trunc(v).toString(16);
      if (typeof v === 'bigint') return v.toString(16);
      if (Array.isArray(v)) { const s = toHexStringFromArray(v); return s ?? String(v); }
      if (v && typeof v === 'object' && v.buffer instanceof ArrayBuffer && typeof v.length === 'number') {
        const s = toHexStringFromArray(Array.from(v)); return s ?? '';
      }
      return String(v ?? '');
    }

    function getMaybeHex(obj) {
      if (!obj) return undefined;
      if (typeof obj.hex !== 'undefined') return normalizeHex(obj.hex);
      const propsHex = obj?.properties?.hex;
      if (typeof propsHex !== 'undefined') return normalizeHex(propsHex);
      return undefined;
    }

    function bboxFromHexes(rows, maxCheck = 2000) {
      let minLng =  180, minLat =  90, maxLng = -180, maxLat = -90, count = 0;
      for (const r of rows) {
        if (count >= maxCheck) break;
        const h = getMaybeHex(r) ?? getMaybeHex(r?.properties);
        if (typeof h === 'string' && h) {
          try {
            const boundary = h3.cellToBoundary(h, true);
            for (const [lng, lat] of boundary) {
              if (lng < minLng) minLng = lng;
              if (lat < minLat) minLat = lat;
              if (lng > maxLng) maxLng = lng;
              if (lat > maxLat) maxLat = lat;
            }
            count++;
          } catch (e) {}
        }
      }
      if (count === 0) return null;
      return [[minLng, minLat], [maxLng, maxLat]];
    }

    async function loadData(url) {
      const resp = await fetch(url, {
        headers: { Accept: 'application/json' },
        redirect: 'follow',
        referrerPolicy: 'no-referrer'
      });
      const raw = await resp.text();

      // Preserve large integers for hex by converting them to strings pre-parse
      function stringifyHexNumbers(s) {
        try {
          let out = s.replace(/("hex"\s*:\s*)(-?\d+)/g, (m, p1, p2) => `${p1}"${p2}"`);
          out = out.replace(/("hex"\s*:\s*)\[(\s*-?\d+(?:\s*,\s*-?\d+)*\s*)\]/g, (m, p1, nums) => {
            const quoted = nums.replace(/-?\d+/g, (n) => `"${n}"`);
            return `${p1}[${quoted}]`;
          });
          return out;
        } catch (e) { return s; }
      }

      const json = JSON.parse(stringifyHexNumbers(raw));
      return Array.isArray(json)
        ? json
        : (Array.isArray(json?.data)
            ? json.data
            : (json?.type === 'FeatureCollection' && Array.isArray(json?.features))
                ? json.features
                : []);
    }

    function normalizeRecord(rec) {
      if (rec && rec.type === 'Feature' && rec.geometry && typeof rec.geometry.type === 'string') {
        const out = JSON.parse(JSON.stringify(rec));
        if (out.properties && typeof out.properties.hex !== 'undefined') {
          out.properties.hex = normalizeHex(out.properties.hex);
          if (typeof out.properties.hex === 'string' && (out.properties.hex.startsWith('0x') || out.properties.hex.startsWith('0X'))) {
            out.properties.hex = out.properties.hex.slice(2);
          }
        }
        return out;
      }
      const r = rec || {};
      const out = {};
      const keys = Object.keys(r);

      const hexKey = keys.find(k => {
        const lk = k.toLowerCase();
        return lk === 'hex' || lk === 'h3' || lk === 'h3index' || lk === 'h3_index' ||
               lk === 'h3_id' || lk === 'h3id' || lk === 'cell' || lk === 'index';
      });

      const valKey = keys.find(k => {
        const lk = k.toLowerCase();
        return lk === 'value' || lk === 'count' || lk === 'sum' || lk === 'n' || lk === 'weight' || lk === 'pct';
      });

      if (hexKey) out.hex = normalizeHex(r[hexKey]);
      if (typeof out.hex === 'string' && (out.hex.startsWith('0x') || out.hex.startsWith('0X'))) {
        out.hex = out.hex.slice(2);
      }

      const numVal = Number(r[valKey ?? 'value']);
      out.value = Number.isFinite(numVal) ? numVal : 1;

      for (const k of keys) if (k !== hexKey && k !== valKey) out[k] = r[k];
      return out;
    }

    function makeH3Layer(data) {
      function getCellId(obj) {
        const h = getMaybeHex(obj) ?? getMaybeHex(obj?.properties);
        return (typeof h === 'string' && h.length > 0) ? h : null;
      }
      return new deck.H3HexagonLayer({
        id: 'h3-hex-layer',
        data,
        pickable: true,
        extruded: true,
        wireframe: WIREFRAME,
        coverage: 1.0,
        opacity: 0.95,
        h3Functions: h3,
        getHexagon: getCellId,
        getElevation: d => {
          const v = (typeof d?.value === 'number') ? d.value :
                    (typeof d?.properties?.value === 'number') ? d.properties.value :
                    (typeof d?.properties?.pct === 'number') ? d.properties.pct : 0;
          return v * ELEVATION_SCALE;
        },
        getFillColor: d => {
          const v = (typeof d?.value === 'number') ? d.value :
                    (typeof d?.properties?.pct === 'number') ? d.properties.pct : 0;
          const g = Math.max(0, Math.min(255, (1 - (v / MAX_COUNT_FOR_COLOR)) * 255));
          return [255, g, 0];
        },
        filled: true,
        stroked: false,
        lineWidthUnits: 'meters',
      });
    }

    map.on('load', async () => {
      map.addControl(overlay);

      const rowsRaw = await loadData(DATA_URL);
      const rows = rowsRaw.map(normalizeRecord);

      overlay.setProps({ layers: [makeH3Layer(rows)] });

      const bbox = bboxFromHexes(rows);
      if (bbox) { try { map.fitBounds(bbox, { padding: 40, duration: 500 }); } catch (e) {} }
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


