common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    config: dict | str = {
        "parameter_name": "form",
        "filter": [
            {
                "column": "location",
                "type": "selectbox",
                "default": "Midtown Manhattan",
                "parameter_name": "loc"
            },

            {
                # NEW COMPONENT
                "type": "geo",
                "parameter_name": "region_geom",
                "height": 240,
                "column": "geometry"  # <- geometry column in parquet
            },
            {
                # keep old bounds, now it's JUST bbox picker
                "type": "bounds",
                "parameter_name": "bbox",
                "default": [-125, 24, -66, 49],
                "height": 240
            },
            {
                "type": "text_input",
                "default": "Sample text",
                "parameter_name": "note"
            },
        ],
        "data_url": "https://unstable.udf.ai/fsh_9zLQaf2GUqTsmQLvJqVAO/run?dtype_out_raster=png&dtype_out_vector=parquet"

    },
    mapbox_token: str = "pk.eyJ1IjoiaXNhYWNmdXNlZGxhYnMiLCJhIjoiY2xicGdwdHljMHQ1bzN4cWhtNThvbzdqcSJ9.73fb6zHMeO_c8eAXpZVNrA",
):
    import json
    import jinja2

    # normalize config
    if isinstance(config, str):
        cfg = json.loads(config)
    else:
        cfg = config

    global_param_name = cfg.get("parameter_name")
    data_url = cfg.get("data_url")

    # normalize fields
    normalized_fields = []
    for f in cfg.get("filter", []):
        f_type = f.get("type")
        raw_param = f.get("parameter_name")

        # helper: get alias/channels
        def _alias_and_channels(default_alias):
            if isinstance(raw_param, str) and raw_param:
                return raw_param, [raw_param]
            else:
                return default_alias, []

        if f_type == "selectbox":
            col = f.get("column")
            default_val = f.get("default")

            if raw_param is None:
                raw_param = col

            alias_base, channels = _alias_and_channels(col)

            normalized_fields.append({
                "kind": "select",
                "col": col,
                "defaultValue": default_val,
                "channels": channels,
                "aliasBase": alias_base,
            })

        elif f_type == "daterange":
            cols = f.get("column", [])
            if isinstance(cols, list) and len(cols) == 2:
                start_col, end_col = cols
            else:
                start_col, end_col = "start_date", "end_date"

            if raw_param is None:
                raw_param = [start_col, end_col]

            if isinstance(raw_param, list) and len(raw_param) == 2:
                channels = [raw_param[0], raw_param[1]]
                alias_base = raw_param[0] or start_col
            elif isinstance(raw_param, str) and raw_param:
                channels = [raw_param]
                alias_base = raw_param
            else:
                channels = []
                alias_base = start_col

            normalized_fields.append({
                "kind": "date",
                "startCol": start_col,
                "endCol": end_col,
                "defaultValue": f.get("default"),
                "channels": channels,
                "aliasBase": alias_base,
            })

        elif f_type == "geo":
            # new component: renders geometries, submits FeatureCollection
            alias_base, channels = _alias_and_channels("geo")
            map_height_px = f.get("height", 240)
            geom_col = f.get("column")  # required
            render_flag = bool(f.get("render", False))
            normalized_fields.append({
                "kind": "geo",
                "channels": channels,
                "aliasBase": alias_base,
                "mapHeightPx": map_height_px,
                "geometryCol": geom_col,
                "render": render_flag,
            })

        elif f_type == "bounds":
            # old bbox picker (no geometry drawing now)
            alias_base, channels = _alias_and_channels("bounds")

            default_bounds = f.get("default", [-180, -90, 180, 90])
            map_height_px = f.get("height", 240)

            normalized_fields.append({
                "kind": "bounds",
                "channels": channels,
                "aliasBase": alias_base,
                "defaultBounds": default_bounds,
                "mapHeightPx": map_height_px,
            })

        elif f_type == "text_input":
            alias_base, channels = _alias_and_channels("text_input")

            default_val = f.get("default", "")
            placeholder = f.get("placeholder", "")

            normalized_fields.append({
                "kind": "text_input",
                "channels": channels,
                "aliasBase": alias_base,
                "defaultValue": default_val,
                "placeholder": placeholder,
            })

        else:
            # unknown type, skip
            pass

    GLOBAL_PARAM_NAME_JS = json.dumps(global_param_name)
    DATA_URL_JS = json.dumps(data_url)
    FIELDS_JS = json.dumps(normalized_fields)
    MAPBOX_TOKEN_JS = json.dumps(mapbox_token)

    template_src = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet"/>
  <link rel="stylesheet" href="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-geocoder/v5.0.0/mapbox-gl-geocoder.css" type="text/css"/>
  <style>
    :root {
      --bg: #121212;
      --text: #eeeeee;
      --border: #333333;
      --input-bg: #1b1b1b;
      --input-hover: #2a2a2a;
      --primary: #e8ff59;
      --primary-dark: #d4eb45;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 20px;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                   "Helvetica Neue", Arial, sans-serif;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: flex-start;
      min-height: 100vh;
    }
    .form-wrapper {
      width: 90vw;
      max-width: 480px;
      display: flex;
      flex-direction: column;
      gap: 20px;
      padding-bottom: 96px;
    }
    .form-field {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    label {
      font-size: 14px;
      font-weight: 500;
      color: var(--text);
      text-transform: capitalize;
    }
    select,
    input {
      width: 100%;
      font-size: 15px;
      padding: 10px 12px;
      border-radius: 8px;
      border: 1px solid var(--border);
      background: var(--input-bg);
      color: var(--text);
      transition: all 0.2s ease;
      outline: none;
    }
    select:hover,
    input:hover {
      background: var(--input-hover);
      border-color: #444444;
    }
    select:focus,
    input:focus {
      border-color: var(--primary);
    }
    .submit-bar-wrapper {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;
      background: linear-gradient(
        to top,
        rgba(18,18,18,0.9) 0%,
        rgba(18,18,18,0.6) 60%,
        rgba(18,18,18,0) 100%
      );
      padding: 16px 16px calc(16px + env(safe-area-inset-bottom));
      display: flex;
      justify-content: center;
      pointer-events: none;
      z-index: 9999;
    }
    .submit-btn {
      width: 100%;
      max-width: 480px;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      font-size: 15px;
      font-weight: 600;
      line-height: 1;
      font-family: inherit;
      background: var(--primary);
      color: #000;
      border: none;
      border-radius: 8px;
      padding: 12px 16px;
      cursor: pointer;
      transition: all 0.15s ease;
      box-shadow:
        0 12px 32px rgba(232,255,89,0.2),
        0 2px 4px rgba(0,0,0,0.8);
      pointer-events: auto;
    }
    .submit-btn:hover:not(:disabled) {
      background: var(--primary-dark);
      box-shadow:
        0 16px 40px rgba(232,255,89,0.28),
        0 3px 6px rgba(0,0,0,0.8);
      transform: translateY(-1px);
    }
    .submit-btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
      box-shadow:
        0 6px 16px rgba(0,0,0,0.6),
        0 1px 2px rgba(0,0,0,0.8);
    }
    .map-container {
      width: 100%;
      border-radius: 8px;
      border: 1px solid var(--border);
      overflow: hidden;
    }
    .map-container .mapboxgl-canvas-container,
    .map-container .mapboxgl-canvas {
      width: 100% !important;
    }
    #loaderOverlay {
      display: flex;
      flex-direction: column;
      gap: 12px;
      align-items: center;
      justify-content: center;
      color: #aaa;
      font-size: 14px;
      min-height: 200px;
    }
    .spinner {
      width: 28px;
      height: 28px;
      border-radius: 50%;
      border: 4px solid rgba(232,255,89,0.15);
      border-top-color: var(--primary);
      animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    #formContent {
      display: none;
      flex-direction: column;
      gap: 20px;
    }
    #formContent.loaded { display: flex; }
    #loaderOverlay.hidden { display: none; }
    .error-message {
      color: #ff6b6b;
      padding: 12px;
      border-radius: 8px;
      border: 1px solid #ff6b6b;
      background: rgba(255,107,107,0.1);
      font-size: 13px;
      line-height: 1.4;
    }
    /* flatpickr dark theme */
    .flatpickr-calendar {
      background: var(--input-bg) !important;
      border: 1px solid var(--border) !important;
      border-radius: 8px !important;
      box-shadow: 0 16px 32px rgba(0,0,0,0.8) !important;
      color: var(--text) !important;
    }
    .flatpickr-months .flatpickr-prev-month svg,
    .flatpickr-months .flatpickr-next-month svg {
      fill: #fff !important;
      stroke: none !important;
    }
    .flatpickr-current-month,
    .flatpickr-current-month input.cur-year {
      color: var(--text) !important;
      font-size: 13px !important;
      font-weight: 500 !important;
    }
    .flatpickr-current-month .numInputWrapper span.arrowUp:after {
      border-bottom-color: #fff !important;
    }
    .flatpickr-current-month .numInputWrapper span.arrowDown:after {
      border-top-color: #fff !important;
    }
    .flatpickr-weekday {
      color: #888 !important;
      font-size: 11px !important;
      font-weight: 400 !important;
    }
    .flatpickr-day {
      background: transparent !important;
      border: 0 !important;
      color: var(--text) !important;
      font-weight: 500;
      border-radius: 9999px !important;
      outline: none !important;
    }
    .flatpickr-day.disabled,
    .flatpickr-day.notAllowed,
    .flatpickr-day.prevMonthDay,
    .flatpickr-day.nextMonthDay {
      color: #444 !important;
      background: transparent !important;
      cursor: default !important;
    }
    .flatpickr-day:hover,
    .flatpickr-day.hover {
      background: var(--input-hover) !important;
      color: var(--text) !important;
    }
    .flatpickr-day.inRange {
      background: rgba(232,255,89,0.2) !important;
      color: var(--text) !important;
    }
    .flatpickr-day.startRange,
    .flatpickr-day.endRange,
    .flatpickr-day.selected {
      background: var(--primary) !important;
      color: #000 !important;
    }
    .flatpickr-day.today:not(.selected):not(.startRange):not(.endRange) {
      border: 1px solid var(--primary) !important;
      color: var(--primary) !important;
      background: transparent !important;
      box-shadow: 0 0 8px rgba(232,255,89,0.4);
    }
    /* Mapbox Geocoder dark theme */
    .mapboxgl-ctrl-geocoder {
      min-width: 180px;
      background: var(--input-bg) !important;
      border-radius: 6px !important;
      box-shadow: 0 2px 8px rgba(0,0,0,0.6) !important;
    }
    .mapboxgl-ctrl-geocoder input[type="text"] {
      background: var(--input-bg) !important;
      color: var(--text) !important;
      border: 1px solid var(--border) !important;
      border-radius: 6px !important;
      padding: 6px 10px 6px 30px !important;
      font-size: 13px !important;
      height: 32px !important;
    }
    .mapboxgl-ctrl-geocoder input[type="text"]:focus {
      border-color: #888 !important;
      outline: none !important;
      background: var(--input-bg) !important;
      color: var(--text) !important;
    }
    .mapboxgl-ctrl-geocoder input[type="text"]::placeholder {
      color: #888 !important;
    }
    .mapboxgl-ctrl-geocoder .suggestions {
      background: var(--input-bg) !important;
      border: 1px solid var(--border) !important;
      border-radius: 8px !important;
      margin-top: 4px !important;
      box-shadow: 0 8px 24px rgba(0,0,0,0.8) !important;
    }
    .mapboxgl-ctrl-geocoder .suggestions > li > a {
      color: var(--text) !important;
      padding: 8px 12px !important;
      border-bottom: 1px solid var(--border) !important;
    }
    .mapboxgl-ctrl-geocoder .suggestions > li > a:hover,
    .mapboxgl-ctrl-geocoder .suggestions > .active > a {
      background: var(--input-hover) !important;
      color: var(--text) !important;
    }
    .mapboxgl-ctrl-geocoder .mapboxgl-ctrl-geocoder--icon {
      fill: #888 !important;
      top: 7px !important;
      left: 7px !important;
      width: 18px !important;
      height: 18px !important;
    }
    .mapboxgl-ctrl-geocoder .mapboxgl-ctrl-geocoder--icon-search {
      fill: #888 !important;
    }
    .mapboxgl-ctrl-geocoder--button {
      background: transparent !important;
      width: 26px !important;
    }
    .mapboxgl-ctrl-geocoder .mapboxgl-ctrl-geocoder--icon-close {
      margin-top: 4px !important;
      width: 16px !important;
      height: 16px !important;
      fill: #bbb !important;
    }
  </style>
</head>
<body>
  <div class="form-wrapper">
    <div id="loaderOverlay">
      <div class="spinner"></div>
      <div>Loading…</div>
    </div>

    <div id="formContent">
      {% for f in fields %}
        {% if f.kind == "select" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.col }}</label>
            <select
              id="field_{{ loop.index0 }}"
              data-kind="select"
              data-col="{{ f.col }}"
              data-channels="{{ f.channels | join(',') }}"
              data-alias-base="{{ f.aliasBase }}"
              data-default-value="{{ f.defaultValue | default('', true) }}"
            >
              <option disabled selected value="">Select {{ f.col }}…</option>
            </select>
          </div>

        {% elif f.kind == "date" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.startCol }} / {{ f.endCol }}</label>
            <input
              id="field_{{ loop.index0 }}"
              class="date-input"
              data-kind="date"
              data-start-col="{{ f.startCol }}"
              data-end-col="{{ f.endCol }}"
              data-channels="{{ f.channels | join(',') }}"
              data-alias-base="{{ f.aliasBase }}"
              data-default-value="{{ f.defaultValue | default('', true) }}"
              placeholder="Select date range…"
              readonly
            />
          </div>

        {% elif f.kind == "geo" %}
          <div class="form-field">
            <label>Region Geometry</label>
            <div
              id="field_{{ loop.index0 }}"
              class="map-container"
              style="height: {{ f.mapHeightPx }}px"
              data-kind="geo"
              data-channels="{{ f.channels | join(',') }}"
              data-alias-base="{{ f.aliasBase }}"
              data-geometry-col="{{ f.geometryCol | default('', true) }}"
              data-render="{{ 'true' if f.render else 'false' }}"
            ></div>
          </div>

        {% elif f.kind == "bounds" %}
          <div class="form-field">
            <label>Bounds</label>
            <div
              id="field_{{ loop.index0 }}"
              class="map-container"
              style="height: {{ f.mapHeightPx }}px"
              data-kind="bounds"
              data-channels="{{ f.channels | join(',') }}"
              data-alias-base="{{ f.aliasBase }}"
              data-default-bounds='{{ f.defaultBounds | tojson }}'
            ></div>
          </div>

        {% elif f.kind == "text_input" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.aliasBase }}</label>
            <input
              id="field_{{ loop.index0 }}"
              type="text"
              data-kind="text_input"
              data-channels="{{ f.channels | join(',') }}"
              data-alias-base="{{ f.aliasBase }}"
              data-default-value="{{ f.defaultValue | default('', true) }}"
              placeholder="{{ f.placeholder | default('', true) }}"
              value="{{ f.defaultValue | default('', true) }}"
            />
          </div>
        {% endif %}
      {% endfor %}
    </div>
  </div>

  <div class="submit-bar-wrapper">
    <button id="submit_btn" class="submit-btn" disabled>Submit</button>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>
  <script src="https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-geocoder/v5.0.0/mapbox-gl-geocoder.min.js"></script>

  <script type="module">
    (async () => {
      const GLOBAL_PARAM_NAME = {{ GLOBAL_PARAM_NAME_JS | safe }};
      const DATA_URL  = {{ DATA_URL_JS | safe }};
      const FIELDS    = {{ FIELDS_JS | safe }};
      const MAPBOX_TOKEN = {{ MAPBOX_TOKEN_JS | safe }};

      const $ = (selector) => {
        if (selector.startsWith('#')) {
          return document.getElementById(selector.slice(1));
        }
        return document.getElementById(selector);
      };

      // local state
      const pickers = {};
      const dateRanges = {};
      const mapInstances = {};     // idx -> mapboxgl.Map
      const textValues = {};
      const geoFeatureCollections = {}; // idx -> FeatureCollection (for "geo")

      function showForm() {
        $("#loaderOverlay").classList.add("hidden");
        $("#formContent").classList.add("loaded");
      }
      function showError(msg) {
        const loaderDiv = $("#loaderOverlay");
        if (loaderDiv) {
          loaderDiv.innerHTML = `<div class="error-message">${msg}</div>`;
        }
      }
      function iso(d) {
        if (!d) return "";
        const y  = d.getFullYear();
        const m  = String(d.getMonth() + 1).padStart(2, "0");
        const da = String(d.getDate()).padStart(2, "0");
        return `${y}-${m}-${da}`;
      }

      // ----- geometry helpers (WKB/WKT/GeoJSON -> FeatureCollection) -----

      function readFloat64LE(view, offset) { return view.getFloat64(offset, true); }
      function readUint32LE(view, offset) { return view.getUint32(offset, true); }
      function readUint8(view, offset)    { return view.getUint8(offset); }

      function wkbToGeoJSONGeom(buf) {
        if (!(buf instanceof Uint8Array)) return null;
        const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

        const byteOrder = readUint8(view, 0);
        if (byteOrder !== 1) {
          return null;
        }

        const geomType = readUint32LE(view, 1);

        function readPoint(off0) {
          const x = readFloat64LE(view, off0);
          const y = readFloat64LE(view, off0 + 8);
          return { coords: [x, y], bytes: 16 };
        }

        function readLineString(off0) {
          const numPoints = readUint32LE(view, off0);
          let off = off0 + 4;
          const coords = [];
          for (let i = 0; i < numPoints; i++) {
            const { coords: xy, bytes } = readPoint(off);
            coords.push(xy);
            off += bytes;
          }
          return { geom: { type: "LineString", coordinates: coords }, bytes: off - off0 };
        }

        function readPolygon(off0) {
          const numRings = readUint32LE(view, off0);
          let off = off0 + 4;
          const rings = [];
          for (let r = 0; r < numRings; r++) {
            const numPoints = readUint32LE(view, off);
            off += 4;
            const ringCoords = [];
            for (let i = 0; i < numPoints; i++) {
              const { coords: xy, bytes } = readPoint(off);
              ringCoords.push(xy);
              off += bytes;
            }
            rings.push(ringCoords);
          }
          return { geom: { type: "Polygon", coordinates: rings }, bytes: off - off0 };
        }

        function readMultiPolygon(off0) {
          const numPolygons = readUint32LE(view, off0);
          let off = off0 + 4;
          const polys = [];
          for (let p = 0; p < numPolygons; p++) {
            const byteOrderInner = readUint8(view, off);
            if (byteOrderInner !== 1) {
              return null;
            }
            const innerType = readUint32LE(view, off + 1);
            if (innerType !== 3) {
              return null;
            }
            const parseInnerPolygon = (o0) => {
              const numRings = readUint32LE(view, o0);
              let o = o0 + 4;
              const rings = [];
              for (let r = 0; r < numRings; r++) {
                const numPoints = readUint32LE(view, o);
                o += 4;
                const ringCoords = [];
                for (let i = 0; i < numPoints; i++) {
                  const { coords: xy, bytes } = readPoint(o);
                  ringCoords.push(xy);
                  o += bytes;
                }
                rings.push(ringCoords);
              }
              return { geom: { type: "Polygon", coordinates: rings }, bytes: o - o0 };
            };

            const { geom: polyGeom, bytes: polyBytes } = parseInnerPolygon(off + 5);
            polys.push(polyGeom.coordinates);
            off += 5 + polyBytes;
          }
          return { geom: { type: "MultiPolygon", coordinates: polys }, bytes: off - off0 };
        }

        const bodyOffset = 5;
        if (geomType === 1) {
          const { coords } = readPoint(bodyOffset);
          return { type: "Point", coordinates: coords };
        }
        if (geomType === 2) {
          const { geom } = readLineString(bodyOffset);
          return geom;
        }
        if (geomType === 3) {
          const { geom } = readPolygon(bodyOffset);
          return geom;
        }
        if (geomType === 6) {
          const { geom } = readMultiPolygon(bodyOffset);
          return geom;
        }
        return null;
      }

      function wktToGeoJSON(wktStr) {
        if (!wktStr || typeof wktStr !== "string") return null;
        const typeMatch = wktStr.match(/^\s*(\w+)\s*\(/i);
        if (!typeMatch) return null;
        const type = typeMatch[1].toUpperCase();

        function parseCoords(text) {
          return text.trim().split(/\s*,\s*/).map(pair => {
            const nums = pair.trim().split(/\s+/).map(Number);
            return [nums[0], nums[1]];
          });
        }

        if (type === "POINT") {
          const body = wktStr.replace(/^POINT\s*\(/i, "").replace(/\)\s*$/, "");
          const nums = body.trim().split(/\s+/).map(Number);
          return { type: "Point", coordinates: [nums[0], nums[1]] };
        }
        if (type === "LINESTRING") {
          const body = wktStr.replace(/^LINESTRING\s*\(/i, "").replace(/\)\s*$/, "");
          return { type: "LineString", coordinates: parseCoords(body) };
        }
        if (type === "POLYGON") {
          const body = wktStr.replace(/^POLYGON\s*\(/i, "").replace(/\)\s*$/, "");
          const rings = body.split(/\)\s*,\s*\(/).map(s => s.replace(/^\(/, "").replace(/\)$/, ""));
          return {
            type: "Polygon",
            coordinates: rings.map(r => parseCoords(r))
          };
        }
        if (type === "MULTIPOLYGON") {
          const body = wktStr.replace(/^MULTIPOLYGON\s*\(/i, "").replace(/\)\s*$/, "");
          const polys = body.split(/\)\s*,\s*\(\s*\(/).map((polyStr, idx, arr) => {
            let p = polyStr;
            if (idx === 0) p = p.replace(/^\s*\(\(/, "");
            if (idx === arr.length - 1) p = p.replace(/\)\)\s*$/, "");
            const rings = p.split(/\)\s*,\s*\(/).map(r => r.replace(/^\(/, "").replace(/\)$/, ""));
            return rings.map(ring => parseCoords(ring));
          });
          return {
            type: "MultiPolygon",
            coordinates: polys
          };
        }
        return null;
      }

      function geomValToFeature(geomVal) {
        if (!geomVal) return null;

        // binary WKB from DuckDB fallback
        if (geomVal instanceof Uint8Array) {
          const gjGeom = wkbToGeoJSONGeom(geomVal);
          if (gjGeom) {
            return { type: "Feature", geometry: gjGeom, properties: {} };
          }
          return null;
        }

        if (typeof geomVal === "string") {
          // try GeoJSON first
          try {
            const parsed = JSON.parse(geomVal);
            if (parsed.type === "Feature") return parsed;
            if (parsed.type === "FeatureCollection") {
              if (parsed.features && parsed.features.length > 0) {
                return parsed.features[0];
              }
              return null;
            }
            if (
              parsed.type === "Polygon" ||
              parsed.type === "MultiPolygon" ||
              parsed.type === "LineString" ||
              parsed.type === "MultiLineString" ||
              parsed.type === "Point"
            ) {
              return { type: "Feature", geometry: parsed, properties: {} };
            }
          } catch (_) {
            // not JSON, treat as WKT
          }

          const gjGeom2 = wktToGeoJSON(geomVal);
          if (gjGeom2) {
            return { type: "Feature", geometry: gjGeom2, properties: {} };
          }

          return null;
        }

        return null;
      }

      function featureCollectionFromList(vals) {
        const feats = [];
        for (const v of vals) {
          const f = geomValToFeature(v);
          if (f) feats.push(f);
        }
        return { type: "FeatureCollection", features: feats };
      }

      function computeBbox(fc) {
        let minX= Infinity, minY= Infinity, maxX= -Infinity, maxY= -Infinity;

        function visitCoords(coords) {
          if (!coords) return;
          if (typeof coords[0] === "number" && typeof coords[1] === "number") {
            const x = coords[0];
            const y = coords[1];
            if (x < minX) minX = x;
            if (y < minY) minY = y;
            if (x > maxX) maxX = x;
            if (y > maxY) maxY = y;
            return;
          }
          for (const c of coords) visitCoords(c);
        }

        for (const f of fc.features || []) {
          if (f && f.geometry) {
            visitCoords(f.geometry.coordinates);
          }
        }
        if (!isFinite(minX) || !isFinite(minY) || !isFinite(maxX) || !isFinite(maxY)) {
          return null;
        }
        return [minX, minY, maxX, maxY];
      }

      function drawFeatureCollectionOnMap(idx, fc, mapRef, shouldRender) {
        if (!mapRef) return;
        const sourceId = `geo-src-${idx}`;
        const lineLayerId = `geo-line-${idx}`;
        const fillLayerId = `geo-fill-${idx}`;

        function applyLayers() {
          if (mapRef.getLayer(fillLayerId)) mapRef.removeLayer(fillLayerId);
          if (mapRef.getLayer(lineLayerId)) mapRef.removeLayer(lineLayerId);
          if (mapRef.getSource(sourceId))  mapRef.removeSource(sourceId);

          const hasFeatures = fc && fc.features && fc.features.length;

          if (shouldRender && hasFeatures) {
            mapRef.addSource(sourceId, {
              type: "geojson",
              data: fc
            });

            mapRef.addLayer({
              id: fillLayerId,
              type: "fill",
              source: sourceId,
              paint: {
                "fill-color": "#e8ff59",
                "fill-opacity": 0.15
              },
              filter: ["any",
                ["==", ["geometry-type"], "Polygon"],
                ["==", ["geometry-type"], "MultiPolygon"]
              ]
            });

            mapRef.addLayer({
              id: lineLayerId,
              type: "line",
              source: sourceId,
              paint: {
                "line-color": "#e8ff59",
                "line-width": 2
              }
            });
          }

          if (hasFeatures) {
            const bbox = computeBbox(fc);
            if (bbox) {
              mapRef.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], {
                padding: 20,
                duration: 0
              });
            }
          }
        }

        if (!mapRef.isStyleLoaded()) {
          mapRef.once("load", () => {
            applyLayers();
          });
        } else {
          applyLayers();
        }
      }

      function fitMapToDefaultBounds(mapRef, defB) {
        const apply = () => {
          mapRef.fitBounds([[defB[0], defB[1]], [defB[2], defB[3]]], {
            padding: 20,
            duration: 0
          });
        };
        if (!mapRef.isStyleLoaded()) {
          mapRef.once("load", apply);
        } else {
          apply();
        }
      }

      // ----- DuckDB helpers ------------------------------------------

        function buildWhere(upToIdx) {
          const clauses = [];
          for (let i = 0; i < upToIdx; i++) {
            const spec = FIELDS[i];
            const el = $("field_" + i);
            if (!el) continue;

            if (spec.kind === "select") {
              const v = el.value;
              if (v) {
                const safe = v.replace(/'/g, "''");
                clauses.push(`${spec.col}='${safe}'`);
              }
            } else if (spec.kind === "date") {
              const rng = dateRanges[i];
              if (rng && rng.start && rng.end) {
                const safeStart = rng.start.replace(/'/g, "''");
                const safeEnd   = rng.end.replace(/'/g, "''");
                clauses.push(
                  `(${spec.endCol} >= '${safeStart}' AND ${spec.startCol} <= '${safeEnd}')`
                );
              }
            }
          }
          return clauses.length ? "WHERE " + clauses.join(" AND ") : "";
        }

      async function loadDistinct(connObj, colName, whereClause) {
          const q = [
            "SELECT DISTINCT",
            colName,
            "AS v FROM df",
            whereClause,
            "ORDER BY 1"
          ].filter(Boolean).join(" ");
        const res = await connObj.query(q);
          return res.toArray().map(r => r.v);
        }

      async function loadMinMax(connObj, startCol, endCol, whereClause) {
          const q = [
            "SELECT",
            "  MIN(" + startCol + ") AS mn,",
            "  MAX(" + endCol   + ") AS mx",
            "FROM df",
            whereClause
          ].filter(Boolean).join(" ");
        const res = await connObj.query(q);
          const row = res.toArray()[0] || {};
          let lo = row.mn ? String(row.mn).slice(0,10) : "";
          let hi = row.mx ? String(row.mx).slice(0,10) : "";
          if (lo && !hi) hi = lo;
          if (!lo && hi) lo = hi;
          return { lo, hi };
        }

      // try spatial first (WKB -> WKT), fallback to raw WKB
      async function duckdbGeometryQuery(connObj, geomCol, whereClause) {
        const wktCandidates = [
          `ST_AsText(${geomCol})`,
          `ST_AsText(ST_GeomFromWKB(${geomCol}))`,
          `ST_AsText(ST_GeomFromWKB(CAST(${geomCol} AS BLOB)))`
        ];

        for (const expr of wktCandidates) {
          const q = [
            "SELECT",
            expr + " AS g",
            "FROM df",
            whereClause 
          ].filter(Boolean).join(" ");

          try {
            const res = await connObj.query(q);
            const rows = res.toArray();
            if (rows.length > 0 && rows[0].g !== undefined) {
              return rows.map(r => r.g);
            }
          } catch (_err) {
            /* try next candidate */
          }
        }

        const rawQuery = [
          "SELECT",
          geomCol,
          "AS g FROM df",
          whereClause
        ].join(" ");

        const res2 = await connObj.query(rawQuery);
        const rows2 = res2.toArray();
        return rows2.map(r => r.g);
      }

      async function loadGeoFeatureCollection(idx, spec, connObj) {
        if (!connObj) {
          return null;
        }
        if (!spec.geometryCol) {
          return null;
        }

        const whereClause = buildWhere(idx);
        const vals = await duckdbGeometryQuery(connObj, spec.geometryCol, whereClause);
        const clean = vals.filter(v => v !== null && v !== undefined);
        if (!clean.length) {
          return null;
        }

        const fc = featureCollectionFromList(clean);
        return fc;
      }

      async function initOrUpdateGeoField(idx, spec, connObj) {
          const el = $("field_" + idx);
        if (!el) {
          return;
        }
          mapboxgl.accessToken = MAPBOX_TOKEN;

        const renderGeometry = async (mapRef) => {
          const fc = await loadGeoFeatureCollection(idx, spec, connObj);
          if (fc && fc.features && fc.features.length) {
            geoFeatureCollections[idx] = fc;
          } else {
            geoFeatureCollections[idx] = { type:"FeatureCollection", features: [] };
          }
          drawFeatureCollectionOnMap(idx, geoFeatureCollections[idx], mapRef, !!spec.render);
        };

        if (mapInstances[idx]) {
          await renderGeometry(mapInstances[idx]);
          return;
        }

        const m = new mapboxgl.Map({
          container: el.id,
          style: "mapbox://styles/mapbox/dark-v11",
          attributionControl: false,
          projection: "mercator",
          dragRotate: false,
          pitchWithRotate: false,
          touchZoomRotate: false,
          touchPitch: false,
          renderWorldCopies: false,
          maxPitch: 0
        });

        const geocoder = new MapboxGeocoder({
          accessToken: MAPBOX_TOKEN,
          mapboxgl: mapboxgl,
          placeholder: "Search places...",
          marker: false,
          collapsed: false
        });
        m.addControl(geocoder, 'top-right');

        m.on("load", async () => {
          setTimeout(async () => {
            m.resize();
            await renderGeometry(m);
          }, 100);
        });

        mapInstances[idx] = m;
      }

      async function initOrUpdateBoundsField(idx, spec) {
        const el = $("field_" + idx);
        if (!el) {
          return;
        }
        mapboxgl.accessToken = MAPBOX_TOKEN;

        // parse default bounds
          const defAttr = el.getAttribute("data-default-bounds");
        let defB;
          try {
          defB = defAttr ? JSON.parse(defAttr) : spec.defaultBounds;
          } catch (e) {
          defB = spec.defaultBounds;
          }
          if (!Array.isArray(defB) || defB.length !== 4) {
            defB = [-180, -90, 180, 90];
          }

        const fitDefault = (mapRef) => {
          fitMapToDefaultBounds(mapRef, defB);
        };

          if (mapInstances[idx]) {
          fitDefault(mapInstances[idx]);
            return;
          }

          const m = new mapboxgl.Map({
            container: el.id,
            style: "mapbox://styles/mapbox/dark-v11",
            attributionControl: false,
            projection: "mercator",
            dragRotate: false,
            pitchWithRotate: false,
            touchZoomRotate: false,
            touchPitch: false,
            renderWorldCopies: false,
            maxPitch: 0
          });

          const geocoder = new MapboxGeocoder({
            accessToken: MAPBOX_TOKEN,
            mapboxgl: mapboxgl,
            placeholder: "Search places...",
            marker: false,
            collapsed: false
          });
          m.addControl(geocoder, 'top-right');
          
        m.on("load", async () => {
          setTimeout(async () => {
              m.resize();
            fitDefault(m);
            }, 100);
          });

          mapInstances[idx] = m;
        }

        function enableSubmit() {
          const btn = $("#submit_btn");
          if (btn && btn.disabled) {
            btn.disabled = false;
          }
        }

      async function hydrateField(idx, connObj) {
          const spec = FIELDS[idx];
          const el = $("field_" + idx);
          if (!el) return;

          if (spec.kind === "select") {
          if (!connObj) { enableSubmit(); return; }
          const vals = await loadDistinct(connObj, spec.col, buildWhere(idx));
              el.innerHTML = "";
              const ph = document.createElement("option");
              ph.disabled = true;
              ph.value = "";
              ph.textContent = "Select " + spec.col + "…";
              el.appendChild(ph);

              vals.forEach(v => {
                const opt = document.createElement("option");
                opt.value = v ?? "";
                opt.textContent = v ?? "(null)";
                el.appendChild(opt);
              });

              let chosenIndex = 0;
              if (vals.length > 0) {
                if (spec.defaultValue && vals.includes(spec.defaultValue)) {
                  chosenIndex = vals.indexOf(spec.defaultValue) + 1;
                } else {
                  chosenIndex = 1;
                }
              }
              el.selectedIndex = chosenIndex;
          if (chosenIndex !== 0) enableSubmit();

          } else if (spec.kind === "date") {
          let minDate;
          let maxDate;
              let initialRange;

          if (connObj) {
            const { lo, hi } = await loadMinMax(connObj, spec.startCol, spec.endCol, buildWhere(idx));
            minDate = lo || undefined;
            maxDate = hi || undefined;

              if (spec.defaultValue && minDate && maxDate) {
                if (spec.defaultValue >= minDate && spec.defaultValue <= maxDate) {
                  initialRange = [spec.defaultValue, spec.defaultValue];
                }
              }
              if (!initialRange) {
                if (lo && hi) {
                  initialRange = [lo, hi];
                } else if (lo) {
                  initialRange = [lo];
                }
              }
          } else {
            const def = spec.defaultValue;
            if (Array.isArray(def) && def.length) {
              const start = def[0];
              const end = def[1] || def[0];
              if (start) {
                initialRange = end ? [start, end] : [start];
              }
            } else if (typeof def === "string" && def) {
              initialRange = [def, def];
            }
          }

              if (pickers[idx]) {
                const inst = pickers[idx];
                inst.set("minDate", minDate);
                inst.set("maxDate", maxDate);
                if (initialRange) {
                  inst.setDate(initialRange, true);
                } else {
                  inst.clear();
                }
                if (initialRange && initialRange.length) {
                  const startISO = initialRange[0];
                  const endISO   = initialRange[1] || initialRange[0] || "";
                  dateRanges[idx] = { start: startISO, end: endISO };
                }
                enableSubmit();
              } else {
                pickers[idx] = flatpickr(el, {
                  mode: "range",
                  dateFormat: "Y-m-d",
                  minDate,
                  maxDate,
                  defaultDate: initialRange,
                  onChange: async (selectedDates) => {
                    enableSubmit();
                    const start = selectedDates[0] ? iso(selectedDates[0]) : "";
                    const end   = selectedDates[1]
                      ? iso(selectedDates[1])
                      : (selectedDates[0] ? iso(selectedDates[0]) : "");
                    dateRanges[idx] = { start, end };
                await hydrateDownstream(idx + 1, connObj);
                  }
                });

                if (initialRange && initialRange.length) {
                  const startISO = initialRange[0];
                  const endISO   = initialRange[1] || initialRange[0] || "";
                  dateRanges[idx] = { start: startISO, end: endISO };
                }
                enableSubmit();
              }

        } else if (spec.kind === "geo") {
          await initOrUpdateGeoField(idx, spec, connObj);
          enableSubmit();

          } else if (spec.kind === "bounds") {
          await initOrUpdateBoundsField(idx, spec);
              enableSubmit();

          } else if (spec.kind === "text_input") {
              if (spec.defaultValue) {
                textValues[idx] = spec.defaultValue;
              }
              enableSubmit();
          }
        }

      async function hydrateDownstream(fromIdx, connObj) {
          for (let i = fromIdx; i < FIELDS.length; i++) {
          await hydrateField(i, connObj);
        }
      }

      try {
        // DuckDB init
        let conn = null;
        if (DATA_URL) {
          const duckdb = await import(
            "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm"
          );
          const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
          const workerCode = await (await fetch(bundle.mainWorker)).text();
          const worker = new Worker(
            URL.createObjectURL(
              new Blob([workerCode], { type:"application/javascript" })
            )
          );
          const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
          await db.instantiate(bundle.mainModule);

          // try spatial extension if available
          try {
            await db.loadExtension("spatial");
          } catch (e) {
            /* spatial extension unavailable, continue without it */
          }

          conn = await db.connect();

          // Ensure spatial extension is installed/loaded when available
          try {
            await conn.query("INSTALL spatial;");
          } catch (_installErr) {
            /* ignore install failure */
          }
          try {
            await conn.query("LOAD spatial;");
          } catch (_loadErr) {
            /* ignore load failure */
          }

          const resp = await fetch(DATA_URL);
          if (!resp.ok) {
            throw new Error(`Failed to fetch data: ${resp.status} ${resp.statusText}`);
          }
          const buf = new Uint8Array(await resp.arrayBuffer());
          await db.registerFileBuffer("data.parquet", buf);
          await conn.query(`
            CREATE OR REPLACE TABLE df AS
            SELECT * FROM read_parquet('data.parquet');
          `);
        }

        // initial hydration of all fields (in order)
        await hydrateDownstream(0, conn);
        showForm();

        // change listeners for cascading updates
        FIELDS.forEach((spec, i) => {
          if (spec.kind === "select") {
            const el = $("field_" + i);
            if (!el) return;
            el.addEventListener("change", async () => {
              enableSubmit();
              await hydrateDownstream(i + 1, conn);
            });
          } else if (spec.kind === "text_input") {
            const el = $("field_" + i);
            if (!el) return;
            el.addEventListener("input", () => {
              textValues[i] = el.value;
              enableSubmit();
            });
          }
        });

        function makeSnapshot() {
          const globalMerged = {};
          const messages = [];

          FIELDS.forEach((spec, i) => {
            const el = $("field_" + i);

            if (spec.kind === "select") {
              const val = el ? (el.value || "") : "";
              globalMerged[spec.aliasBase] = val;
              spec.channels.forEach(ch => {
                messages.push({ channel: ch, payload: val });
              });

            } else if (spec.kind === "date") {
              const inst = pickers[i];
              const sel = inst ? inst.selectedDates : [];
              const start = sel[0] ? iso(sel[0]) : "";
              const end   = sel[1]
                ? iso(sel[1])
                : (sel[0] ? iso(sel[0]) : "");

              if (spec.channels.length === 2) {
                const [chStart, chEnd] = spec.channels;
                if (chStart) globalMerged[chStart] = start;
                if (chEnd)   globalMerged[chEnd]   = end;
              } else if (spec.channels.length === 1) {
                const only = spec.channels[0];
                globalMerged[only + "_start"] = start;
                globalMerged[only + "_end"]   = end;
              } else {
                globalMerged[spec.aliasBase + "_start"] = start;
                globalMerged[spec.aliasBase + "_end"]   = end;
              }

              if (spec.channels.length === 2) {
                const [chStart, chEnd] = spec.channels;
                if (chStart) messages.push({ channel: chStart, payload: start });
                if (chEnd)   messages.push({ channel: chEnd,   payload: end });
              } else if (spec.channels.length === 1) {
                const ch = spec.channels[0];
                messages.push({
                  channel: ch,
                  payload: { start, end }
                });
              }

            } else if (spec.kind === "geo") {
              if (spec.render) {
                const fc = geoFeatureCollections[i] ||
                  { type: "FeatureCollection", features: [] };
                globalMerged[spec.aliasBase] = fc;
                spec.channels.forEach(ch => {
                  messages.push({ channel: ch, payload: fc });
                });
              } else {
                const m = mapInstances[i];
                let arr = [-180, -90, 180, 90];
                if (m && m.getBounds) {
                  try {
                    const b = m.getBounds();
                    arr = [
                      b.getWest(),
                      b.getSouth(),
                      b.getEast(),
                      b.getNorth()
                    ].map(v => Number(v.toFixed(6)));
                  } catch (_e) { /* ignore */ }
                } else {
                  const fc = geoFeatureCollections[i];
                  if (fc && fc.features && fc.features.length) {
                    const bbox = computeBbox(fc);
                    if (bbox) {
                      arr = bbox.map(v => Number(v.toFixed(6)));
                    }
                  }
                }
                globalMerged[spec.aliasBase] = arr;
                spec.channels.forEach(ch => {
                  messages.push({ channel: ch, payload: arr });
                });
              }

            } else if (spec.kind === "bounds") {
              // send the current viewport bbox
              const m = mapInstances[i];
              let arr = spec.defaultBounds || [-180, -90, 180, 90];
              if (m && m.getBounds) {
                try {
                  const b = m.getBounds();
                  arr = [
                    b.getWest(),
                    b.getSouth(),
                    b.getEast(),
                    b.getNorth()
                  ].map(v => Number(v.toFixed(6)));
                } catch (_e) { /* ignore */ }
              }
              globalMerged[spec.aliasBase] = arr;
              spec.channels.forEach(ch => {
                messages.push({ channel: ch, payload: arr });
              });

            } else if (spec.kind === "text_input") {
              const val = textValues[i] || "";
              globalMerged[spec.aliasBase] = val;
              spec.channels.forEach(ch => {
                messages.push({ channel: ch, payload: val });
              });
            }
          });

          return { globalMerged, messages };
        }

        const submitBtn = $("#submit_btn");
        if (submitBtn) {
          submitBtn.addEventListener("click", () => {
            if (submitBtn.disabled) return;

            const { globalMerged, messages } = makeSnapshot();

            if (GLOBAL_PARAM_NAME) {
              window.parent.postMessage({
                type: "hierarchical_form_submit",
                payload: globalMerged,
                origin: "hierarchical_form",
                parameter: GLOBAL_PARAM_NAME,
                ts: Date.now()
              }, "*");
            } else {
              messages.forEach(({ channel, payload }) => {
                if (!channel) return;
                window.parent.postMessage({
                  type: "hierarchical_form_submit",
                  payload,
                  origin: "hierarchical_form",
                  parameter: channel,
                  ts: Date.now()
                }, "*");
              });
            }
          });
        }

      } catch (err) {
        showError(`Error: ${err.message}`);
        console.error(err);
      }
    })();
  </script>
</body>
</html>
"""

    rendered_html = jinja2.Template(template_src).render(
        fields=normalized_fields,
        GLOBAL_PARAM_NAME_JS=GLOBAL_PARAM_NAME_JS,
        DATA_URL_JS=DATA_URL_JS,
        FIELDS_JS=FIELDS_JS,
        MAPBOX_TOKEN_JS=MAPBOX_TOKEN_JS,
    )

    return common.html_to_obj(rendered_html)
