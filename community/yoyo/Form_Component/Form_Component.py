common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    config: dict | str = {
        "parameter_name": "form",
        "filter": [
            {
                "column": "mission",
                "type": "selectbox",
                "default": "goes16",
                "parameter_name": "mis"
            },
            {
                "column": "product_name",
                "type": "selectbox",
                "default": "ABI",
                "parameter_name": "prod"
            },
            {
                "column": ["start_date", "end_date"],
                "type": "daterange",
                "parameter_name": ["start_meow", "end"]
            },
            {
                "type": "bounds",
                "parameter_name": "bounds",
                "default": [-125, 24, -66, 49],
                "height": 240
            },
            {
                "type": "text_input",
                "default": "Sample text",
                "parameter_name": "text"
            },
        ],
        "data_url": "https://unstable.udf.ai/fsh_aotlErnaYWdIlKcGg6huq/run?dtype_out_raster=png&dtype_out_vector=parquet"
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

        if f_type == "selectbox":
            col = f.get("column")
            default_val = f.get("default")

            if isinstance(raw_param, str) and raw_param:
                channels = [raw_param]
            else:
                channels = []

            alias_base = channels[0] if channels else col

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

        elif f_type == "bounds":
            if isinstance(raw_param, str) and raw_param:
                channels = [raw_param]
                alias_base = raw_param
            else:
                channels = []
                alias_base = "bounds"

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
            if isinstance(raw_param, str) and raw_param:
                channels = [raw_param]
                alias_base = raw_param
            else:
                channels = []
                alias_base = "text_input"

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
            pass

    GLOBAL_PARAM_NAME_JS = json.dumps(global_param_name)
    DATA_URL_JS = json.dumps(data_url)
    FIELDS_JS = json.dumps(normalized_fields)
    MAPBOX_TOKEN_JS = json.dumps(mapbox_token)

    # template
    template_src = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css"/>
  <link href="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet"/>

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

      /* so last control isn't covered by sticky submit */
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

    /* sticky footer container */
    .submit-bar-wrapper {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;

      /* transparent background so you can see page behind edges */
      background: linear-gradient(
        to top,
        rgba(18,18,18,0.9) 0%,
        rgba(18,18,18,0.6) 60%,
        rgba(18,18,18,0) 100%
      );

      padding: 16px 16px calc(16px + env(safe-area-inset-bottom));
      display: flex;
      justify-content: center;
      pointer-events: none; /* we'll re-enable on the button */
      z-index: 9999;
    }

    /* the actual button */
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

      pointer-events: auto; /* re-enable click on the button itself */
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

    /* map container */
    .bounds-map {
      width: 100%;
      border-radius: 8px;
      border: 1px solid var(--border);
      overflow: hidden;
    }
    .bounds-map .mapboxgl-canvas-container,
    .bounds-map .mapboxgl-canvas {
      width: 100% !important;
    }

    /* loader */
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

    /* flatpickr dark theme tweaks */
    .flatpickr-calendar {
      background: var(--input-bg) !important;
      border: 1px solid var(--border) !important;
      border-radius: 8px !important;
      box-shadow: 0 16px 32px rgba(0,0,0,0.8) !important;
      color: var(--text) !important;
    }
    .flatpickr-current-month,
    .flatpickr-current-month input.cur-year {
      color: var(--text) !important;
      font-size: 13px !important;
      font-weight: 500 !important;
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
      border-radius: 6px !important;
    }
    .flatpickr-day.inRange {
      background: rgba(232,255,89,0.2) !important;
      color: var(--text) !important;
      border-radius: 0 !important;
    }
    .flatpickr-day.startRange,
    .flatpickr-day.endRange,
    .flatpickr-day.selected {
      background: var(--primary) !important;
      color: #000 !important;
      border-radius: 6px !important;
    }
    .flatpickr-day.today:not(.selected):not(.startRange):not(.endRange) {
      border: 1px solid var(--primary) !important;
      color: var(--primary) !important;
      background: transparent !important;
      border-radius: 6px !important;
      box-shadow: 0 0 8px rgba(232,255,89,0.4);
    }

    @keyframes spin { to { transform: rotate(360deg); } }
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

        {% elif f.kind == "bounds" %}
          <div class="form-field">
            <label>Bounds</label>
            <div
              id="field_{{ loop.index0 }}"
              class="bounds-map"
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

  <!-- sticky footer with inset button -->
  <div class="submit-bar-wrapper">
    <button id="submit_btn" class="submit-btn" disabled>Submit</button>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
  <script src="https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js"></script>
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
      const mapInstances = {};
      const textValues = {};

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

      try {
        // DuckDB init
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
        const conn = await db.connect();

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

        // WHERE builder (bounds doesn't filter)
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

        async function loadDistinct(colName, whereClause) {
          const q = [
            "SELECT DISTINCT",
            colName,
            "AS v FROM df",
            whereClause,
            "ORDER BY 1"
          ].filter(Boolean).join(" ");
          const res = await conn.query(q);
          return res.toArray().map(r => r.v);
        }

        async function loadMinMax(startCol, endCol, whereClause) {
          const q = [
            "SELECT",
            "  MIN(" + startCol + ") AS mn,",
            "  MAX(" + endCol   + ") AS mx",
            "FROM df",
            whereClause
          ].filter(Boolean).join(" ");
          const res = await conn.query(q);
          const row = res.toArray()[0] || {};
          let lo = row.mn ? String(row.mn).slice(0,10) : "";
          let hi = row.mx ? String(row.mx).slice(0,10) : "";
          if (lo && !hi) hi = lo;
          if (!lo && hi) lo = hi;
          return { lo, hi };
        }

        // map init (first time only)
        function initOrUpdateBoundsField(idx, spec) {
          const el = $("field_" + idx);
          if (!el) return;

          mapboxgl.accessToken = MAPBOX_TOKEN;

          const defAttr = el.getAttribute("data-default-bounds");
          let defB = spec.defaultBounds;
          try {
            if (defAttr) defB = JSON.parse(defAttr);
          } catch (e) {
            /* ignore parse issue */
          }
          if (!Array.isArray(defB) || defB.length !== 4) {
            defB = [-180, -90, 180, 90];
          }

          // don't reset if already created
          if (mapInstances[idx]) {
            return;
          }

          function fitToBounds(m, arr) {
            m.fitBounds([[arr[0], arr[1]], [arr[2], arr[3]]], {
              padding: 20,
              duration: 0
            });
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

          m.on("load", () => {
            setTimeout(() => {
              m.resize();
              fitToBounds(m, defB);
            }, 100);
          });

          mapInstances[idx] = m;
        }

        // enable submit button
        function enableSubmit() {
          const btn = $("#submit_btn");
          if (btn && btn.disabled) {
            btn.disabled = false;
          }
        }

        // hydrate a field
        async function hydrateField(idx) {
          const spec = FIELDS[idx];
          const el = $("field_" + idx);
          if (!el) return;

          if (spec.kind === "select") {
              const vals = await loadDistinct(spec.col, buildWhere(idx));

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

              if (chosenIndex !== 0) {
                enableSubmit();
              }

          } else if (spec.kind === "date") {
              const { lo, hi } = await loadMinMax(spec.startCol, spec.endCol, buildWhere(idx));

              const minDate = lo || undefined;
              const maxDate = hi || undefined;

              let initialRange;
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

                    await hydrateDownstream(idx + 1);
                  }
                });

                if (initialRange && initialRange.length) {
                  const startISO = initialRange[0];
                  const endISO   = initialRange[1] || initialRange[0] || "";
                  dateRanges[idx] = { start: startISO, end: endISO };
                }

                enableSubmit();
              }

          } else if (spec.kind === "bounds") {
              initOrUpdateBoundsField(idx, spec);
              enableSubmit();

          } else if (spec.kind === "text_input") {
              if (spec.defaultValue) {
                textValues[idx] = spec.defaultValue;
              }
              enableSubmit();
          }
        }

        // hydrate downstream
        async function hydrateDownstream(fromIdx) {
          for (let i = fromIdx; i < FIELDS.length; i++) {
            await hydrateField(i);
          }
        }

        // initial load
        await hydrateDownstream(0);
        showForm();

        // listeners
        FIELDS.forEach((spec, i) => {
          if (spec.kind === "select") {
            const el = $("field_" + i);
            if (!el) return;
            el.addEventListener("change", async () => {
              enableSubmit();
              await hydrateDownstream(i + 1);
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

        // snapshot
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

            } else if (spec.kind === "bounds") {
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
                } catch (_e) {
                  /* fallback */
                }
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

        // submit
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