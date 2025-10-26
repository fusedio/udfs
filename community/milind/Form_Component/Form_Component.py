common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    config: str = """{
        "parameter_name": "form",
        "filter": [
            {
                "column": ["start_date", "end_date"],
                "type": "daterange"
            },
            {
                "column": "mission",
                "type": "selectbox"
            },
            {
                "column": "product_name",
                "type": "selectbox"
            },
            {
                "column": "prefix",
                "type": "selectbox"
            }

        ],
        "data_url": "https://unstable.udf.ai/fsh_aotlErnaYWdIlKcGg6huq/run?dtype_out_raster=png&dtype_out_vector=parquet"
    }"""
):
    import json
    import jinja2

    cfg = json.loads(config)

    # pull top-level config
    parameter_name = cfg.get("parameter_name", "form")
    data_url = cfg.get("data_url")

    # map cfg["filter"] -> field_specs used by template/JS
    field_specs = []
    for f in cfg.get("filter", []):
        f_type = f.get("type")
        if f_type == "selectbox":
            col = f.get("column")
            field_specs.append({
                "type": "select",
                "col": col,
                "alias": col,  # output key
                "default": f.get("default"),  # may be None
            })
        elif f_type == "daterange":
            cols = f.get("column", [])
            if isinstance(cols, list) and len(cols) == 2:
                start_col, end_col = cols
            else:
                start_col, end_col = "start_date", "end_date"
            field_specs.append({
                "type": "date_filtered",
                "start": start_col,
                "end": end_col,
                "alias": start_col,          # output key base
                "default": f.get("default"), # seed date if inside range
            })
        else:
            # ignore unknown types for now
            pass

    # normalize alias defaults just in case
    for f in field_specs:
        t = f.get("type")
        if t == "select":
            f.setdefault("alias", f.get("col", "value"))
        elif t == "date":
            f.setdefault("alias", "date")
        elif t == "date_filtered":
            f.setdefault("alias", f.get("start", "date"))

    PARAMETER_NAME_JS = json.dumps(parameter_name)
    DATA_URL_JS = json.dumps(data_url)
    FIELDS_JS = json.dumps(field_specs)

    template_src = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css"/>

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
      justify-content: center;
      min-height: 100vh;
    }

    .form-wrapper {
      width: 90vw;
      max-width: 480px;
      display: flex;
      flex-direction: column;
      gap: 20px;
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

    .submit-btn {
      background: var(--primary);
      color: #000000;
      font-size: 15px;
      font-weight: 600;
      border: none;
      border-radius: 8px;
      padding: 12px 24px;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-top: 10px;
    }

    .submit-btn:hover:not(:disabled) {
      background: var(--primary-dark);
      transform: translateY(-1px);
    }

    .submit-btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
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

    /* flatpickr minimal dark */
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
        {% if f.type == "select" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.col }}</label>
            <select
              id="field_{{ loop.index0 }}"
              data-type="select"
              data-col="{{ f.col }}"
              data-alias="{{ f.alias }}"
              data-default="{{ f.default | default('', true) }}"
            >
              <option disabled selected value="">Select {{ f.col }}…</option>
            </select>
          </div>

        {% elif f.type == "date" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.alias }}</label>
            <input
              id="field_{{ loop.index0 }}"
              class="date-input"
              data-type="date"
              data-alias="{{ f.alias }}"
              data-default="{{ f.default | default('', true) }}"
              placeholder="Select date range…"
              readonly
            />
          </div>

        {% elif f.type == "date_filtered" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.alias }}</label>
            <input
              id="field_{{ loop.index0 }}"
              class="date-input"
              data-type="date_filtered"
              data-start-col="{{ f.start }}"
              data-end-col="{{ f.end }}"
              data-alias="{{ f.alias }}"
              data-default="{{ f.default | default('', true) }}"
              placeholder="Select date range…"
              readonly
            />
          </div>
        {% endif %}
      {% endfor %}

      <button id="submit_btn" class="submit-btn" disabled>Submit</button>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
  <script type="module">
    (async () => {
      const PARAMETER_NAME = {{ PARAMETER_NAME_JS | safe }};
      const DATA_URL  = {{ DATA_URL_JS | safe }};
      const FIELDS    = {{ FIELDS_JS | safe }};

      const $ = (id) => document.getElementById(id);

      // hold flatpickr instances for each field index
      const pickers = {};
      // store chosen ranges for date_filtered fields so we can include them in WHERE
      // shape: { [idx]: { start:"YYYY-MM-DD", end:"YYYY-MM-DD" } }
      const dateFilterState = {};

      function showForm() {
        $("loaderOverlay").classList.add("hidden");
        $("formContent").classList.add("loaded");
      }

      function iso(d) {
        if (!d) return "";
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, "0");
        const da = String(d.getDate()).padStart(2, "0");
        return `${y}-${m}-${da}`;
      }

      // --- DuckDB init ---
      let conn;
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
      conn = await db.connect();

      const resp = await fetch(DATA_URL);
      const buf = new Uint8Array(await resp.arrayBuffer());
      await db.registerFileBuffer("data.parquet", buf);

      await conn.query(`
        CREATE OR REPLACE TABLE df AS
        SELECT * FROM read_parquet('data.parquet');
      `);

      // --- WHERE builder (includes date_filtered ranges)
      function buildWhere(upToIdx) {
        const clauses = [];

        for (let i = 0; i < upToIdx; i++) {
          const spec = FIELDS[i];

          if (spec.type === "select") {
            const el = $("field_" + i);
            if (!el) continue;
            const val = el.value;
            if (!val) continue;
            const safe = val.replace(/'/g, "''");
            clauses.push(`${spec.col}='${safe}'`);
          }

          if (spec.type === "date_filtered") {
            const rng = dateFilterState[i];
            if (!rng) continue;

            const startCol = spec.start;
            const endCol   = spec.end;
            const startVal = rng.start;
            const endVal   = rng.end;

            if (startVal && endVal) {
              const safeStart = startVal.replace(/'/g, "''");
              const safeEnd   = endVal.replace(/'/g, "''");
              // overlap
              clauses.push(
                `(${endCol} >= '${safeStart}' AND ${startCol} <= '${safeEnd}')`
              );
            }
          }

          // plain "date" does not contribute to WHERE
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

      // populate a single field, respecting defaults
      async function populateField(idx) {
        const spec = FIELDS[idx];
        const el = $("field_" + idx);
        if (!el) return;

        if (spec.type === "select") {
          // load distinct values given all filters ABOVE this field
          const vals = await loadDistinct(spec.col, buildWhere(idx));

          // reset dropdown
          el.innerHTML = "";
          const ph = document.createElement("option");
          ph.disabled = true;
          ph.value = "";
          ph.textContent = "Select " + spec.col + "…";
          el.appendChild(ph);

          // append options
          vals.forEach(v => {
            const opt = document.createElement("option");
            opt.value = v ?? "";
            opt.textContent = v ?? "(null)";
            el.appendChild(opt);
          });

          // choose default if possible
          let chosenIndex = 0;
          if (vals.length > 0) {
            if (spec.default && vals.includes(spec.default)) {
              // find the index (vals index +1 because of placeholder)
              const matchIdx = vals.indexOf(spec.default);
              chosenIndex = matchIdx + 1;
            } else {
              chosenIndex = 1; // first actual option
            }
          }

          el.selectedIndex = chosenIndex;
          $("submit_btn").disabled = (chosenIndex === 0);

          // No explicit cascade() here because cascade() already calls populateField in order

        } else if (spec.type === "date") {
          // plain free date range (not in WHERE)
          if (!pickers[idx]) {
            pickers[idx] = flatpickr(el, {
              mode: "range",
              dateFormat: "Y-m-d",
              onChange: () => {
                $("submit_btn").disabled = false;
                // doesn't affect WHERE, so no cascade
              }
            });
          }

          // if there's a default, try to set it as [default, default]
          if (spec.default && pickers[idx]) {
            const d = spec.default;
            pickers[idx].setDate([d, d], true);
            $("submit_btn").disabled = false;
          }

        } else if (spec.type === "date_filtered") {
          // determine allowed range given current filters
          const { lo, hi } = await loadMinMax(spec.start, spec.end, buildWhere(idx));

          const minDate = lo || undefined;
          const maxDate = hi || undefined;

          // figure out what initial range to show
          // priority:
          //   1. spec.default if inside [minDate,maxDate] -> [default, default]
          //   2. [lo, hi] if present
          //   3. [lo] if only lo
          let initialRange;
          if (spec.default && minDate && maxDate) {
            if (spec.default >= minDate && spec.default <= maxDate) {
              initialRange = [spec.default, spec.default];
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

            // sync dateFilterState
            if (initialRange && initialRange.length) {
              const startISO = initialRange[0];
              const endISO   = initialRange[1] || initialRange[0] || "";
              dateFilterState[idx] = { start: startISO, end: endISO };
            }

            $("submit_btn").disabled = false;
          } else {
            pickers[idx] = flatpickr(el, {
              mode: "range",
              dateFormat: "Y-m-d",
              minDate: minDate,
              maxDate: maxDate,
              defaultDate: initialRange,
              onChange: (selectedDates) => {
                $("submit_btn").disabled = false;

                // update dateFilterState from user selection
                const start = selectedDates[0] ? iso(selectedDates[0]) : "";
                const end   = selectedDates[1]
                  ? iso(selectedDates[1])
                  : (selectedDates[0] ? iso(selectedDates[0]) : "");
                dateFilterState[idx] = { start, end };

                // when user changes, later fields depend on this now
                cascade(idx + 1);
              }
            });

            // seed state from initialRange for WHERE usage
            if (initialRange && initialRange.length) {
              const startISO = initialRange[0];
              const endISO   = initialRange[1] || initialRange[0] || "";
              dateFilterState[idx] = { start: startISO, end: endISO };
            }

            if (minDate || maxDate) {
              $("submit_btn").disabled = false;
            }
          }
        }
      }

      // populate fields in order, so downstream sees upstream defaults
      async function cascade(fromIdx) {
        for (let i = fromIdx; i < FIELDS.length; i++) {
          await populateField(i);
        }
      }

      // initial population of all fields
      await cascade(0);
      showForm();

      // attach listeners for interactive updates
      FIELDS.forEach((spec, i) => {
        if (spec.type === "select") {
          const el = $("field_" + i);
          if (el) {
            el.addEventListener("change", async () => {
              $("submit_btn").disabled = false;
              await cascade(i + 1);
            });
          }
        }
        // date_filtered already cascades in its onChange
        // plain date does not affect WHERE so no cascade
      });

      // submit handler
      $("submit_btn").addEventListener("click", () => {
        const out = {};

        FIELDS.forEach((spec, i) => {
          const alias = spec.alias;
          const el = $("field_" + i);

          if (spec.type === "select") {
            out[alias] = el ? (el.value || "") : "";

          } else if (spec.type === "date") {
            const inst = pickers[i];
            const sel = inst ? inst.selectedDates : [];
            const start = sel[0] || null;
            const end   = sel[1] || sel[0] || null;
            out[alias + "_start"] = start ? iso(start) : "";
            out[alias + "_end"]   = end   ? iso(end)   : "";

          } else if (spec.type === "date_filtered") {
            const inst = pickers[i];
            const sel = inst ? inst.selectedDates : [];
            const start = sel[0] || null;
            const end   = sel[1] || sel[0] || null;
            out[alias + "_start"] = start ? iso(start) : "";
            out[alias + "_end"]   = end   ? iso(end)   : "";
          }
        });

        window.parent.postMessage({
          type: "hierarchical_form_submit",
          payload: out,
          origin: "hierarchical_form",
          parameter: PARAMETER_NAME,
          ts: Date.now()
        }, "*");
      });
    })();
  </script>
</body>
</html>
"""

    rendered_html = jinja2.Template(template_src).render(
        fields=field_specs,
        PARAMETER_NAME_JS=PARAMETER_NAME_JS,
        DATA_URL_JS=DATA_URL_JS,
        FIELDS_JS=FIELDS_JS,
    )

    return common.html_to_obj(rendered_html)
