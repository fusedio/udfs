common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    parameter: str = "form",
    data_url: str = "https://unstable.udf.ai/fsh_aotlErnaYWdIlKcGg6huq/run?dtype_out_raster=png&dtype_out_vector=parquet",
    columns: str = '["mission","product_name","prefix","@date::start_date,end_date"]'
):
    import json
    import jinja2

    # Parse columns spec -> [{type, ...}]
    raw_cols = json.loads(columns)

    field_specs = []
    for token in raw_cols:
        if token.startswith("@date::"):
            # Ranged date that depends on DB min/max per filter chain
            _, rest = token.split("::", 1)
            start_col, end_col = [p.strip() for p in rest.split(",")]
            field_specs.append({
                "type": "date_range",
                "start_col": start_col,
                "end_col": end_col,
            })
        elif token.startswith("@date"):
            # Simple independent date range picker, no DB filtering
            # token can be "@date" or "@date columnname"
            col_name = token.replace("@date", "").strip() or "date"
            field_specs.append({
                "type": "date_simple",
                "col": col_name,
            })
        else:
            # Regular categorical dropdown sourced from DuckDB distinct()
            field_specs.append({
                "type": "categorical",
                "col": token.strip(),
            })

    PARAM_JS = json.dumps(parameter)
    DATA_URL_JS = json.dumps(data_url)
    FIELDS_JS = json.dumps(field_specs)

    template_src = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta
    name="viewport"
    content="width=device-width, initial-scale=1"
  />
  <link
    rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css"
  />
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

    /* Loading spinner */
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
      border: 4px solid rgba(232, 255, 89, 0.15);
      border-top-color: var(--primary);
      animation: spin 0.8s linear infinite;
    }

    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    #formContent {
      display: none;
      flex-direction: column;
      gap: 20px;
    }

    #formContent.loaded {
      display: flex;
    }

    #loaderOverlay.hidden {
      display: none;
    }

    /* flatpickr dark tweaks */
    .flatpickr-calendar {
      background: var(--input-bg) !important;
      border: 1px solid var(--border) !important;
      border-radius: 8px !important;
      box-shadow: 0 16px 32px rgba(0,0,0,0.8) !important;
      color: var(--text) !important;
    }
    .flatpickr-months {
      background: var(--input-bg) !important;
      border-bottom: 1px solid var(--border) !important;
    }
    .flatpickr-current-month {
      color: var(--text) !important;
      font-size: 13px !important;
      font-weight: 500 !important;
    }
    .flatpickr-current-month input.cur-year {
      color: var(--text) !important;
    }
    .flatpickr-weekdays {
      background: var(--input-bg) !important;
    }
    .flatpickr-weekday {
      color: #888 !important;
      font-size: 11px !important;
      font-weight: 400 !important;
    }
    .flatpickr-day {
      background: transparent !important;
      border: 0 !important;
      box-shadow: none !important;
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
      box-shadow: none !important;
    }
    .flatpickr-day.startRange,
    .flatpickr-day.endRange,
    .flatpickr-day.selected {
      background: var(--primary) !important;
      color: #000 !important;
      border-radius: 6px !important;
      position: relative;
      z-index: 2;
    }
    .flatpickr-day.today:not(.selected):not(.startRange):not(.endRange) {
      border: 1px solid var(--primary) !important;
      color: var(--primary) !important;
      background: transparent !important;
      border-radius: 6px !important;
      box-shadow: 0 0 8px rgba(232,255,89,0.4);
    }
    .flatpickr-months .flatpickr-prev-month,
    .flatpickr-months .flatpickr-next-month {
      fill: var(--text) !important;
      stroke: none !important;
      opacity: 0.8;
    }
    .flatpickr-months .flatpickr-prev-month:hover,
    .flatpickr-months .flatpickr-next-month:hover {
      fill: var(--primary) !important;
      opacity: 1;
    }
  </style>
</head>
<body>
  <div class="form-wrapper" id="formRoot">
    <!-- Loading overlay -->
    <div id="loaderOverlay">
      <div class="spinner"></div>
      <div>Loading…</div>
    </div>

    <!-- Form content (hidden until loaded) -->
    <div id="formContent">
      {% for f in fields %}
        {% if f.type == "categorical" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.col }}</label>
            <select
              id="field_{{ loop.index0 }}"
              data-kind="categorical"
              data-col="{{ f.col }}"
            >
              <option disabled selected value="">Select {{ f.col }}…</option>
            </select>
          </div>

        {% elif f.type == "date_range" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">Date Range</label>
            <input
              id="field_{{ loop.index0 }}"
              class="date-input"
              data-kind="date_range"
              data-start="{{ f.start_col }}"
              data-end="{{ f.end_col }}"
              placeholder="Select date range…"
              readonly
            />
          </div>

        {% elif f.type == "date_simple" %}
          <div class="form-field">
            <label for="field_{{ loop.index0 }}">{{ f.col }}</label>
            <input
              id="field_{{ loop.index0 }}"
              class="date-input"
              data-kind="date_simple"
              data-col="{{ f.col }}"
              placeholder="Select date…"
              readonly
            />
          </div>
        {% endif %}
      {% endfor %}

      <button id="submit_btn" class="submit-btn" disabled>
        Submit
      </button>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
  <script type="module">
    (async () => {
      // ---------------- config ----------------
      const PARAMETER = {{ PARAM_JS | safe }};
      const DATA_URL  = {{ DATA_URL_JS | safe }};
      const FIELDS    = {{ FIELDS_JS | safe }};

      const $ = (id) => document.getElementById(id);

      // keep per-index flatpickr handles here
      const rangePickers  = {}; // { [index]: flatpickrInstance for date_range }
      const simplePickers = {}; // { [index]: flatpickrInstance for date_simple }

      // helper: show form and hide loader once data is ready
      function showForm() {
        const loader = $("loaderOverlay");
        const form   = $("formContent");
        if (loader) loader.classList.add("hidden");
        if (form) form.classList.add("loaded");
      }

      // helper: convert Date -> "YYYY-MM-DD" using local time parts
      function iso(d) {
        if (!d) return "";
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, "0");
        const da = String(d.getDate()).padStart(2, "0");
        return `${y}-${m}-${da}`;
      }

      // ---------------- duckdb init ----------------
      let conn;
      const duckdb = await import(
        "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm"
      );
      const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

      const workerCode = await (await fetch(bundle.mainWorker)).text();
      const worker = new Worker(
        URL.createObjectURL(
          new Blob([workerCode], { type: "application/javascript" })
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

      // ---------------- helpers ----------------
      function buildWhereClause(upToIndex) {
        const parts = [];
        for (let i = 0; i < upToIndex; i++) {
          const f = FIELDS[i];
          if (f.type !== "categorical") continue;
          const el = $("field_" + i);
          if (!el) continue;
          const val = el.value;
          if (!val) continue;
          const safeVal = val.replace(/'/g, "''");
          parts.push(`${f.col}='${safeVal}'`);
        }
        return parts.length ? "WHERE " + parts.join(" AND ") : "";
      }

      async function getDistinctValues(colName, whereClause) {
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

      async function getMinMaxDates(startCol, endCol, whereClause) {
        const q = [
          "SELECT",
          `  MIN(${startCol}) AS min_date,`,
          `  MAX(${endCol})   AS max_date`,
          "FROM df",
          whereClause
        ].filter(Boolean).join(" ");
        const res = await conn.query(q);
        const row = res.toArray()[0] || {};
        let minDate = row.min_date ? String(row.min_date).slice(0,10) : "";
        let maxDate = row.max_date ? String(row.max_date).slice(0,10) : "";
        // normalize if only one side present
        if (minDate && !maxDate) maxDate = minDate;
        if (!minDate && maxDate) minDate = maxDate;
        return { minDate, maxDate };
      }

      // ---------------- population ----------------
      async function populateCategoricalField(idx) {
        const f = FIELDS[idx];
        const el = $("field_" + idx);
        if (!el) return;

        // Reset dropdown
        el.innerHTML = "";
        const ph = document.createElement("option");
        ph.disabled = true;
        ph.selected = true;
        ph.value = "";
        ph.textContent = `Select ${f.col}…`;
        el.appendChild(ph);

        // Fetch distinct values with all filters above this idx applied
        const whereClause = buildWhereClause(idx);
        let values = [];
        try {
          values = await getDistinctValues(f.col, whereClause);
        } catch (err) {
          console.error("populateCategoricalField failed:", f.col, err);
        }

        for (const v of values) {
          const opt = document.createElement("option");
          opt.value = v ?? "";
          opt.textContent = v ?? "(null)";
          el.appendChild(opt);
        }

        // autoselect first real option and enable submit
        if (values.length > 0) {
          el.selectedIndex = 1;
          $("submit_btn").disabled = false;
        }
      }

      async function populateDateRangeField(idx) {
        const f = FIELDS[idx];
        const el = $("field_" + idx);
        if (!el) return;

        const whereClause = buildWhereClause(idx);
        const { minDate, maxDate } = await getMinMaxDates(
          f.start_col,
          f.end_col,
          whereClause
        );

        // If we already created a picker for this field, update it and try
        // to preserve the user's previously chosen dates if still valid.
        const existing = rangePickers[idx];
        if (existing) {
          const prevDates = existing.selectedDates.slice();

          existing.set("minDate", minDate || undefined);
          existing.set("maxDate", maxDate || undefined);

          function inRange(d) {
            if (!d) return false;
            // compare in local date terms instead of UTC to avoid TZ drift
            const y = d.getFullYear();
            const m = String(d.getMonth() + 1).padStart(2, "0");
            const da = String(d.getDate()).padStart(2, "0");
            const localIso = `${y}-${m}-${da}`;

            if (minDate && localIso < minDate) return false;
            if (maxDate && localIso > maxDate) return false;
            return true;
          }

          if (prevDates.length && prevDates.every(inRange)) {
            existing.setDate(prevDates, true);
          } else if (minDate && maxDate) {
            existing.setDate([minDate, maxDate], true);
          } else if (minDate) {
            existing.setDate([minDate], true);
          } else {
            existing.clear();
          }

          $("submit_btn").disabled = false;
          return;
        }

        // First-time init
        const fp = flatpickr(el, {
          mode: "range",
          dateFormat: "Y-m-d",
          minDate: minDate || undefined,
          maxDate: maxDate || undefined,
          defaultDate:
            minDate && maxDate
              ? [minDate, maxDate]
              : (minDate ? [minDate] : undefined),
          onChange: () => {
            $("submit_btn").disabled = false;
          }
        });

        rangePickers[idx] = fp;

        if (minDate || maxDate) {
          $("submit_btn").disabled = false;
        }
      }

      async function populateSimpleDateField(idx) {
        const el = $("field_" + idx);
        if (!el) return;

        // simplePickers[idx] is only created once
        if (!simplePickers[idx]) {
          const fp = flatpickr(el, {
            mode: "range",
            dateFormat: "Y-m-d",
            onChange: () => {
              $("submit_btn").disabled = false;
            }
          });
          simplePickers[idx] = fp;
          $("submit_btn").disabled = false;
        }
      }

      async function cascadeFrom(startIdx) {
        for (let i = startIdx; i < FIELDS.length; i++) {
          const f = FIELDS[i];
          if (f.type === "categorical") {
            await populateCategoricalField(i);
          } else if (f.type === "date_range") {
            await populateDateRangeField(i);
          } else if (f.type === "date_simple") {
            await populateSimpleDateField(i);
          }
        }
      }

      // ---------------- wire up cascading ----------------
      // initial fill
      await cascadeFrom(0);

      // show form after first successful cascade
      showForm();

      // when a categorical changes, repopulate everything after it
      FIELDS.forEach((f, idx) => {
        if (f.type === "categorical") {
          const el = $("field_" + idx);
          if (el) {
            el.addEventListener("change", async () => {
              $("submit_btn").disabled = false;
              await cascadeFrom(idx + 1);
            });
          }
        }
      });

      // ---------------- submit handling ----------------
      $("submit_btn").addEventListener("click", () => {
        const out = {};

        FIELDS.forEach((f, idx) => {
          if (f.type === "categorical") {
            const el = $("field_" + idx);
            out[f.col] = el ? (el.value || "") : "";

          } else if (f.type === "date_range") {
            const inst = rangePickers[idx];
            if (inst) {
              const sel = inst.selectedDates || [];
              const start = sel[0] || null;
              const end   = sel[1] || sel[0] || null;

              out[f.start_col] = iso(start);
              out[f.end_col]   = iso(end);
            } else {
              out[f.start_col] = "";
              out[f.end_col]   = "";
            }

          } else if (f.type === "date_simple") {
            const inst = simplePickers[idx];
            if (inst) {
              const sel = inst.selectedDates || [];
              const start = sel[0] || null;
              const end   = sel[1] || sel[0] || null;

              out[f.col + "_start"] = iso(start);
              out[f.col + "_end"]   = iso(end);
            } else {
              out[f.col + "_start"] = "";
              out[f.col + "_end"]   = "";
            }
          }
        });

        window.parent.postMessage(
          {
            type: "hierarchical_form_submit",
            payload: out,
            origin: "hierarchical_form",
            parameter: PARAMETER,
            ts: Date.now()
          },
          "*"
        );
      });
    })();
  </script>
</body>
</html>
"""

    rendered_html = jinja2.Template(template_src).render(
        fields=field_specs,
        PARAM_JS=PARAM_JS,
        DATA_URL_JS=DATA_URL_JS,
        FIELDS_JS=FIELDS_JS,
    )

    return common.html_to_obj(rendered_html)
