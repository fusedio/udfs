common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    parameter: str = "form",
    data_url: str = "https://unstable.udf.ai/fsh_aotlErnaYWdIlKcGg6huq/run?dtype_out_raster=png&dtype_out_vector=parquet",
    columns: str = '["mission","product_name","prefix","@date::start_date,end_date"]'
):
    import ast
    import json
    import jinja2

    raw_cols = json.loads(columns)

    field_specs = []
    for token in raw_cols:
        if token.startswith("@date::"):
            _, rest = token.split("::", 1)
            start_col, end_col = [p.strip() for p in rest.split(",")]
            field_specs.append({
                "type": "date_range",
                "start_col": start_col,
                "end_col": end_col,
            })
        else:
            field_specs.append({
                "type": "categorical",
                "col": token.strip(),
            })


    # Prepare data for JavaScript
    PARAM_JS = json.dumps(parameter)
    DATA_URL_JS = json.dumps(data_url)
    FIELDS_JS = json.dumps(field_specs)

    # HTML Template
    template_src = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
  <style>
    /* ==================== Variables ==================== */
    :root {
      --bg: #121212;
      --text: #eeeeee;
      --border: #333333;
      --input-bg: #1b1b1b;
      --input-hover: #2a2a2a;
      --primary: #e8ff59;
      --primary-dark: #d4eb45;
    }

    /* ==================== Base Styles ==================== */
    * {
      box-sizing: border-box;
    }

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

    /* ==================== Form Layout ==================== */
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

    /* ==================== Input Styles ==================== */
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

    /* ==================== Submit Button ==================== */
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

    /* ==================== Flatpickr Minimal Theme ==================== */
    /* ==================== Flatpickr Dark Neon Theme ==================== */
    .flatpickr-calendar {
      background: var(--input-bg) !important;
      border: 1px solid var(--border) !important;
      border-radius: 8px !important;
      box-shadow: 0 16px 32px rgba(0, 0, 0, 0.8) !important;
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

    /* Day cell base */
    .flatpickr-day {
      background: transparent !important;
      border: 0 !important;
      box-shadow: none !important;
      color: var(--text) !important;
      font-weight: 500;
    }

    /* Disabled / out-of-month days */
    .flatpickr-day.prevMonthDay,
    .flatpickr-day.nextMonthDay,
    .flatpickr-day.disabled,
    .flatpickr-day.notAllowed {
      color: #444 !important;
      background: transparent !important;
      cursor: default !important;
    }

    /* Hover */
    .flatpickr-day.hover,
    .flatpickr-day:hover {
      background: var(--input-hover) !important;
      color: var(--text) !important;
      border-radius: 6px !important;
    }

    /* Range highlight BETWEEN start and end  */
    .flatpickr-day.inRange {
      background: color-mix(in srgb, var(--primary) 20%, transparent) !important;
      color: var(--text) !important;
      border-radius: 0 !important;
      box-shadow: none !important;
    }

    /* Start / end of range bubble */
    .flatpickr-day.startRange,
    .flatpickr-day.endRange,
    .flatpickr-day.selected {
      background: var(--primary) !important;
      color: #000 !important;
      border-radius: 6px !important;
      position: relative;
      z-index: 2;
    }

    /* Smooth pill edges connecting startRange → inRange → endRange */
    .flatpickr-day.startRange + .flatpickr-day.inRange {
      box-shadow:
        -4px 0 0 0 color-mix(in srgb, var(--primary) 20%, transparent) !important;
    }

    /* Today ring */
    .flatpickr-day.today:not(.selected):not(.startRange):not(.endRange) {
      border: 1px solid var(--primary) !important;
      color: var(--primary) !important;
      background: transparent !important;
      border-radius: 6px !important;
      box-shadow: 0 0 8px rgba(232,255,89,0.4);
    }

    /* Nav arrows */
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
  <div class="form-wrapper" id="form">
    {% for f in fields %}
      {% if f.type == "categorical" %}
        <div class="form-field">
          <label for="field_{{loop.index0}}">{{ f.col }}</label>
          <select 
            id="field_{{loop.index0}}" 
            data-kind="categorical" 
            data-col="{{ f.col }}">
            <option disabled selected value="">Select {{ f.col }}…</option>
          </select>
        </div>
      {% elif f.type == "date_range" %}
        <div class="form-field">
          <label for="field_{{loop.index0}}">Date Range</label>
          <input 
            id="field_{{loop.index0}}" 
            class="date-input" 
            data-kind="date_range"
            data-start="{{ f.start_col }}" 
            data-end="{{ f.end_col }}" 
            placeholder="Select date range…" 
            readonly />
        </div>
      {% endif %}
    {% endfor %}
    
    <button id="submit_btn" class="submit-btn" disabled>Submit</button>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
  <script type="module">
    (async () => {
      // ==================== Configuration ====================
      const PARAMETER = {{ PARAM_JS|safe }};
      const DATA_URL = {{ DATA_URL_JS|safe }};
      const FIELDS = {{ FIELDS_JS|safe }};

      const $ = (id) => document.getElementById(id);

      // ==================== DuckDB Setup ====================
      let conn;
      const duckdb = await import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm");
      const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
      
      const workerBlob = new Blob(
        [await (await fetch(bundle.mainWorker)).text()],
        { type: "application/javascript" }
      );
      const worker = new Worker(URL.createObjectURL(workerBlob));
      
      const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
      await db.instantiate(bundle.mainModule);
      conn = await db.connect();

      // Load data
      const buffer = new Uint8Array(await (await fetch(DATA_URL)).arrayBuffer());
      await db.registerFileBuffer("data.parquet", buffer);
      await conn.query("CREATE OR REPLACE TABLE df AS SELECT * FROM read_parquet('data.parquet');");

      // ==================== Helper Functions ====================
      async function getDistinctValues(column, whereClause) {
        const query = [
          "SELECT DISTINCT",
          column,
          "AS v FROM df",
          whereClause,
          "ORDER BY 1"
        ].filter(Boolean).join(" ");
        
        const result = await conn.query(query);
        return result.toArray().map(row => row.v);
      }

      function buildWhereClause(upToIndex) {
        const conditions = [];
        
        for (let i = 0; i < upToIndex; i++) {
          const field = FIELDS[i];
          if (field.type !== "categorical") continue;
          
          const element = $(`field_${i}`);
          if (element && element.value) {
            const escapedValue = element.value.replace(/'/g, "''");
            conditions.push(`${field.col}='${escapedValue}'`);
          }
        }
        
        return conditions.length ? "WHERE " + conditions.join(" AND ") : "";
      }

      // ==================== Field Population ====================
      async function populateCategoricalField(index) {
        const element = $(`field_${index}`);
        const field = FIELDS[index];
        
        element.innerHTML = `<option disabled selected value="">Select ${field.col}…</option>`;
        
        const values = await getDistinctValues(field.col, buildWhereClause(index));
        
        for (const value of values) {
          const option = document.createElement("option");
          option.value = value;
          option.textContent = value;
          element.appendChild(option);
        }
        
        if (values.length > 0) {
          element.selectedIndex = 1;
          $("submit_btn").disabled = false;
        }
      }

      async function populateDateRangeField(index) {
        const field = FIELDS[index];
        const element = $(`field_${index}`);
        
        const query = `
          SELECT 
            MIN(${field.start_col}) AS min_date,
            MAX(${field.end_col}) AS max_date 
          FROM df ${buildWhereClause(index)}
        `;
        
        const result = await conn.query(query);
        const row = result.toArray()[0] || {};
        const minDate = row.min_date?.slice(0, 10);
        const maxDate = row.max_date?.slice(0, 10);
        
        flatpickr(element, {
          mode: "range",
          dateFormat: "Y-m-d",
          minDate: minDate,
          maxDate: maxDate,
          defaultDate: [minDate, maxDate],
          onChange: () => {
            $("submit_btn").disabled = false;
          }
        });
      }

      async function cascadePopulateFields(startIndex) {
        for (let i = startIndex; i < FIELDS.length; i++) {
          if (FIELDS[i].type === "categorical") {
            await populateCategoricalField(i);
          } else {
            await populateDateRangeField(i);
          }
        }
      }

      // ==================== Event Listeners ====================
      await cascadePopulateFields(0);

      FIELDS.forEach((field, index) => {
        if (field.type === "categorical") {
          $(`field_${index}`).addEventListener("change", () => {
            cascadePopulateFields(index + 1);
          });
        }
      });

      $("submit_btn").addEventListener("click", () => {
        const payload = {};
        
        FIELDS.forEach((field, index) => {
          if (field.type === "categorical") {
            payload[field.col] = $(`field_${index}`).value;
          } else {
            const pickerInstance = flatpickr.instances.find(
              inst => inst._input.id === `field_${index}`
            );
            const [startDate, endDate] = pickerInstance.selectedDates || [];
            
            if (startDate) {
              payload[field.start_col] = startDate.toISOString().slice(0, 10);
            }
            if (endDate) {
              payload[field.end_col] = endDate.toISOString().slice(0, 10);
            }
          }
        });
        
        window.parent.postMessage(
          {
            type: "hierarchical_form_submit",
            payload: payload,
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

    # Render template
    html = jinja2.Template(template_src).render(
        fields=field_specs,
        PARAM_JS=PARAM_JS,
        DATA_URL_JS=DATA_URL_JS,
        FIELDS_JS=FIELDS_JS
    )
    
    return common.html_to_obj(html)