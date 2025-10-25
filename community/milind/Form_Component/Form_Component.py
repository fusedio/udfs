common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    parameter: str = "form",
    data_url: str = "https://udf.ai/fsh_3FY8CeL0kaeIyu7f8X013F/run?dtype_out_raster=png&dtype_out_vector=parquet",
    columns: str = "BOROUGH,NEIGHBORHOOD,BLOCK, YEAR_BUILT",
):
    import json

    # turn "A,B,C" -> ["A","B","C"], drop empties/whitespace
    col_list = [c.strip() for c in columns.split(",") if c.strip()]

    PARAM_JS = json.dumps(parameter)
    DATA_URL_JS = json.dumps(data_url)
    COLS_JS = json.dumps(col_list)

    html = """<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {{
    --bg: #121212;
    --text: #eee;
    --text-muted: #999;
    --border: #333;
    --input-bg: #1b1b1b;
    --input-hover: #2a2a2a;
    --primary: #e8ff59;
    --primary-dim: rgba(232, 255, 89, 0.1);
  }}
  * {{
    box-sizing: border-box;
  }}
  html, body {{
    height:100vh; margin:0; padding:0;
    background:var(--bg); color:var(--text);
    font-family:system-ui,-apple-system,sans-serif;
    display:flex; flex-direction:column; align-items:center; justify-content:center;
    gap:min(4vh,24px);
  }}

  .form-wrapper {{
    width:90vw;
    max-width:480px;
    display:flex;
    flex-direction:column;
    gap:min(4vh,24px);
  }}

  .dynamic-fields {{
    display:flex;
    flex-direction:column;
    gap:min(4vh,24px);
  }}

  .field-block {{
    display:flex;
    flex-direction:column;
    gap:min(2vh,12px);
  }}

  .field-label {{
    color:#ddd;
    font-size:min(17vh,17px);
    line-height:1.2;
    word-break:break-word;
  }}

  .select-wrapper {{
    position: relative;
    width: 100%;
  }}
  .select-wrapper::after {{
    content: '';
    position: absolute;
    right: min(10vh,10px);
    top: 50%;
    transform: translateY(-50%);
    width: 0;
    height: 0;
    border-left: min(5vh,5px) solid transparent;
    border-right: min(5vh,5px) solid transparent;
    border-top: min(5vh,5px) solid var(--text-muted);
    pointer-events: none;
    transition: border-top-color 150ms ease;
  }}

  select {{
    width: 100%;
    font-size: min(15vh,15px);
    padding: min(7.5vh,7.5px) min(15vh,15px) min(7.5vh,7.5px) min(10vh,10px);
    border: 1px solid var(--border);
    border-radius: min(7.5vh,7.5px);
    background: var(--input-bg);
    color: var(--text);
    outline: none;
    cursor: pointer;
    transition: all 150ms ease;
    appearance: none;
    -webkit-appearance: none;
    -moz-appearance: none;
    box-shadow:
      1px 1px 0px 0px rgba(0,0,0,0.3),
      0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  select:hover {{
    background: var(--input-hover);
    border-color: #444;
    box-shadow:
      2px 2px 0px 0px rgba(0,0,0,0.3),
      0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  .select-wrapper:hover::after {{
    border-top-color: var(--primary);
  }}
  select:focus {{
    border-color: var(--primary);
    background: var(--input-hover);
    box-shadow:
      0 0 0 2px var(--primary-dim),
      1px 1px 0px 0px rgba(0,0,0,0.3);
  }}
  select:focus-visible {{
    outline: 2px solid var(--primary);
    outline-offset: -2px;
  }}
  .select-wrapper:has(select:focus)::after {{
    border-top-color: var(--primary);
  }}
  select option {{
    background: var(--input-bg);
    color: var(--text);
    padding: 0.5rem;
  }}
  select option:disabled {{
    color: var(--text-muted);
    font-style: italic;
  }}
  select option:checked {{
    background: var(--primary-dim);
  }}

  .submit-btn {{
    width:100%;
    font-size:min(18vh,18px);
    line-height:1.2;
    font-weight:600;
    padding:min(10vh,10px) min(15vh,15px);
    color:#000;
    background:var(--primary);
    border:0;
    border-radius:min(7.5vh,7.5px);
    cursor:pointer;
    box-shadow:
      0 4px 20px rgba(232,255,89,0.4),
      0 0 30px rgba(232,255,89,0.2) inset;
    text-align:center;
  }}
  .submit-btn:active {{
    transform:scale(0.99);
  }}

  #status {{
    font-size:min(15vh,15px);
    line-height:1.4;
    color:var(--text-muted);
    text-align:center;
  }}

  #errorBox {{
    font-size:min(13vh,13px);
    line-height:1.4;
    color:#ff6b6b;
    text-align:center;
    white-space:pre-wrap;
  }}
</style>

<div class="form-wrapper">

  <div id="status">Loading…</div>
  <div id="errorBox" style="display:none;"></div>

  <!-- dynamic dropdown container -->
  <div id="dynamicFields" class="dynamic-fields"></div>

  <button id="submit_btn" class="submit-btn" disabled>Submit</button>
</div>

<script type="module">
(async () => {{

  // --------------------------------------------------
  // injected config from python
  // --------------------------------------------------
  const PARAMETER = {PARAM_JS};
  const DATA_URL  = {DATA_URL_JS};
  const COLS      = {COLS_JS}; // e.g. ["BOROUGH","NEIGHBORHOOD","BLOCK"]

  // --------------------------------------------------
  // DOM helpers / state
  // --------------------------------------------------
  const $ = (id) => document.getElementById(id);

  function setStatus(msg) {{
    $("status").textContent = msg;
  }}

  function showError(msg) {{
    const box = $("errorBox");
    box.textContent = msg;
    box.style.display = "block";
  }}

  function hideError() {{
    $("errorBox").style.display = "none";
  }}

  // build one dropdown block for column `colName` at level `idx`
  function createFieldBlock(colName, idx) {{
    const wrapper = document.createElement("div");
    wrapper.className = "field-block";
    wrapper.innerHTML = `
      <div class="field-label">${{colName}}</div>
      <div class="select-wrapper">
        <select id="select_${{idx}}" aria-label="${{colName}}"></select>
      </div>
    `;
    return wrapper;
  }}

  // clear <select> + placeholder
  function clearSelect(el, placeholderText) {{
    el.innerHTML = "";
    const ph = document.createElement("option");
    ph.textContent = placeholderText || "Select…";
    ph.disabled = true;
    ph.selected = true;
    ph.value = "";
    el.appendChild(ph);
  }}

  // add options
  function appendOptions(el, values) {{
    for (const v of values) {{
      const opt = document.createElement("option");
      opt.value = (v === null || v === undefined) ? "" : String(v);
      opt.textContent = (v === null || v === undefined) ? "(null)" : String(v);
      el.appendChild(opt);
    }}
  }}

  // auto-pick first non-placeholder option (index 1)
  // returns that value or ""
  function autoSelectFirstAndGetValue(selectEl) {{
    if (selectEl.options.length > 1) {{
      selectEl.selectedIndex = 1;
      return selectEl.options[1].value;
    }}
    return "";
  }}

  // --------------------------------------------------
  // DuckDB WASM init/load
  // --------------------------------------------------
  let conn = null;

  async function initDuckDB() {{
    try {{
      setStatus("Initializing DuckDB…");

      const duckdb = await import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm");
      const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

      const workerCode = await (await fetch(bundle.mainWorker)).text();
      const worker = new Worker(
        URL.createObjectURL(
          new Blob([workerCode], {{ type:"application/javascript" }})
        )
      );

      const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
      await db.instantiate(bundle.mainModule);
      conn = await db.connect();

      setStatus("Downloading data…");
      const resp = await fetch(DATA_URL);
      const buf = await resp.arrayBuffer();
      const bytes = new Uint8Array(buf);

      await db.registerFileBuffer("data.parquet", bytes);

      setStatus("Loading table…");
      await conn.query(`
        CREATE OR REPLACE TABLE df AS
        SELECT * FROM read_parquet('data.parquet');
      `);

      setStatus("Ready");
    }} catch (err) {{
      console.error(err);
      setStatus("Init error");
      showError(err && err.message ? err.message : String(err));
      throw err;
    }}
  }}

  // --------------------------------------------------
  // Query helpers (hierarchy)
  // --------------------------------------------------

  // builds WHERE clause for level `lvl`
  // includes equality constraints from all previous levels
  function buildWhereForLevel(lvl) {{
    const clauses = [];
    for (let i = 0; i < lvl; i++) {{
      const prevSelEl = document.getElementById(`select_${{i}}`);
      const prevVal = prevSelEl ? (prevSelEl.value || "") : "";
      if (!prevVal) {{
        // no selection at an earlier level -> skip that filter
        continue;
      }}
      const colName = COLS[i];
      const lit = "'" + prevVal.replace(/'/g, "''") + "'";
      clauses.push(colName + " = " + lit);
    }}
    if (!clauses.length) {{
      return "";
    }}
    return "WHERE " + clauses.join(" AND ");
  }}

  // run DISTINCT query for col at `lvl`, then populate its <select>
  async function loadLevel(lvl) {{
    const colName = COLS[lvl];
    const selEl   = document.getElementById(`select_${{lvl}}`);

    clearSelect(selEl, "Select " + colName + "…");

    const whereClause = buildWhereForLevel(lvl);

    const q = [
      "SELECT DISTINCT " + colName + " AS v",
      "FROM df",
      whereClause,
      "ORDER BY 1"
    ].filter(Boolean).join("\\n");

    const res = await conn.query(q);
    const vals = res.toArray().map(row => row.v);

    appendOptions(selEl, vals);

    const chosen = autoSelectFirstAndGetValue(selEl);

    // cascade to next level if we chose something and next level exists
    if (chosen && lvl + 1 < COLS.length) {{
      $("submit_btn").disabled = false;
      await loadLevel(lvl + 1);
    }} else if (chosen) {{
      $("submit_btn").disabled = false;
    }}
  }}

  // on manual change of select at `lvl`:
  // 1. enable submit
  // 2. wipe deeper selects
  // 3. repopulate next level
  async function onLevelChange(lvl) {{
    $("submit_btn").disabled = false;

    // wipe deeper selects
    for (let deeper = lvl + 1; deeper < COLS.length; deeper++) {{
      const deeperEl = document.getElementById(`select_${{deeper}}`);
      if (!deeperEl) continue;
      clearSelect(deeperEl, "Select " + COLS[deeper] + "…");
    }}

    // cascade repop from next level
    if (lvl + 1 < COLS.length) {{
      await loadLevel(lvl + 1);
    }}
  }}

  // --------------------------------------------------
  // submit
  // --------------------------------------------------
  function postSelection() {{
    // build payload = {{ colName: selectedValue, ... }}
    const payload = {{}};
    for (let i = 0; i < COLS.length; i++) {{
      const c = COLS[i];
      const selEl = document.getElementById(`select_${{i}}`);
      const val = selEl ? (selEl.value || "") : "";
      payload[c] = val;
    }}

    window.parent.postMessage({{
      type: "hierarchical_form_submit",
      payload,
      origin: "hierarchical_form",
      parameter: PARAMETER,
      ts: Date.now()
    }}, "*");
  }}

  // --------------------------------------------------
  // boot
  // --------------------------------------------------
  try {{
    hideError();

    // 1. Build the dropdown DOMs
    const fieldsRoot = $("dynamicFields");
    fieldsRoot.innerHTML = "";
    COLS.forEach((colName, idx) => {{
      const block = createFieldBlock(colName, idx);
      fieldsRoot.appendChild(block);
    }});

    // 2. Init DuckDB + df
    await initDuckDB();

    // 3. Initial cascade: load first level (will auto-select and recurse)
    if (COLS.length > 0) {{
      await loadLevel(0);
    }}

    // 4. Attach change listeners
    COLS.forEach((_, idx) => {{
      const selEl = document.getElementById(`select_${{idx}}`);
      selEl.addEventListener("change", async () => {{
        await onLevelChange(idx);
      }});
    }});

    // 5. Submit listener
    $("submit_btn").addEventListener("click", () => {{
      postSelection();
    }});

  }} catch (err) {{
    console.error(err);
    showError(err && err.message ? err.message : String(err));
  }}

}})();
</script>
""".format(
        PARAM_JS=PARAM_JS,
        DATA_URL_JS=DATA_URL_JS,
        COLS_JS=COLS_JS,
    )

    return common.html_to_obj(html)
