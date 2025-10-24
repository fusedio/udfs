common = fused.load("https://github.com/fusedio/udfs/tree/b7fe87a/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    parameter: str = "form",
    data_url: str = "https://udf.ai/fsh_3FY8CeL0kaeIyu7f8X013F/run?dtype_out_raster=png&dtype_out_vector=parquet",
    columns: str = "BOROUGH,NEIGHBORHOOD,BLOCK",
    # data_url: str = "https://udf.ai/fsh_4TNmU0UqT2qj2dqPxyVvY0/run?dtype_out_raster=png&dtype_out_vector=parquet",
    # columns: str = "property_type, room_type, bedrooms",
):

    import json

    # Parse and normalize column list
    col_list = [c.strip() for c in columns.split(",") if c.strip()]
    while len(col_list) < 3:
        col_list.append(f"COLUMN_{len(col_list)+1}")
    col0, col1, col2 = col_list[0], col_list[1], col_list[2]

    # Serialize for JS string literals
    PARAM_JS = json.dumps(parameter)
    DATA_URL_JS = json.dumps(data_url)
    COL0_JS = json.dumps(col0)
    COL1_JS = json.dumps(col1)
    COL2_JS = json.dumps(col2)

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
    gap:min(4vh, 24px);
  }}

  .form-wrapper {{
    width:90vw;
    max-width:480px;
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
    font-size:min(25vh, 25px);
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
    right: min(10vh, 10px);
    top: 50%;
    transform: translateY(-50%);
    width: 0;
    height: 0;
    border-left: min(5vh, 5px) solid transparent;
    border-right: min(5vh, 5px) solid transparent;
    border-top: min(5vh, 5px) solid var(--text-muted);
    pointer-events: none;
    transition: border-top-color 150ms ease;
  }}

  select {{
    width: 100%;
    font-size: min(15vh, 15px);
    padding: min(7.5vh, 7.5px) min(15vh, 15px) min(7.5vh, 7.5px) min(10vh, 10px);
    border: 1px solid var(--border);
    border-radius: min(7.5vh, 7.5px);
    background: var(--input-bg);
    color: var(--text);
    outline: none;
    cursor: pointer;
    transition: all 150ms ease;
    appearance: none;
    -webkit-appearance: none;
    -moz-appearance: none;
    box-shadow: 1px 1px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  select:hover {{
    background: var(--input-hover);
    border-color: #444;
    box-shadow: 2px 2px 0px 0px rgba(0,0,0,0.3), 0px 0px 2px 0px rgba(0,0,0,0.2);
  }}
  .select-wrapper:hover::after {{
    border-top-color: var(--primary);
  }}
  select:focus {{
    border-color: var(--primary);
    background: var(--input-hover);
    box-shadow: 0 0 0 2px var(--primary-dim), 1px 1px 0px 0px rgba(0,0,0,0.3);
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
    box-shadow:0 4px 20px rgba(232,255,89,0.4), 0 0 30px rgba(232,255,89,0.2) inset;
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

  <!-- Dropdown level 0 -->
  <div class="field-block">
    <div class="field-label" id="label_lvl0"></div>
    <div class="select-wrapper">
      <select id="lvl0_sel" aria-label="Level 0"></select>
    </div>
  </div>

  <!-- Dropdown level 1 -->
  <div class="field-block">
    <div class="field-label" id="label_lvl1"></div>
    <div class="select-wrapper">
      <select id="lvl1_sel" aria-label="Level 1"></select>
    </div>
  </div>

  <!-- Dropdown level 2 -->
  <div class="field-block">
    <div class="field-label" id="label_lvl2"></div>
    <div class="select-wrapper">
      <select id="lvl2_sel" aria-label="Level 2"></select>
    </div>
  </div>

  <button id="submit_btn" class="submit-btn" disabled>Submit</button>
</div>

<script type="module">
(async () => {{

  // ------------------------------------------------------------------------
  // injected config from python
  // ------------------------------------------------------------------------
  const PARAMETER = {PARAM_JS};
  const DATA_URL = {DATA_URL_JS};
  const COL0 = {COL0_JS};
  const COL1 = {COL1_JS};
  const COL2 = {COL2_JS};

  // ------------------------------------------------------------------------
  // helpers
  // ------------------------------------------------------------------------
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

  function clearSelect(el, placeholderText) {{
    el.innerHTML = "";
    const ph = document.createElement("option");
    ph.textContent = placeholderText || "Select…";
    ph.disabled = true;
    ph.selected = true;
    ph.value = "";
    el.appendChild(ph);
  }}

  function appendOptions(el, values) {{
    for (const v of values) {{
      const opt = document.createElement("option");
      opt.value = (v === null || v === undefined) ? "" : String(v);
      opt.textContent = (v === null || v === undefined) ? "(null)" : String(v);
      el.appendChild(opt);
    }}
  }}

  // ------------------------------------------------------------------------
  // duckdb wasm init
  // ------------------------------------------------------------------------
  let conn = null;

  async function initDuckDB() {{
    try {{
      setStatus("Initializing DuckDB…");

      const duckdb = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm');

      const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

      const workerCode = await (await fetch(bundle.mainWorker)).text();
      const worker = new Worker(
        URL.createObjectURL(
          new Blob([workerCode], {{ type:'application/javascript' }})
        )
      );

      const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
      await db.instantiate(bundle.mainModule);
      conn = await db.connect();

      setStatus("Downloading data…");
      const resp = await fetch(DATA_URL);
      const buf = await resp.arrayBuffer();
      const bytes = new Uint8Array(buf);

      await db.registerFileBuffer('data.parquet', bytes);

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

  // ------------------------------------------------------------------------
  // hierarchical dropdown logic
  // ------------------------------------------------------------------------

  // level 0 -> DISTINCT COL0
  async function loadLevel0() {{
    const el0 = $("lvl0_sel");
    clearSelect(el0, "Select " + COL0 + "…");

    const q = [
      "SELECT DISTINCT " + COL0 + " AS v",
      "FROM df",
      "ORDER BY 1"
    ].join("\\n");

    const res = await conn.query(q);
    const vals = res.toArray().map(row => row.v);

    appendOptions(el0, vals);

    // reset level1 + level2
    clearSelect($("lvl1_sel"), "Select " + COL1 + "…");
    clearSelect($("lvl2_sel"), "Select " + COL2 + "…");
  }}

  // level 1 -> DISTINCT COL1 WHERE COL0 = selected0
  async function loadLevel1() {{
    const v0 = $("lvl0_sel").value;
    const el1 = $("lvl1_sel");
    const el2 = $("lvl2_sel");

    clearSelect(el1, "Select " + COL1 + "…");
    clearSelect(el2, "Select " + COL2 + "…");

    if (!v0) return;

    const lit0 = "'" + v0.replace(/'/g, "''") + "'";

    const q = [
      "SELECT DISTINCT " + COL1 + " AS v",
      "FROM df",
      "WHERE " + COL0 + " = " + lit0,
      "ORDER BY 1"
    ].join("\\n");

    const res = await conn.query(q);
    const vals = res.toArray().map(row => row.v);

    appendOptions(el1, vals);
  }}

  // level 2 -> DISTINCT COL2 WHERE COL0 = selected0 AND COL1 = selected1
  async function loadLevel2() {{
    const v0 = $("lvl0_sel").value;
    const v1 = $("lvl1_sel").value;
    const el2 = $("lvl2_sel");

    clearSelect(el2, "Select " + COL2 + "…");

    if (!v0 || !v1) return;

    const lit0 = "'" + v0.replace(/'/g, "''") + "'";
    const lit1 = "'" + v1.replace(/'/g, "''") + "'";

    const q = [
      "SELECT DISTINCT " + COL2 + " AS v",
      "FROM df",
      "WHERE " + COL0 + " = " + lit0,
      "  AND " + COL1 + " = " + lit1,
      "ORDER BY 1"
    ].join("\\n");

    const res = await conn.query(q);
    const vals = res.toArray().map(row => row.v);

    appendOptions(el2, vals);
  }}

  // ------------------------------------------------------------------------
  // submit
  // ------------------------------------------------------------------------
  function postSelection() {{
    const v0 = $("lvl0_sel").value || "";
    const v1 = $("lvl1_sel").value || "";
    const v2 = $("lvl2_sel").value || "";

    // payload keys should be the actual column names
    const payload = {{
      [COL0]: v0,
      [COL1]: v1,
      [COL2]: v2,
    }};

    window.parent.postMessage({{
      type: 'hierarchical_form_submit',
      payload,
      origin: 'hierarchical_form',
      parameter: PARAMETER,
      ts: Date.now()
    }}, '*');
  }}

  // ------------------------------------------------------------------------
  // boot
  // ------------------------------------------------------------------------
  try {{
    hideError();

    // label the dropdowns using your provided column names
    $("label_lvl0").textContent = COL0;
    $("label_lvl1").textContent = COL1;
    $("label_lvl2").textContent = COL2;

    await initDuckDB();
    await loadLevel0();

    // reactivity
    $("lvl0_sel").addEventListener("change", async () => {{
      await loadLevel1();
      $("submit_btn").disabled = false;
    }});

    $("lvl1_sel").addEventListener("change", async () => {{
      await loadLevel2();
      $("submit_btn").disabled = false;
    }});

    $("lvl2_sel").addEventListener("change", () => {{
      $("submit_btn").disabled = false;
    }});

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
        COL0_JS=COL0_JS,
        COL1_JS=COL1_JS,
        COL2_JS=COL2_JS,
    )

    return common.html_to_obj(html)
