common = fused.load("https://github.com/fusedio/udfs/tree/6b98ee5/public/common/")

@fused.udf(cache_max_age = 0)
def udf(
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_Airbnb_listings_nyc_parquet/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
    initial_sql: str = "SELECT * FROM df LIMIT 3"
):
    """
    DuckDB-WASM SQL viewer with VIRTUALIZED table:
    - Loads parquet as view `df`
    - Auto-runs query as you type (debounced)
    - Table renders only visible rows for performance
    - Displays the original S3 URL (not the signed one) in the UI
    """

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>DuckDB SQL Viewer (Virtualized)</title>
  <style>
    body {{ margin:16px; font-family: monospace; background:#fff; color:#000; }}
    textarea {{ width:100%; height:120px; }}
    .status {{ margin:8px 0; }}
    .grid {{ margin-top: 10px; border:1px solid #000; border-radius:4px; }}
    .grid-header {{ position: sticky; top: 0; background:#f8f8f8; z-index: 1; }}
    .grid-header table {{ width:100%; border-collapse: collapse; }}
    .grid-header th {{ border-bottom:1px solid #000; padding:4px 6px; text-align:left; }}
    .grid-body {{ height: 60vh; overflow: auto; }}
    .grid-body table {{ width:100%; border-collapse: collapse; }}
    .grid-body td {{ border-bottom:1px solid #eee; padding:4px 6px; }}
    .muted {{ color:#555; }}
    code {{ background:#f3f3f3; padding:2px 4px; border-radius:3px; }}
  </style>
</head>
<body>
  <h3>DuckDB SQL Viewer</h3>
  <!-- Show the raw S3 path -->
  <div>Dataset: <code>{data_url}</code></div>

  <textarea id="queryInput">{initial_sql}</textarea><br/>
  <button id="runBtn" disabled>Run</button>
  <div class="status" id="status">Loading DuckDB and dataset…</div>

  <div id="results" class="grid" style="display:none;">
    <div class="grid-header">
      <table><thead><tr id="theadRow"></tr></thead></table>
    </div>
    <div class="grid-body" id="gridBody">
      <div id="topPad"></div>
      <table>
        <tbody id="tbody"></tbody>
      </table>
      <div id="bottomPad"></div>
    </div>
    <div class="muted" style="padding:6px 8px;" id="rowCount"></div>
  </div>

  <script type="module">
    // Fetch directly from data_url (no signing)
    const signed_url = {data_url!r};
    let conn = null;
    let typingTimer = 0;
    const DEBOUNCE_MS = 250;

    // --- Virtualization state ---
    let V_STATE = {{
      rows: [],          // array of row objects
      cols: [],          // array of column names
      rowHeight: 28,     // px per row
      buffer: 10,        // extra rows above/below viewport
      total: 0           // total number of rows
    }};

    function setStatus(t) {{
      const s = document.getElementById('status');
      if (s) s.textContent = t;
    }}

    // ---------- VIRTUALIZED RENDERER ----------
    function renderVirtual(result) {{
      const rows = result.toArray();
      const cols = result.schema.fields.map(f => f.name);
      V_STATE.rows = rows;
      V_STATE.cols = cols;
      V_STATE.total = rows.length;

      const resEl = document.getElementById('results');
      const theadRow = document.getElementById('theadRow');
      const gridBody = document.getElementById('gridBody');
      const topPad = document.getElementById('topPad');
      const bottomPad = document.getElementById('bottomPad');
      const tbody = document.getElementById('tbody');
      const rowCount = document.getElementById('rowCount');

      // If empty
      if (!rows.length) {{
        resEl.style.display = 'none';
        document.getElementById('results').insertAdjacentHTML('afterend', '<div>No results</div>');
        return;
      }} else {{
        resEl.style.display = 'block';
      }}

      // Build header
      theadRow.innerHTML = '';
      for (const c of cols) {{
        const th = document.createElement('th');
        th.textContent = c;
        theadRow.appendChild(th);
      }}

      // Reset body & pads
      tbody.innerHTML = '';
      topPad.style.height = '0px';
      bottomPad.style.height = Math.max(0, V_STATE.total * V_STATE.rowHeight) + 'px';

      rowCount.textContent = `${{V_STATE.total}} rows`;

      // Hook scroll + initial paint
      gridBody.onscroll = onScroll;
      window.onresize = onScroll;
      onScroll();

      function onScroll() {{
        const viewH = gridBody.clientHeight;
        const scrollTop = gridBody.scrollTop;

        const first = Math.max(0, Math.floor(scrollTop / V_STATE.rowHeight) - V_STATE.buffer);
        const visibleCount = Math.ceil(viewH / V_STATE.rowHeight) + 2 * V_STATE.buffer;
        const last = Math.min(V_STATE.total, first + visibleCount);

        const padTop = first * V_STATE.rowHeight;
        const padBottom = (V_STATE.total - last) * V_STATE.rowHeight;

        // Update spacers
        topPad.style.height = padTop + 'px';
        bottomPad.style.height = padBottom + 'px';

        // Render window
        const frag = document.createDocumentFragment();
        for (let i = first; i < last; i++) {{
          const tr = document.createElement('tr');
          const r = rows[i];
          for (const c of cols) {{
            const td = document.createElement('td');
            const v = r[c];
            td.textContent = (v === null || v === undefined) ? 'NULL' : String(v);
            tr.appendChild(td);
          }}
          frag.appendChild(tr);
        }}
        // Replace tbody content
        tbody.innerHTML = '';
        tbody.appendChild(frag);
      }}
    }}
    // ------------------------------------------

    async function init() {{
      try {{
        const duckdb = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm');
        const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
        const workerCode = await (await fetch(bundle.mainWorker)).text();
        const worker = new Worker(URL.createObjectURL(new Blob([workerCode], {{ type:'application/javascript' }})));
        const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule);
        conn = await db.connect();

        // Fetch parquet and register (direct URL)
        const resp = await fetch(signed_url);
        if (!resp.ok) throw new Error("HTTP " + resp.status + " " + resp.statusText);
        const buf = new Uint8Array(await resp.arrayBuffer());
        await db.registerFileBuffer('data.parquet', buf);
        await conn.query("CREATE OR REPLACE VIEW df AS SELECT * FROM read_parquet('data.parquet')");

        setStatus("Data ready");
        document.getElementById('runBtn').disabled = false;

        // Auto-run initial query
        runQuery();
      }} catch (e) {{
        setStatus("Error: " + (e?.message || e));
      }}
    }}

    async function runQuery() {{
      if (!conn) return;
      let sql = document.getElementById('queryInput').value || "";

      // sanitize to avoid parser hiccups
      sql = sql.trim().replace(/;+$/g, '').trim();
      if (!sql) return;

      try {{
        setStatus("Running…");
        const res = await conn.query(sql);
        renderVirtual(res);
        setStatus("Done");
      }} catch (e) {{
        document.getElementById('results').style.display = 'none';
        document.getElementById('results').insertAdjacentHTML('afterend', "<pre>" + (e?.message || e) + "</pre>");
        setStatus("Error");
      }}
    }}

    // Auto-run with small debounce as you type
    const q = document.getElementById('queryInput');
    q.addEventListener('input', () => {{
      clearTimeout(typingTimer);
      typingTimer = setTimeout(runQuery, 250);
    }});

    // Manual run
    document.getElementById('runBtn').addEventListener('click', runQuery);

    // Ctrl/⌘ + Enter runs immediately
    document.addEventListener('keydown', (e) => {{
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') runQuery();
    }});

    init();
  </script>
</body>
</html>"""
    return common.html_to_obj(html)
