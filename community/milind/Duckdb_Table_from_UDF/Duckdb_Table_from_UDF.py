common = fused.load("https://github.com/fusedio/udfs/tree/6b98ee5/public/common/")

@fused.udf
def udf(
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_Airbnb_listings_nyc_parquet/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
    initial_sql: str = """\
    SELECT 
      room_type,
      ROUND(AVG(price_in_dollar), 2) AS avg_price,
      COUNT(*) AS listings
    FROM df
    WHERE price_in_dollar IS NOT NULL AND room_type IS NOT NULL
    GROUP BY room_type
    ORDER BY avg_price DESC;"""
):
    """
    DuckDB-WASM SQL viewer :
    - Loads parquet from a public/accessible URL as view `df`
    - Auto-runs query as you type (small debounce)
    - Run button disabled until data is ready
    """
    
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>DuckDB Minimal SQL Viewer</title>
  <style>
    body {{ margin:16px; font-family: monospace; background:#fff; color:#000; }}
    textarea {{ width:100%; height:120px; }}
    table {{ border-collapse: collapse; margin-top:10px; }}
    th, td {{ border:1px solid #000; padding:4px 6px; }}
    #status {{ margin:8px 0; }}
  </style>
</head>
<body>
  <h3>DuckDB SQL Viewer</h3>
  <div>Dataset: <code>{data_url}</code></div>

  <textarea id="queryInput">{initial_sql}</textarea><br/>
  <button id="runBtn" disabled>Run</button>
  <div id="status">Loading DuckDB and dataset…</div>

  <div id="results"></div>

  <script type="module">
    const DATA_URL = {data_url!r};
    let conn = null;
    let typingTimer = 0;
    const DEBOUNCE_MS = 250;

    function render(result) {{
      const rows = result.toArray();
      if (!rows.length) {{
        document.getElementById('results').innerHTML = "<div>No results</div>";
        return;
      }}
      const cols = result.schema.fields.map(f => f.name);
      let html = "<table><thead><tr>";
      for (const c of cols) html += "<th>"+c+"</th>";
      html += "</tr></thead><tbody>";
      for (const r of rows) {{
        html += "<tr>";
        for (const c of cols) {{
          const v = r[c];
          html += "<td>"+(v===null?"NULL":String(v))+"</td>";
        }}
        html += "</tr>";
      }}
      html += "</tbody></table>";
      document.getElementById('results').innerHTML = html;
    }}

    async function init() {{
      try {{
        const duckdb = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.1-dev132.0/+esm');
        const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
        const workerCode = await (await fetch(bundle.mainWorker)).text();
        const worker = new Worker(URL.createObjectURL(new Blob([workerCode], {{ type:'application/javascript' }})));
        const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule);
        conn = await db.connect();

        // Fetch parquet and register
        const resp = await fetch(DATA_URL);
        if (!resp.ok) throw new Error("HTTP " + resp.status + " " + resp.statusText);
        const buf = new Uint8Array(await resp.arrayBuffer());
        await db.registerFileBuffer('data.parquet', buf);
        await conn.query("CREATE OR REPLACE VIEW df AS SELECT * FROM read_parquet('data.parquet')");

        document.getElementById('status').textContent = "Data ready";
        document.getElementById('runBtn').disabled = false;

        // Auto-run initial query
        runQuery();
      }} catch (e) {{
        document.getElementById('status').textContent = "Error: " + (e?.message || e);
      }}
    }}

    async function runQuery() {{
      if (!conn) return;
      const sql = document.getElementById('queryInput').value || "";
      try {{
        document.getElementById('status').textContent = "Running…";
        const res = await conn.query(sql);
        render(res);
        document.getElementById('status').textContent = "Done";
      }} catch (e) {{
        document.getElementById('results').innerHTML = "<pre>" + (e?.message || e) + "</pre>";
        document.getElementById('status').textContent = "Error";
      }}
    }}

    // Auto-run with small debounce as you type
    const q = document.getElementById('queryInput');
    q.addEventListener('input', () => {{
      clearTimeout(typingTimer);
      typingTimer = setTimeout(runQuery, DEBOUNCE_MS);
    }});

    // Optional manual run
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
