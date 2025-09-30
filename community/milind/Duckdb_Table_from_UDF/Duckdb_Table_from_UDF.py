common = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/")

@fused.udf(cache_max_age=0)
def udf(
    data_url: str = "https://www.fused.io/server/v1/realtime-shared/UDF_Airbnb_listings_nyc_parquet/run/file?dtype_out_raster=png&dtype_out_vector=parquet",
    initial_sql: str = "SELECT * FROM df LIMIT 50"
):
    """
    DuckDB-WASM SQL viewer with simple virtualized table.
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
    .grid-header table {{ width:100%; border-collapse: collapse; table-layout: fixed; }}
    .grid-header th {{ border-bottom:1px solid #000; padding:4px 6px; text-align:left; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; user-select:none; cursor:pointer; }}
    .grid-header th .dir {{ opacity:0.6; margin-left:4px; }}
    .grid-body {{ height: 60vh; overflow: auto; }}
    .grid-body table {{ width:100%; border-collapse: collapse; table-layout: fixed; }}
    .grid-body td {{ border-bottom:1px solid #eee; padding:4px 6px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .grid-body tr:nth-child(even) td {{ background:#fafafa; }}
    .grid-body tr:hover td {{ background:#f2f7ff; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .null {{ color:#999; font-style: italic; }}
    .muted {{ color:#555; }}
    code {{ background:#f3f3f3; padding:2px 4px; border-radius:3px; }}
    #toast {{ position:fixed; bottom:10px; right:10px; background:#000; color:#fff; padding:6px 10px; border-radius:6px; font-size:12px; opacity:0; transition:opacity .2s; pointer-events:none; }}
  </style>
</head>
<body>
  <h3>DuckDB SQL Viewer</h3>
  <div>Dataset: <code>{data_url}</code></div>

  <textarea id="queryInput">{initial_sql}</textarea><br/>
  <button id="runBtn" disabled>Run</button>
  <div class="status" id="status">Loading DuckDB and dataset…</div>

  <div id="results" class="grid" style="display:none;">
    <div class="grid-header">
      <div style="overflow:auto;">
        <table><thead><tr id="theadRow"></tr></thead></table>
      </div>
    </div>
    <div class="grid-body" id="gridBody">
      <div id="topPad"></div>
      <div style="overflow:auto;">
        <table>
          <tbody id="tbody"></tbody>
        </table>
      </div>
      <div id="bottomPad"></div>
    </div>
    <div class="muted" style="padding:6px 8px;" id="rowCount"></div>
  </div>

  <div id="toast">Copied</div>

  <script type="module">
    const DATA_URL = {data_url!r};
    const DEBOUNCE_MS = 250;
    let conn = null;
    let typingTimer = 0;

    const V = {{
      rows: [],
      cols: [],
      total: 0,
      rowHeight: 28,
      buffer: 10,
      sortKey: null,
      sortDir: 1,
      raf: 0
    }};

    const $ = (id) => document.getElementById(id);
    const setStatus = (t) => ($("status").textContent = t);

    const isNum = (v) => typeof v === "number" || (typeof v === "string" && v.trim() !== "" && !isNaN(Number(v)));
    function cmp(a,b) {{
      if (a==null && b==null) return 0;
      if (a==null) return -1;
      if (b==null) return 1;
      const na = Number(a), nb = Number(b);
      if (!isNaN(na) && !isNaN(nb)) return na - nb;
      return String(a).localeCompare(String(b));
    }}
    function showToast(msg="Copied") {{
      const t = $("toast");
      t.textContent = msg;
      t.style.opacity = 1;
      setTimeout(() => t.style.opacity = 0, 800);
    }}

    function renderVirtual(result) {{
      V.rows = result.toArray();
      V.cols = result.schema.fields.map(f => f.name);
      V.total = V.rows.length;

      const resEl = $("results");
      if (!V.total) {{
        resEl.style.display = 'none';
        $("results").insertAdjacentHTML('afterend', '<div>No results</div>');
        return;
      }} else {{
        resEl.style.display = 'block';
      }}

      const theadRow = $("theadRow");
      theadRow.innerHTML = '';
      V.cols.forEach((c,i) => {{
        const th = document.createElement('th');
        th.textContent = c;
        th.title = "Sort by " + c;
        th.dataset.index = String(i);
        if (V.sortKey === c) {{
          const dir = document.createElement('span');
          dir.className = 'dir';
          dir.textContent = V.sortDir === 1 ? '▲' : '▼';
          th.appendChild(dir);
        }}
        th.addEventListener('click', () => {{
          if (V.sortKey === c) {{
            V.sortDir *= -1;
          }} else {{
            V.sortKey = c;
            V.sortDir = 1;
          }}
          sortRows();
          renderHeader();
          schedulePaint();
        }});
        theadRow.appendChild(th);
      }});
      function renderHeader(){{
        const kids = [...theadRow.children];
        kids.forEach(k => {{
          const c = k.firstChild?.nodeValue || '';
          k.innerHTML = c;
          if (c === V.sortKey) {{
            const dir = document.createElement('span');
            dir.className='dir';
            dir.textContent = V.sortDir === 1 ? '▲' : '▼';
            k.appendChild(dir);
          }}
        }});
      }}

      $("tbody").innerHTML = '';
      $("topPad").style.height = '0px';
      $("bottomPad").style.height = Math.max(0, V.total * V.rowHeight) + 'px';
      $("rowCount").textContent = `${{V.total}} rows`;

      if (V.sortKey) sortRows();

      const sc = $("gridBody");
      sc.onscroll = schedulePaint;
      window.onresize = schedulePaint;
      schedulePaint();
    }}

    function sortRows(){{
      const key = V.sortKey;
      if (!key) return;
      V.rows.sort((a,b) => V.sortDir * cmp(a[key], b[key]));
    }}

    function paintWindow(){{
      const sc = $("gridBody");
      const viewH = sc.clientHeight;
      const scrollTop = sc.scrollTop;

      const first = Math.max(0, Math.floor(scrollTop / V.rowHeight) - V.buffer);
      const visible = Math.ceil(viewH / V.rowHeight) + 2 * V.buffer;
      const last = Math.min(V.total, first + visible);

      $("topPad").style.height = (first * V.rowHeight) + 'px';
      $("bottomPad").style.height = ((V.total - last) * V.rowHeight) + 'px';

      const frag = document.createDocumentFragment();
      for (let i = first; i < last; i++) {{
        const tr = document.createElement('tr');
        const r = V.rows[i];
        for (const c of V.cols) {{
          const v = r[c];
          const td = document.createElement('td');
          if (v === null || v === undefined) {{
            td.textContent = "NULL";
            td.className = "null";
          }} else {{
            td.textContent = String(v);
            if (isNum(v)) td.classList.add('num');
          }}
          td.addEventListener('click', () => {{
            navigator.clipboard?.writeText(String(v ?? 'NULL'));
            showToast("Copied cell");
          }});
          tr.appendChild(td);
        }}
        frag.appendChild(tr);
      }}
      const tb = $("tbody");
      tb.innerHTML = '';
      tb.appendChild(frag);
    }}

    function schedulePaint(){{
      if (V.raf) return;
      V.raf = requestAnimationFrame(() => {{ V.raf = 0; paintWindow(); }});
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

        const resp = await fetch(DATA_URL);
        if (!resp.ok) throw new Error("HTTP " + resp.status + " " + resp.statusText);
        const buf = new Uint8Array(await resp.arrayBuffer());
        await db.registerFileBuffer('data.parquet', buf);
        await conn.query("CREATE OR REPLACE VIEW df AS SELECT * FROM read_parquet('data.parquet')");

        setStatus("Data ready");
        $("runBtn").disabled = false;

        runQuery();
      }} catch (e) {{
        setStatus("Error: " + (e?.message || e));
      }}
    }}

    async function runQuery() {{
      if (!conn) return;
      let sql = $("queryInput").value || "";
      sql = sql.trim().replace(/;+$/g, '').trim();
      if (!sql) return;
      try {{
        setStatus("Running…");
        const res = await conn.query(sql);
        renderVirtual(res);
        setStatus("Done");
      }} catch (e) {{
        $("results").style.display = 'none';
        $("results").insertAdjacentHTML('afterend', "<pre>" + (e?.message || e) + "</pre>");
        setStatus("Error");
      }}
    }}

    $("queryInput").addEventListener('input', () => {{
      clearTimeout(typingTimer);
      typingTimer = setTimeout(runQuery, DEBOUNCE_MS);
    }});
    $("runBtn").addEventListener('click', runQuery);
    document.addEventListener('keydown', (e) => {{
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') runQuery();
    }});

    init();
  </script>
</body>
</html>"""
    return common.html_to_obj(html)