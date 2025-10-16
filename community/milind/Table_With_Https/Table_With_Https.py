import fused
from jinja2 import Template
import json

@fused.udf(cache_max_age=0)
def udf():
    html = Template(r"""
<!doctype html>
<meta charset="utf-8">
<title>Handsontable fetch-in-cells demo</title>

<!-- Handsontable CDN -->
<script src="https://cdn.jsdelivr.net/npm/handsontable/dist/handsontable.full.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/handsontable/styles/handsontable.min.css" />
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/handsontable/styles/ht-theme-main.min.css" />

<style>
  html, body { height:100%; margin:0; }
  #wrap { position:fixed; inset:0; }
  #grid { height:100%; width:100%; }
</style>

<div id="wrap">
  <div id="grid" class="ht-theme-main-dark-auto"></div>
</div>

<script>
(function(){
  // Seed data (array of objects). Edit the URL and watch Result fill in.
  const data = [
    { URL: "https://api.github.com/repos/openai/openai-cookbook", Result: "" },
    { URL: "https://httpbin.org/get", Result: "" }
  ];

  const container = document.getElementById("grid");
  const hot = new Handsontable(container, {
    data,
    columns: [
      { data: "URL"    , type: "text"   },
      { data: "Result" , type: "text"   },
    ],
    colHeaders: ["URL (GET)", "Result (preview)"],
    rowHeaders: true,
    stretchH: "all",
    height: "100%",
    manualColumnResize: true,
    licenseKey: "non-commercial-and-evaluation"
  });

  async function fetchIntoRow(row, url) {
    // Basic guard
    if (!url || !/^https?:\/\//i.test(url)) {
      hot.setDataAtRowProp(row, "Result", "(not an http/https URL)");
      return;
    }
    hot.setDataAtRowProp(row, "Result", "Loading…");

    try {
      const res = await fetch(url, { method: "GET" });
      // CORS might block access to body if not allowed:
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      // Try JSON first, fall back to text
      let text;
      const ct = res.headers.get("content-type") || "";
      if (ct.includes("application/json")) {
        const obj = await res.json();
        text = JSON.stringify(obj, null, 2);
      } else {
        text = await res.text();
      }
      // Store a compact preview (first few lines)
      const preview = String(text).split("\n").slice(0, 8).join("\n");
      hot.setDataAtRowProp(row, "Result", preview);
    } catch (err) {
      hot.setDataAtRowProp(row, "Result", "ERROR: " + (err && err.message || String(err)));
      console.error("fetch error:", err);
    }
  }

  // When a URL cell changes, fetch it
  hot.addHook("afterChange", (changes, source) => {
    if (!changes || source === "loadData") return;
    for (const [row, prop, oldVal, newVal] of changes) {
      if (prop === "URL" && newVal !== oldVal) {
        fetchIntoRow(row, newVal);
      }
    }
  });

  // Optional: fetch any prefilled URLs at startup
  for (let r = 0; r < hot.countRows(); r++) {
    const url = hot.getDataAtRowProp(r, "URL");
    if (url) fetchIntoRow(r, url);
  }
})();
</script>
""").render()

    common = fused.load("https://github.com/fusedio/udfs/tree/747c1af/public/common/")
    return common.html_to_obj(html)
