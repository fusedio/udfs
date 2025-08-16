@fused.udf
def udf(data_url: str = None, topo_url: str = None):
    """
    US county unemployment choropleth using D3 (quantize scale + discrete legend).
    - Default data: plotly fips-unemp CSV (has 'fips' and 'unemp' columns).
    - Default topo: us-atlas counties TopoJSON.
    - Keeps required prints: dataframe dtypes and first-5 rows.
    - Accepts `data_url` and `topo_url` to override defaults.
    - Configurable domain and discrete color scheme (uses d3.scaleQuantize + d3.schemeBlues[9]).
    - Responsive SVG (viewBox + preserveAspectRatio).
    """
    import pandas as pd
    import json
    import requests
    from jinja2 import Template

    # REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    @fused.cache
    def load_data(data_url=None, topo_url=None):
        # sensible defaults
        default_data = "https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv"
        default_topo = "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json"

        src = data_url or default_data
        topo_src = topo_url or default_topo

        df = None
        try:
            df = pd.read_csv(src)
        except Exception as e:
            print("Warning: failed to load data_url:", e)
            df = None

        if df is None or df.shape[0] == 0:
            # tiny fallback
            df = pd.DataFrame({"fips": ["01001", "01003", "01005"], "rate": [5.3, 5.4, 8.6]})

        # normalize common column names:
        # prefer 'rate' (user example). fall back to 'unemp' or 'unemployment'
        if "rate" not in df.columns:
            for alt in ["unemp", "unemployment", "value"]:
                if alt in df.columns:
                    df = df.rename(columns={alt: "rate"})
                    break

        if "fips" not in df.columns:
            for alt in ["id", "FIPS", "geoid"]:
                if alt in df.columns:
                    df = df.rename(columns={alt: "fips"})
                    break

        # ensure fips are zero-padded strings
        if "fips" in df.columns:
            df["fips"] = df["fips"].astype(str).str.zfill(5)

        # ensure rate numeric
        if "rate" in df.columns:
            df["rate"] = pd.to_numeric(df["rate"], errors="coerce")

        # load topojson (we don't parse it server-side, but attempt quick fetch to validate)
        topojson_raw = None
        try:
            r = requests.get(topo_src, timeout=10)
            r.raise_for_status()
            topojson_raw = r.text
        except Exception as e:
            print("Warning: failed to load topojson preview:", e)
            topojson_raw = None

        # required prints
        print("=== DataFrame dtypes ===")
        print(df.dtypes)
        print("=== DataFrame sample (first 5 rows) ===")
        try:
            print(df.head(5).to_dict(orient="records"))
        except Exception:
            print(df.head(5))

        return df, topo_src

    df, topo_src = load_data(data_url, topo_url)

    # Template configuration (exposed for ease of changes)
    config = {
        "data_id_field": "fips",      # match county id d.id
        "value_field": "rate",        # numeric unemployment rate
        # color scale config: quantize with explicit domain and d3.schemeBlues[9]
        "scale": "quantize",
        "domain": [1, 10],            # keep domain as user requested
        "scheme": "Blues",            # "Blues" -> d3.schemeBlues
        "scheme_size": 9,             # use d3.schemeBlues[9]
        "unknown_color": "#e0e0e0",
        "stroke_color": "#ffffff",
        "stroke_width": 0.25,
        "title": "US County Unemployment (rate %)",
        "width": 975,
        "height": 610,
    }

    data_js = df.to_json(orient="records")

    # HTML + D3: uses scaleQuantize with explicit domain and d3.schemeBlues[9].
    # Note: avoid JS template literals; use concatenation for strings.
    html_template = Template("""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{{ title }}</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <script src="https://unpkg.com/topojson@3"></script>
  <style>
    html,body{margin:0;height:100%;font-family:Arial,Helvetica,sans-serif}
    .container{padding:12px;max-width:1200px;margin:0 auto}
    svg{width:100%;height:auto;display:block}
    .tooltip{position:absolute;padding:6px;background:rgba(0,0,0,0.75);color:#fff;border-radius:4px;
      font-size:12px;pointer-events:none;opacity:0;transition:opacity .12s}
    .title{font-weight:600;margin-bottom:8px}
    #legend{display:flex;align-items:center;gap:8px;margin-top:8px;color:#333;font-size:12px}
    .legend-swatch { width:28px; height:14px; display:inline-block; margin-right:4px; border:1px solid #ccc; }
    @media (max-width:900px) { .container{padding:8px} }
  </style>
</head>
<body>
  <div class="container">
    <div class="title">{{ title }}</div>
    <svg id="map" viewBox="0 0 {{ width }} {{ height }}" preserveAspectRatio="xMidYMid meet"></svg>
    <div id="tooltip" class="tooltip"></div>
    <div id="legend"></div>
  </div>

  <script>
    var data = {{ data_js | safe }};
    var topoUrl = "{{ topo_url }}";
    var cfg = {
      dataId: "{{ data_id_field }}",
      value: "{{ value_field }}",
      domainLow: {{ domain[0] }},
      domainHigh: {{ domain[1] }},
      scheme: "{{ scheme }}",
      schemeSize: {{ scheme_size }},
      unknownColor: "{{ unknown_color }}",
      stroke: "{{ stroke_color }}",
      strokeW: {{ stroke_width }},
      width: {{ width }},
      height: {{ height }}
    };

    // build map from data
    var dataMap = new Map();
    data.forEach(function(d) {
      var k = d[cfg.dataId] != null ? String(d[cfg.dataId]) : null;
      dataMap.set(k, d[cfg.value] == null ? NaN : +d[cfg.value]);
    });

    // color scale: quantize to fixed number of buckets using d3.schemeBlues[9] etc.
    var colorRange = (d3["scheme" + cfg.scheme] && d3["scheme" + cfg.scheme][cfg.schemeSize])
      ? d3["scheme" + cfg.scheme][cfg.schemeSize]
      : d3.schemeBlues[cfg.schemeSize];

    var color = d3.scaleQuantize().domain([cfg.domainLow, cfg.domainHigh]).range(colorRange);

    var svg = d3.select("#map");
    var projection = d3.geoAlbersUsa();
    var path = d3.geoPath().projection(projection);
    var tip = d3.select("#tooltip");

    d3.json(topoUrl).then(function(us) {
      // counties feature collection
      var counties = topojson.feature(us, us.objects.counties);
      // optional state borders mesh
      var statemesh = topojson.mesh(us, us.objects.states, function(a, b) { return a !== b; });

      // fit projection to counties
      projection.fitSize([cfg.width, cfg.height], counties);

      svg.selectAll("path.county")
        .data(counties.features)
        .join("path")
        .attr("class", "county")
        .attr("d", path)
        .attr("fill", function(d) {
          var id = d.id != null ? String(d.id) : null;
          var v = dataMap.get(id);
          return (v != null && !isNaN(v)) ? color(v) : cfg.unknownColor;
        })
        .attr("stroke", cfg.stroke)
        .attr("stroke-width", cfg.strokeW)
        .on("mouseover", function(event, d) {
          var id = d.id != null ? String(d.id) : null;
          var v = dataMap.get(id);
          var vtxt = (v != null && !isNaN(v)) ? d3.format(".1f")(v) + "%" : "No data";
          var name = (d.properties && (d.properties.name || d.properties.NAMELSAD || d.properties.NAME)) ? (d.properties.name || d.properties.NAMELSAD || d.properties.NAME) : id;
          tip.style("opacity", 1)
             .html("<strong>" + name + "</strong><br/>Unemployment: " + vtxt)
             .style("left", (event.pageX + 10) + "px")
             .style("top", (event.pageY - 10) + "px");
          d3.select(this).attr("stroke-width", cfg.strokeW * 2);
        })
        .on("mousemove", function(event) {
          tip.style("left", (event.pageX + 10) + "px")
             .style("top", (event.pageY - 10) + "px");
        })
        .on("mouseout", function() {
          tip.style("opacity", 0);
          d3.select(this).attr("stroke-width", cfg.strokeW);
        });

      // draw state borders on top
      svg.append("path")
        .datum(statemesh)
        .attr("fill", "none")
        .attr("stroke", "#666")
        .attr("stroke-width", 0.6)
        .attr("d", path)
        .attr("pointer-events", "none");

      // Legend for quantized scale: discrete swatches and threshold labels
      (function createLegend() {
        var legendContainer = d3.select("#legend");
        legendContainer.selectAll("*").remove();

        var range = color.range();
        var n = range.length;
        // compute bucket thresholds (left edges)
        var thresholds = [];
        for (var i = 0; i < n; i++) {
          var t = cfg.domainLow + i * (cfg.domainHigh - cfg.domainLow) / n;
          thresholds.push(t);
        }
        // build legend row
        var legendRow = legendContainer.append("div").style("display", "flex").style("align-items", "center");
        // swatches
        for (var i = 0; i < n; i++) {
          legendRow.append("div")
            .attr("class", "legend-swatch")
            .style("background", range[i]);
        }
        // labels: show min and max plus tick marks for thresholds
        var label = legendContainer.append("div").style("margin-left", "8px");
        var fmt = d3.format(".1f");
        label.html("Range: " + fmt(cfg.domainLow) + " — " + fmt(cfg.domainHigh));
      })();

    }).catch(function(err) {
      console.error("Failed to load topojson:", err);
      d3.select("#map").append("text").attr("x",10).attr("y",20).text("Failed to load map data.");
    });
  </script>
</body>
</html>
    """)

    rendered = html_template.render(
        title=config["title"],
        width=config["width"],
        height=config["height"],
        data_js=data_js,
        topo_url=topo_src,
        data_id_field=config["data_id_field"],
        value_field=config["value_field"],
        domain=config["domain"],
        scheme=config["scheme"],
        scheme_size=config["scheme_size"],
        unknown_color=config["unknown_color"],
        stroke_color=config["stroke_color"],
        stroke_width=config["stroke_width"],
    )

    return common.html_to_obj(rendered)