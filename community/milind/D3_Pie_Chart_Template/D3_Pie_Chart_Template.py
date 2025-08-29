@fused.udf
def udf():
    """
    Load the ACS race data (same as the `aqua_chinchilla` UDF) and render it as a
    responsive D3 pie chart with a legend (key) on the side.
    """
    import pandas as pd
    from jinja2 import Template
    import json

    # ------------------------------------------------------------------
    # Load the ACS data (cached for speed)
    # ------------------------------------------------------------------
    @fused.cache
    def load_acs_data():
        table_id = "B02001"
        year = 2022
        url = f"https://www2.census.gov/programs-surveys/acs/summary_file/{year}/table-based-SF/data/5YRData/acsdt5y{year}-{table_id.lower()}.dat"
        return pd.read_csv(url, delimiter="|")

    df = load_acs_data()

    # Map column codes to readable categories
    category_mapping = {
        "B02001_E002": "White alone",
        "B02001_E003": "Black or African American alone",
        "B02001_E004": "American Indian and Alaska Native alone",
        "B02001_E005": "Asian alone",
        "B02001_E006": "Native Hawaiian and Other Pacific Islander alone",
        "B02001_E007": "Some other race alone",
        "B02001_E008": "Two or more races"
    }

    # Build the data structure expected by D3: [[label, value], ...]
    chart_data = [
        [category, int(df[col].iloc[0])]
        for col, category in category_mapping.items()
        if col in df.columns and df[col].iloc[0] > 0
    ]

    # ------------------------------------------------------------------
    # HTML / D3 template (responsive) with legend
    # ------------------------------------------------------------------
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    html_template = Template("""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <title>U.S. Race Composition (2022)</title>
      <script src="https://d3js.org/d3.v7.min.js"></script>
      <style>
        html,body { height:100%; margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; background:#f5f7fa; }
        .wrap { box-sizing:border-box; height:100%; display:flex; align-items:center; justify-content:center; }
        .card { width:100%; max-width:800px; height:100%; background:#fff; border-radius:12px;
                box-shadow:0 6px 25px rgba(0,0,0,0.08); overflow:hidden; display:flex; flex-direction:column; }
        .title { padding:20px; margin:0; font-size:18px; font-weight:600; color:#333; }
        #pie { flex:1; min-height:0; }
        .tooltip { position:absolute; padding:8px; background:rgba(0,0,0,0.8); color:#fff;
                   border-radius:4px; pointer-events:none; font-size:12px; opacity:0; transition:opacity 0.2s; }
        .legend { font-size:12px; }
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="card">
          <h1 class="title">U.S. Race Composition (2022)</h1>
          <div id="pie"></div>
        </div>
      </div>

      <script>
        const data = {{ chart_data | safe }};

        function drawChart() {
          // Clear any existing chart
          d3.select("#pie").selectAll("*").remove();

          // Get container dimensions
          const container = d3.select("#pie").node();
          const { width: cw, height: ch } = container.getBoundingClientRect();
          const margin = {top:20, right:150, bottom:20, left:20};
          const width = Math.max(cw - margin.left - margin.right, 0);
          const height = Math.max(ch - margin.top - margin.bottom, 0);
          if (width === 0 || height === 0) return;
          const radius = Math.min(width, height) / 2;

          // SVG with viewBox for true responsiveness
          const svg = d3.select("#pie")
            .append("svg")
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("viewBox", `0 0 ${cw} ${ch}`)
            .append("g")
            .attr("transform", `translate(${margin.left + width/2},${margin.top + height/2})`);

          const color = d3.scaleOrdinal(d3.schemeCategory10);

          const pie = d3.pie()
            .value(d => d[1]);

          const arc = d3.arc()
            .innerRadius(0)
            .outerRadius(radius);

          // Draw slices
          svg.selectAll("path")
            .data(pie(data))
            .enter()
            .append("path")
            .attr("d", arc)
            .attr("fill", (d,i) => color(i))
            .append("title")
            .text(d => d.data[0] + ": " + d.data[1].toLocaleString());

          // Tooltip
          const tooltip = d3.select("body").append("div").attr("class", "tooltip");
          svg.selectAll("path")
            .on("mouseover", function(event, d) {
              tooltip.style("opacity", 0.9)
                     .html("<strong>" + d.data[0] + "</strong><br/>" + d.data[1].toLocaleString())
                     .style("left", (event.pageX + 10) + "px")
                     .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseout", function() {
              tooltip.style("opacity", 0);
            });

          // Legend (key) on the right side
          const legend = svg.append("g")
            .attr("class", "legend")
            .attr("transform", `translate(${radius + 20},${-radius})`);

          legend.selectAll("rect")
            .data(data)
            .enter()
            .append("rect")
            .attr("x", 0)
            .attr("y", (d,i) => i * 20)
            .attr("width", 12)
            .attr("height", 12)
            .attr("fill", (d,i) => color(i));

          legend.selectAll("text")
            .data(data)
            .enter()
            .append("text")
            .attr("x", 18)
            .attr("y", (d,i) => i * 20 + 10)
            .text(d => d[0]);
        }

        // Initial render
        drawChart();

        // Redraw on window resize (debounced)
        let resizeTimer;
        window.addEventListener('resize', () => {
          clearTimeout(resizeTimer);
          resizeTimer = setTimeout(drawChart, 150);
        });
      </script>
    </body>
    </html>
    """)

    rendered = html_template.render(chart_data=json.dumps(chart_data))
    return common.html_to_obj(rendered)