@fused.udf
def udf():
    """
    Load the 30‑year mortgage rate time‑series from the FRED CSV endpoint
    and return a D3 histogram (distribution) chart as HTML.
    """
    # Load common utilities for returning HTML objects
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    import pandas as pd
    from jinja2 import Template

    # ------------------------------------------------------------------
    # Cached download of the CSV – runs only once per unique URL
    # ------------------------------------------------------------------
    @fused.cache
    def load_data():
        csv_url = (
            "https://fred.stlouisfed.org/graph/fredgraph.csv?"
            "bgcolor=%23ebf3fb&chart_type=line&drp=0&fo=open%20sans&"
            "graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&"
            "txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0&"
            "show_legend=yes&show_axis_titles=yes&show_tooltip=yes&"
            "id=MORTGAGE30US&scale=left&cosd=1971-04-02&coed=2025-08-14&"
            "line_color=%230073e6&link_values=false&line_style=solid&"
            "mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a&"
            "fq=Weekly%2C%20Ending%20Thursday&fam=avg&fgst=lin&"
            "fgsnd=2020-02-01&line_index=1&transformation=lin&"
            "vintage_date=2025-08-15&revision_date=2025-08-15&nd=1971-04-02"
        )
        return pd.read_csv(csv_url)

    # ------------------------------------------------------------------
    # Load the data and prepare it for the chart
    # ------------------------------------------------------------------
    df = load_data()

    # Keep only the rate column (ignore dates for the distribution)
    if "MORTGAGE30US" in df.columns:
        rates = df["MORTGAGE30US"].dropna()
    else:
        # Fallback: use the second column if the expected name is missing
        rates = df.iloc[:, 1].dropna()

    # Convert the series to JSON (records orientation) – each value as a dict
    data_json = rates.to_frame(name="rate").reset_index(drop=True).to_json(orient="records")

    # ------------------------------------------------------------------
    # Chart configuration (can be tweaked later)
    # ------------------------------------------------------------------
    config = {
        "title": "Distribution of 30‑Year Mortgage Rates (US)",
        "x_label": "Rate (%)",
        "y_label": "Count",
        "bar_color": "#0073e6",
        "opacity": 0.8,
        "bins": 30  # number of histogram bins
    }

    # ------------------------------------------------------------------
    # Jinja2 HTML template that builds a responsive D3 histogram
    # ------------------------------------------------------------------
    html_template = Template(
        """
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width,initial-scale=1" />
          <title>{{ config.title }}</title>
          <script src="https://d3js.org/d3.v7.min.js"></script>
          <style>
            :root { --bg:#f5f7fa; --card:#ffffff; --radius:12px; }
            html,body { height:100%; margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; background:var(--bg); }
            .wrap { box-sizing:border-box; height:100%; display:flex; align-items:center; justify-content:center; }
            .card { width:100%; max-width:1200px; height:100%; background:var(--card); border-radius:var(--radius);
                    box-shadow:0 6px 25px rgba(0,0,0,0.08); overflow:hidden; display:flex; flex-direction:column; }
            .title { padding:20px 20px 0 20px; margin:0; font-size:18px; font-weight:600; color:#333; }
            #chart { flex:1; min-height:0; }
            .tooltip { position:absolute; padding:8px; background:rgba(0,0,0,0.7); color:#fff;
                       border-radius:4px; pointer-events:none; font-size:12px; opacity:0; transition:opacity 0.2s; }
            .axis text { font-size:11px; }
            .axis-title { font-size:12px; font-weight:500; }
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="card">
              <h1 class="title">{{ config.title }}</h1>
              <div id="chart"></div>
            </div>
          </div>

          <script>
            const data = {{ data_json | safe }};
            const cfg = {
              barColor: "{{ config.bar_color }}",
              opacity: {{ config.opacity }},
              xLabel: "{{ config.x_label }}",
              yLabel: "{{ config.y_label }}",
              bins: {{ config.bins }}
            };

            function drawChart() {
              d3.select("#chart").selectAll("*").remove();

              const container = d3.select("#chart").node();
              const { width, height } = container.getBoundingClientRect();
              const margin = {top:20, right:30, bottom:60, left:60};
              const w = Math.max(width - margin.left - margin.right, 0);
              const h = Math.max(height - margin.top - margin.bottom, 0);
              if (w===0 || h===0) return;

              const svg = d3.select("#chart")
                .append("svg")
                .attr("width", w + margin.left + margin.right)
                .attr("height", h + margin.top + margin.bottom)
                .append("g")
                .attr("transform", `translate(${margin.left},${margin.top})`);

              // Extract the numeric values
              const values = data.map(d => +d.rate);

              // Create histogram bins
              const histogram = d3.bin()
                .domain(d3.extent(values))
                .thresholds(cfg.bins);

              const bins = histogram(values);

              // X scale (rate)
              const x = d3.scaleLinear()
                .domain([bins[0].x0, bins[bins.length-1].x1])
                .range([0, w]);

              // Y scale (count)
              const y = d3.scaleLinear()
                .domain([0, d3.max(bins, d => d.length)])
                .nice()
                .range([h, 0]);

              // X axis
              svg.append("g")
                .attr("class","axis")
                .attr("transform", `translate(0,${h})`)
                .call(d3.axisBottom(x));

              // X axis label
              svg.append("text")
                .attr("class","axis-title")
                .attr("x", w/2)
                .attr("y", h + margin.bottom - 10)
                .style("text-anchor","middle")
                .text(cfg.xLabel);

              // Y axis
              svg.append("g")
                .attr("class","axis")
                .call(d3.axisLeft(y));

              // Y axis label
              svg.append("text")
                .attr("class","axis-title")
                .attr("transform","rotate(-90)")
                .attr("y", -margin.left + 15)
                .attr("x", -h/2)
                .style("text-anchor","middle")
                .text(cfg.yLabel);

              // Tooltip
              const tooltip = d3.select("body").append("div")
                .attr("class","tooltip");

              // Bars
              svg.selectAll(".bar")
                .data(bins)
                .enter()
                .append("rect")
                .attr("class","bar")
                .attr("x", d => x(d.x0) + 1)
                .attr("y", d => y(d.length))
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
                .attr("height", d => h - y(d.length))
                .attr("fill", cfg.barColor)
                .attr("fill-opacity", cfg.opacity)
                .on("mouseover", (event,d) => {
                  tooltip.transition().duration(200).style("opacity", .9);
                  tooltip.html(
                    `<strong>Range:</strong> ${d.x0.toFixed(2)} – ${d.x1.toFixed(2)}<br/>
                     <strong>Count:</strong> ${d.length}`
                  )
                  .style("left",(event.pageX+10)+"px")
                  .style("top",(event.pageY-28)+"px");
                })
                .on("mouseout", () => {
                  tooltip.transition().duration(500).style("opacity", 0);
                });
            }

            // Initial render
            drawChart();

            // Redraw on window resize
            window.addEventListener("resize", drawChart);
          </script>
        </body>
        </html>
        """
    )

    rendered = html_template.render(config=config, data_json=data_json)
    return common.html_to_obj(rendered)