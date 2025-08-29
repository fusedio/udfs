@fused.udf
def udf(url: str = "https://www.spc.noaa.gov/wcm/data/1950-2024_all_tornadoes.csv"):
    # Load utilities for returning HTML objects
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    import pandas as pd
    from jinja2 import Template

    @fused.cache
    def load_csv(u):
        # Use a robust encoding for the NOAA CSV
        return pd.read_csv(u, encoding="utf-8-sig")

    # ------------------------------------------------------------------
    # Load and prepare the tornado dataset
    # ------------------------------------------------------------------
    df = load_csv(url)

    # Clean column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Ensure the year and magnitude columns exist
    if "yr" not in df.columns or "mag" not in df.columns:
        raise ValueError("Expected columns 'yr' and 'mag' not found in dataset.")

    # Aggregate counts per year per magnitude
    agg = (
        df.groupby(["yr", "mag"])
        .size()
        .reset_index(name="count")
        .pivot(index="yr", columns="mag", values="count")
        .fillna(0)
        .reset_index()
    )
    # Sort by year
    agg = agg.sort_values("yr")

    # ------------------------------------------------------------------
    # Keep only the top 5 magnitude categories (by total count) to avoid
    # an overly wide legend that spills out of the chart area
    # ------------------------------------------------------------------
    mag_totals = agg.drop(columns="yr").sum().sort_values(ascending=False)
    top_mags = mag_totals.head(5).index.astype(str).tolist()
    # Ensure 'yr' stays as the first column
    agg = agg[["yr"] + [int(m) for m in top_mags]]
    # List of magnitude categories (as strings) for D3 stacking
    mags = top_mags

    # Convert the aggregated DataFrame to JSON records for D3
    data_json = agg.to_json(orient="records")

    # ------------------------------------------------------------------
    # Jinja2 template for a responsive stacked area chart (D3 v7)
    # ------------------------------------------------------------------
    html_template = Template(
        """
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width,initial-scale=1" />
          <title>US Tornadoes by Magnitude (Stacked Area)</title>
          <script src="https://d3js.org/d3.v7.min.js"></script>
          <style>
            html,body { height:100%; margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; background:#f5f7fa; }
            .wrap { box-sizing:border-box; height:100%; display:flex; align-items:center; justify-content:center; }
            .card { width:100%; max-width:1200px; height:100%; background:#ffffff; border-radius:12px;
                    box-shadow:0 6px 25px rgba(0,0,0,0.08); overflow:hidden; display:flex; flex-direction:column; }
            .title { padding:20px 20px 0 20px; margin:0; font-size:18px; font-weight:600; color:#333; }
            #chart { flex:1; min-height:0; }
            .tooltip { position:absolute; padding:8px; background:rgba(0,0,0,0.8); color:#fff;
                       border-radius:4px; pointer-events:none; font-size:12px; opacity:0; transition:opacity 0.2s; }
            .axis text { font-size:11px; }
            .axis-title { font-size:12px; font-weight:500; }
            .legend { font-size:11px; }
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="card">
              <h1 class="title">US Tornadoes by Magnitude (Top 5)</h1>
              <div id="chart"></div>
            </div>
          </div>

          <script>
            const data = {{ data_json | safe }};
            const mags = {{ mags | tojson }};
            const colors = d3.schemeCategory10;

            function drawChart() {
              d3.select("#chart").selectAll("*").remove();

              const container = document.getElementById('chart');
              const rect = container.getBoundingClientRect();
              const margin = {top:20, right:150, bottom:50, left:60};
              const width = Math.max(rect.width - margin.left - margin.right, 300);
              const height = Math.max(rect.height - margin.top - margin.bottom, 200);
              if (width <= 0 || height <= 0) return;

              const svg = d3.select("#chart")
                .append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", `translate(${margin.left},${margin.top})`);

              // Stack the data
              const stack = d3.stack()
                .keys(mags)
                .order(d3.stackOrderNone)
                .offset(d3.stackOffsetNone);

              const series = stack(data);

              const x = d3.scaleLinear()
                .domain(d3.extent(data, d => +d.yr))
                .range([0, width]);

              const y = d3.scaleLinear()
                .domain([0, d3.max(series, s => d3.max(s, d => d[1]))])
                .nice()
                .range([height, 0]);

              const area = d3.area()
                .x(d => x(d.data.yr))
                .y0(d => y(d[0]))
                .y1(d => y(d[1]));

              // X axis
              svg.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${height})`)
                .call(d3.axisBottom(x).ticks(10).tickFormat(d3.format("d")));

              // Y axis
              svg.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y));

              // X axis label
              svg.append("text")
                .attr("class", "axis-title")
                .attr("x", width/2)
                .attr("y", height + margin.bottom - 10)
                .style("text-anchor", "middle")
                .text("Year");

              // Y axis label
              svg.append("text")
                .attr("class", "axis-title")
                .attr("transform", "rotate(-90)")
                .attr("y", -margin.left + 15)
                .attr("x", -height/2)
                .style("text-anchor", "middle")
                .text("Number of Tornadoes");

              // Tooltip
              const tooltip = d3.select("body").append("div")
                .attr("class", "tooltip");

              // Draw stacked areas
              svg.selectAll(".layer")
                .data(series)
                .enter()
                .append("path")
                .attr("class", "layer")
                .attr("fill", (d,i) => colors[i % colors.length])
                .attr("d", area)
                .on("mouseover", function(event, d) {
                  const mag = d.key;
                  tooltip.transition().duration(200).style("opacity", .9);
                  tooltip.html(`<strong>Magnitude: ${mag}</strong>`)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");
                })
                .on("mousemove", function(event) {
                  tooltip.style("left", (event.pageX + 10) + "px")
                         .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", function() {
                  tooltip.transition().duration(500).style("opacity", 0);
                });

              // Legend (placed to the right of the chart)
              const legend = svg.selectAll(".legend")
                .data(mags)
                .enter().append("g")
                .attr("class", "legend")
                .attr("transform", (d,i) => `translate(${width + 20},${i * 20})`);

              legend.append("rect")
                .attr("x", 0)
                .attr("width", 18)
                .attr("height", 18)
                .attr("fill", (d,i) => colors[i % colors.length]);

              legend.append("text")
                .attr("x", 24)
                .attr("y", 9)
                .attr("dy", ".35em")
                .style("text-anchor", "start")
                .style("font-size", "12px")
                .text(d => `Mag ${d}`);
            }

            // Initial render and resize handling
            drawChart();
            window.addEventListener("resize", drawChart);
          </script>
        </body>
        </html>
        """
    )

    rendered = html_template.render(data_json=data_json, mags=mags)
    return common.html_to_obj(rendered)