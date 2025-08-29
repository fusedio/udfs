import fused
import pandas as pd
import json
from jinja2 import Template

@fused.udf
def udf():
    """
    =============================================================================
    CHART TYPE: D3 Interactive Scatter Plot - Penguin Dataset  
    Clean Jinja version - no f-strings, safe templating throughout
    =============================================================================
    """
    
    # REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    @fused.cache
    def load_data():
        url = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        return pd.read_csv(url)

    df = load_data()

    # Configuration
    config = {
        "x_field": "bill_length_mm",
        "y_field": "bill_depth_mm", 
        "category_field": "species",
        "size_field": "body_mass_g",
        "color_scheme": ["#440154ff", "#21908dff", "#fde725ff"],
        "point_size": 5,
        "opacity": 0.7,
        "title": "Palmer Penguins: Bill Dimensions Analysis",
        "x_label": "Bill Length (mm)",
        "y_label": "Bill Depth (mm)",
    }

    # Prepare data
    df_clean = df.dropna(subset=[config["x_field"], config["y_field"], config["category_field"]])
    data_json = df_clean.to_json(orient="records")
    categories = df_clean[config["category_field"]].unique().tolist()

    # Single Jinja template for everything
    html_template = Template("""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <title>{{ config.title }}</title>
      <script src="https://d3js.org/d3.v7.min.js"></script>
      <style>
        :root { --bg: #f5f7fa; --card-bg: #ffffff; --radius: 12px; }
        html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:var(--bg); }
        .wrap { box-sizing:border-box; height:100%; display:flex; align-items:center; justify-content:center; }
        .card { width:100%; max-width:1200px; height:100%; background:var(--card-bg); border-radius:var(--radius);
                box-shadow:0 6px 25px rgba(0,0,0,0.08); overflow:hidden; display:flex; flex-direction:column; }
        .title { padding:20px 20px 0 20px; margin:0; font-size:18px; font-weight:600; color:#333; }
        #my_dataviz { flex: 1; min-height: 0; }
        .tooltip { position: absolute; padding: 10px; background: rgba(0, 0, 0, 0.8);
                   color: white; border-radius: 5px; pointer-events: none; font-size: 12px; }
        .axis text { font-size: 11px; }
        .axis-title { font-size: 12px; font-weight: 500; }
        @media (max-width:640px) { .card { border-radius:8px; } }
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="card">
          <h1 class="title">{{ config.title }}</h1>
          <div id="my_dataviz"></div>
        </div>
      </div>

      <script>
        // Data from Python - safely injected via Jinja
        const data = {{ data_json | safe }};
        const categories = {{ categories | tojson }};
        const colorScheme = {{ config.color_scheme | tojson }};
        
        // Config from Python
        const chartConfig = {
          xField: "{{ config.x_field }}",
          yField: "{{ config.y_field }}",
          categoryField: "{{ config.category_field }}",
          pointSize: {{ config.point_size }},
          opacity: {{ config.opacity }},
          xLabel: "{{ config.x_label }}",
          yLabel: "{{ config.y_label }}"
        };

        function updateChart() {
          // Clear previous chart
          d3.select("#my_dataviz").selectAll("*").remove();

          // Get container dimensions
          const container = d3.select("#my_dataviz").node();
          const containerRect = container.getBoundingClientRect();

          // Margins & dimensions
          const margin = {top: 20, right: 100, bottom: 60, left: 80};
          const width = containerRect.width - margin.left - margin.right;
          const height = containerRect.height - margin.top - margin.bottom;

          if (width <= 0 || height <= 0) return; // nothing to draw

          // SVG canvas
          const svg = d3.select("#my_dataviz")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${margin.left}, ${margin.top})`);

          // Scales
          const xExtent = d3.extent(data, d => +d[chartConfig.xField]);
          const yExtent = d3.extent(data, d => +d[chartConfig.yField]);

          const x = d3.scaleLinear()
            .domain([xExtent[0] * 0.95, xExtent[1] * 1.05])
            .range([0, width]);

          const y = d3.scaleLinear()
            .domain([yExtent[0] * 0.95, yExtent[1] * 1.05])
            .range([height, 0]);

          // Color scale
          const color = d3.scaleOrdinal()
            .domain(categories)
            .range(colorScheme);

          // X axis
          svg.append("g")
            .attr("class", "axis")
            .attr("transform", `translate(0, ${height})`)
            .call(d3.axisBottom(x).tickFormat(d3.format(".1f")));

          // X axis title
          svg.append("text")
            .attr("class", "axis-title")
            .attr("transform", `translate(${width/2}, ${height + 40})`)
            .style("text-anchor", "middle")
            .text(chartConfig.xLabel);

          // Y axis
          svg.append("g")
            .attr("class", "axis")
            .call(d3.axisLeft(y).tickFormat(d3.format(".1f")));

          // Y axis title
          svg.append("text")
            .attr("class", "axis-title")
            .attr("transform", "rotate(-90)")
            .attr("y", 0 - margin.left + 20)
            .attr("x", 0 - (height / 2))
            .style("text-anchor", "middle")
            .text(chartConfig.yLabel);

          // Tooltip
          const tooltip = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);

          // Points
          svg.append('g')
            .selectAll("dot")
            .data(data)
            .enter().append("circle")
            .attr("cx", d => x(+d[chartConfig.xField]))
            .attr("cy", d => y(+d[chartConfig.yField]))
            .attr("r", chartConfig.pointSize)
            .style("fill", d => color(d[chartConfig.categoryField]))
            .style("opacity", chartConfig.opacity)
            .style("stroke", "white")
            .style("stroke-width", 1)
            .on("mouseover", function(event, d) {
              tooltip.transition().duration(200).style("opacity", .9);
              tooltip.html(`
                <strong>${d[chartConfig.categoryField]}</strong><br/>
                ${chartConfig.xLabel}: ${(+d[chartConfig.xField]).toFixed(1)}<br/>
                ${chartConfig.yLabel}: ${(+d[chartConfig.yField]).toFixed(1)}
              `)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseout", function() {
              tooltip.transition().duration(500).style("opacity", 0);
            });

          // Legend
          const legend = svg.selectAll(".legend")
            .data(categories)
            .enter().append("g")
            .attr("class", "legend")
            .attr("transform", (d, i) => `translate(${width + 20}, ${i * 25})`);

          legend.append("rect")
            .attr("x", 0)
            .attr("width", 18)
            .attr("height", 18)
            .style("fill", color)
            .style("opacity", chartConfig.opacity);

          legend.append("text")
            .attr("x", 24)
            .attr("y", 9)
            .attr("dy", ".35em")
            .style("text-anchor", "start")
            .style("font-size", "12px")
            .text(d => d);
        }

        // Initial render
        updateChart();

        // Re-render on resize
        window.addEventListener('resize', updateChart);
      </script>
    </body>
    </html>
    """)

    # Render with all variables
    rendered = html_template.render(
        config=config,
        data_json=data_json,
        categories=categories
    )

    return common.html_to_obj(rendered)