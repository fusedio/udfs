@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Scatter Plot with Tooltips
    # WHEN TO USE: Show relationship between TWO numeric variables, identify correlations, outliers, clusters
    # DATA REQUIREMENTS: Exactly 2 numeric columns (continuous data), optional 1 categorical column for color grouping
    # SCATTER PLOT SPECIFICS: Points represent individual data records, both axes are numeric scales, good for 20+ data points
    # =============================================================================
    
    import pandas as pd
    import json
    from jinja2 import Template

    # Load common utilities for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    @fused.cache
    def load_data():
        """
        SCATTER PLOT DATA REQUIREMENTS:
        - Must have 2 numeric columns with continuous values
        - Both X and Y should have reasonable spread (not constant values)
        - Remove/handle missing values in both numeric columns
        - Works best with 20+ data points to show patterns
        """
        return (
            pd.read_csv(
                "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
            )
            .dropna(subset=["bill_length_mm", "bill_depth_mm"])  # SCATTER PLOT REQUIREMENT: Clean both numeric columns
        )

    df = load_data()

    # SCATTER PLOT REQUIREMENT: Select X column, Y column, and optional category column
    chart_data = df[["bill_length_mm", "bill_depth_mm", "species"]]  # [x_col, y_col, optional_category_col]
    data_json = chart_data.to_json(orient="records")

    # SCATTER PLOT CONFIGURATION
    config = {
        # CORE SCATTER PLOT SETTINGS
        "xField": "bill_length_mm",              # The X-axis numeric variable
        "yField": "bill_depth_mm",               # The Y-axis numeric variable  
        "categoryField": "species",              # Optional: for color-coded grouping of points
        
        # POINT APPEARANCE
        "pointRadius": 5,                        # Point size: 3-8 typical, larger for fewer points
        "colorPalette": ["#F97316", "#059669", "#6B7280"],  # Colors for categories (if used)
        
        # LABELS
        "xAxisLabel": "Bill Length (mm)",        # What the X variable represents
        "yAxisLabel": "Bill Depth (mm)",         # What the Y variable represents
        "chartTitle": "Penguin Measurements",
        
        # LAYOUT
        "margin": {"top": 50, "right": 100, "bottom": 60, "left": 60},  # Extra right margin for legend
    }

    config_json = json.dumps(config)

    html_template = Template(
        """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{{ config.chartTitle }}</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    :root {
      --accent: #F97316;
      --secondary: #059669;
      --mid: #6B7280;
      --bg: #FAFAFA;
      --contrast: #1F2937;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--contrast);
      font-family: var(--font);
      height: 100vh;
      overflow: hidden;
    }
    svg { width: 100%; height: 100%; display: block; }
    .dot { cursor: pointer; transition: r 0.12s; }
    .dot:hover { stroke: var(--accent); stroke-width: 2px; }
    .axis text { fill: var(--contrast); font-size: 11px; }
    .axis path, .axis line { stroke: var(--mid); }
    .title { fill: var(--accent); font-size: 16px; font-weight: 700; text-anchor: middle; }
    .axis-label { fill: var(--contrast); font-size: 12px; text-anchor: middle; }
    .tooltip {
      position: absolute; pointer-events: none;
      padding: 8px 10px; background: var(--contrast); color: var(--bg);
      border-radius: 4px; font-size: 12px; opacity: 0; transition: opacity 0.12s;
    }
    .legend { font-size: 12px; fill: var(--contrast); }
  </style>
</head>
<body>
  <svg></svg>
  <div class="tooltip"></div>

  <script>
    const data = {{ data_json | safe }};
    const CONFIG = {{ config_json | safe }};

    const svg = d3.select("svg");
    const tooltip = d3.select(".tooltip");

    function draw() {
      const w = svg.node().clientWidth;
      const h = svg.node().clientHeight;
      const margin = CONFIG.margin;
      const width = w - margin.left - margin.right;
      const height = h - margin.top - margin.bottom;

      svg.selectAll("*").remove();
      const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

      // SCATTER PLOT SCALES: Both X and Y are continuous numeric scales
      const x = d3.scaleLinear()
        .domain(d3.extent(data, d => d[CONFIG.xField])).nice()  // nice() creates clean axis ticks
        .range([0, width]);

      const y = d3.scaleLinear()
        .domain(d3.extent(data, d => d[CONFIG.yField])).nice()
        .range([height, 0]);  // Invert Y for screen coordinates

      // Color scale for categories (if present)
      const categories = Array.from(new Set(data.map(d => d[CONFIG.categoryField] || "")));
      const color = d3.scaleOrdinal().domain(categories).range(CONFIG.colorPalette);

      svg.append("text").attr("class", "title").attr("x", w/2).attr("y", 28).text(CONFIG.chartTitle);

      g.append("g").attr("transform", `translate(0,${height})`).call(d3.axisBottom(x));
      g.append("g").call(d3.axisLeft(y));

      g.append("text").attr("class", "axis-label").attr("x", width/2).attr("y", height + 45).text(CONFIG.xAxisLabel);
      g.append("text").attr("class", "axis-label").attr("transform", "rotate(-90)").attr("x", -height/2).attr("y", -45).text(CONFIG.yAxisLabel);

      // SCATTER PLOT POINTS: Each circle represents one data record
      g.selectAll("circle")
        .data(data)
        .enter()
        .append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d[CONFIG.xField]))      // X position based on X variable
        .attr("cy", d => y(d[CONFIG.yField]))      // Y position based on Y variable
        .attr("r", CONFIG.pointRadius)             // Fixed radius for all points
        .attr("fill", d => color(d[CONFIG.categoryField]))  // Color by category
        .on("mouseover", function(event, d) {
          // Scatter plot tooltip: show both X and Y values plus category
          tooltip.style("opacity", 1)
            .html(
              `<strong>${CONFIG.categoryField}:</strong> ${d[CONFIG.categoryField] || ""}<br/>
               <strong>${CONFIG.xAxisLabel}:</strong> ${d[CONFIG.xField]}<br/>
               <strong>${CONFIG.yAxisLabel}:</strong> ${d[CONFIG.yField]}`
            );
        })
        .on("mousemove", function(event) {
          tooltip.style("left", (event.pageX + 10) + "px").style("top", (event.pageY - 28) + "px");
        })
        .on("mouseout", function() {
          tooltip.style("opacity", 0);
        });

      // LEGEND: Show color coding for categories (if multiple categories exist)
      if (categories.length > 1) {
        const legend = svg.append("g").attr("transform", `translate(${w - margin.right + 10}, ${margin.top})`);
        categories.forEach((c, i) => {
          const item = legend.append("g").attr("transform", `translate(0, ${i * 20})`);
          item.append("rect").attr("width", 10).attr("height", 10).attr("fill", color(c));
          item.append("text").attr("x", 16).attr("y", 8).attr("fill", "var(--contrast)").style("font-size", "12px").text(c);
        });
      }
    }

    draw();
    window.addEventListener("resize", draw);
  </script>
</body>
</html>
"""
    )

    html_content = html_template.render(data_json=data_json, config_json=config_json, config=config)
    return common.html_to_obj(html_content)