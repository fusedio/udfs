@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Scatter Plot with Dark Theme
    # WHEN TO USE: Analyze relationships between TWO continuous variables, identify correlations, detect outliers, explore data clusters
    # DATA REQUIREMENTS: Exactly 2 numeric columns for X/Y positioning, optional categorical column for color grouping
    # SCATTER PLOT SPECIFICS: Each point = one data record, both axes are continuous scales, reveals linear/non-linear relationships
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
        - Must have 2 continuous numeric columns (X and Y variables)
        - Both variables should have reasonable spread/variance
        - Remove missing values from both numeric columns
        - Works best with 50+ points to identify patterns
        - Avoid too many points (>10k) which can cause overplotting
        """
        return (
            pd.read_csv(
                "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
            )
            .dropna(subset=["bill_length_mm", "bill_depth_mm"])  # SCATTER REQUIREMENT: Clean both X and Y columns
        )

    df = load_data()

    # SCATTER PLOT REQUIREMENT: Select X variable, Y variable, and optional grouping variable
    chart_data = df[["bill_length_mm", "bill_depth_mm", "species"]]  # [x_var, y_var, optional_category]
    data_json = chart_data.to_json(orient="records")

    # SCATTER PLOT CONFIGURATION
    config = {
        # CORE SCATTER PLOT VARIABLES
        "xField": "bill_length_mm",              # X-axis continuous variable
        "yField": "bill_depth_mm",               # Y-axis continuous variable  
        "categoryField": "species",              # Optional: grouping variable for color coding
        
        # POINT APPEARANCE
        "pointRadius": 5,                        # Point size (3-8 typical range)
        "colorPalette": ["#E5FF44", "#ff7f0e", "#2ca02c"],  # Colors for different categories
        
        # LABELS
        "xAxisLabel": "Bill Length (mm)",        # What X variable represents
        "yAxisLabel": "Bill Depth (mm)",         # What Y variable represents
        "chartTitle": "Penguin Measurements",
        
        # LAYOUT
        "margin": {"top": 50, "right": 100, "bottom": 60, "left": 60},  # Space for legend on right
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
      --bg: #141414;
      --text: #ffffff;
      --accent: #E5FF44;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: var(--font);
      height: 100vh;
      overflow: hidden;
    }
    svg { width: 100%; height: 100%; display: block; }
    .dot { opacity: 0.9; cursor: pointer; transition: r 0.12s; }
    .dot:hover { stroke: var(--accent); stroke-width: 1.5px; opacity: 1; }
    .axis text { fill: var(--text); font-size: 11px; }
    .title { fill: var(--accent); font-size: 16px; font-weight: 700; text-anchor: middle; }
    .axis-label { fill: var(--text); font-size: 12px; text-anchor: middle; }
    .tooltip {
      position: absolute; pointer-events: none;
      padding: 8px 10px; background: rgba(0,0,0,0.85); color: var(--text);
      border-radius: 4px; font-size: 12px; opacity: 0; transition: opacity 0.12s;
      box-shadow: 0 0 10px rgba(0,0,0,0.3);
    }
    .legend { font-size: 12px; fill: var(--text); }
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

      // SCATTER PLOT SCALES: Both X and Y are continuous linear scales
      const x = d3.scaleLinear()
        .domain(d3.extent(data, d => d[CONFIG.xField])).nice()  // X domain from data range
        .range([0, width]);

      const y = d3.scaleLinear()
        .domain(d3.extent(data, d => d[CONFIG.yField])).nice()  // Y domain from data range
        .range([height, 0]);  // Invert Y for screen coordinates

      // Color scale for categorical grouping
      const categories = Array.from(new Set(data.map(d => d[CONFIG.categoryField] || "")));
      const color = d3.scaleOrdinal().domain(categories).range(CONFIG.colorPalette);

      // Chart title
      svg.append("text").attr("class", "title").attr("x", w/2).attr("y", 28).text(CONFIG.chartTitle);

      // Axes with continuous scales
      g.append("g").attr("transform", `translate(0,${height})`).call(d3.axisBottom(x));
      g.append("g").call(d3.axisLeft(y));

      // Axis labels showing what variables are being compared
      g.append("text").attr("class", "axis-label").attr("x", width/2).attr("y", height + 45).text(CONFIG.xAxisLabel);
      g.append("text").attr("class", "axis-label").attr("transform", "rotate(-90)").attr("x", -height/2).attr("y", -45).text(CONFIG.yAxisLabel);

      // SCATTER PLOT POINTS: Each circle represents one data record's X,Y position
      g.selectAll("circle")
        .data(data)
        .enter()
        .append("circle")
        .attr("class", "dot")
        .attr("cx", d => x(d[CONFIG.xField]))      // X position from X variable
        .attr("cy", d => y(d[CONFIG.yField]))      // Y position from Y variable
        .attr("r", CONFIG.pointRadius)             // Fixed radius for all points
        .attr("fill", d => color(d[CONFIG.categoryField]));  // Color by category

      // SCATTER PLOT INTERACTIONS: Show data values for each point
      g.selectAll(".dot")
        .on("mouseover", function(event, d) {
          // Tooltip shows both X and Y values plus category
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

      // LEGEND: Shows color coding for categories (if present)
      if (categories.length > 1) {
        const legend = svg.append("g").attr("transform", `translate(${w - margin.right + 10}, ${margin.top})`);
        categories.forEach((c, i) => {
          const item = legend.append("g").attr("transform", `translate(0, ${i * 20})`);
          item.append("rect").attr("width", 10).attr("height", 10).attr("fill", color(c));
          item.append("text").attr("x", 16).attr("y", 8).attr("fill", "var(--text)").style("font-size", "12px").text(c);
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