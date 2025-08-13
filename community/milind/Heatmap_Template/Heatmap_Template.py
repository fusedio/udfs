@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Correlation Heatmap
    # WHEN TO USE: Show relationships between multiple numeric variables, identify variable clusters, detect multicollinearity
    # DATA REQUIREMENTS: Multiple numeric columns (3+ variables), works best with 5-15 variables for readability
    # CORRELATION HEATMAP SPECIFICS: Symmetric matrix, diagonal = 1.0, values range -1 to +1, color encodes correlation strength
    # =============================================================================
    
    import pandas as pd
    import numpy as np
    from jinja2 import Template
    
    # Load common utilities (required to return HTML object in Workbench)
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_data():
        """
        CORRELATION HEATMAP DATA REQUIREMENTS:
        - Must have multiple numeric columns (3+ variables minimum)
        - Numeric columns should have reasonable variance (not constant values)
        - Remove/handle missing values before correlation calculation
        - Works best with 5-15 variables (too many = hard to read labels)
        """
        # Example default: sklearn wine dataset
        from sklearn.datasets import load_wine
        wine = load_wine()
        df = pd.DataFrame(wine.data, columns=wine.feature_names)
        return df
    
    df = load_data()
    
    # CORRELATION ANALYSIS CONFIGURATION
    data_cfg = {
        "num_columns": 10,              # Limit variables for readability
        "method": "pearson"             # pearson, spearman, or kendall correlation
    }
    
    # CORRELATION HEATMAP REQUIREMENT: Select numeric columns only
    numeric_cols = df.select_dtypes(include=[np.number]).columns[: data_cfg["num_columns"]]
    
    # CORE CORRELATION CALCULATION: Creates symmetric matrix
    corr = df[numeric_cols].corr(method=data_cfg["method"])
    
    # HEATMAP DATA STRUCTURE: Convert correlation matrix to grid format
    # Each cell represents correlation between row variable and column variable
    heatmap_records = []
    for i, r in enumerate(corr.columns):
        for j, c in enumerate(corr.columns):
            heatmap_records.append({
                "row": r, 
                "col": c, 
                "value": float(corr.iat[i, j])  # Correlation value between -1 and 1
            })
    
    data_json = pd.DataFrame(heatmap_records).to_json(orient="records")
    
    # CORRELATION HEATMAP CONFIGURATION
    config = {
        # GRID STRUCTURE
        "padding": 0.06,                        # Space between cells
        "cellRadius": 3,                        # Rounded corners for cells
        "maxCell": 60,                          # Max cell size for large screens
        
        # CORRELATION COLOR ENCODING
        "colorInterpolator": "interpolateRdYlBu",  # Color scheme: red(-1) -> yellow(0) -> blue(+1)
        "colorDomain": [-1, 1],                 # Always -1 to +1 for correlations
        "correlationPrecision": 2               # Decimal places for correlation values
    }
    
    # Minimal theme values
    theme = {
        "font_family": "Arial, Helvetica, sans-serif",
        "text_color": "#111",
        "subtitle_color": "#555",
        "tooltip_bg": "#fff",
        "tooltip_border": "1px solid #ccc"
    }
    
    metadata = {
        "title": "Correlation Heatmap",
        "subtitle": f"{data_cfg['method'].title()} correlations — {len(numeric_cols)} vars",
        "num_vars": len(numeric_cols)
    }

    html_template = Template("""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{{ metadata.title }}</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    body { font-family: {{ theme.font_family }}; margin:12px; color: {{ theme.text_color }}; }
    .title { font-size:18px; font-weight:700; margin-bottom:4px; }
    .subtitle { font-size:12px; color: {{ theme.subtitle_color }}; margin-bottom:8px; }
    #heatmap_wrap { width:100%; height: calc(60vh); min-height:260px; }
    svg { width:100%; display:block; }
    .tooltip {
      position: absolute;
      pointer-events: none;
      background: {{ theme.tooltip_bg }};
      border: {{ theme.tooltip_border }};
      padding:8px;
      border-radius:4px;
      font-size:12px;
      box-shadow: 0 2px 6px rgba(0,0,0,0.08);
      opacity:0;
      transition: opacity 0.12s;
    }
    .axis text { font-size:12px; fill: {{ theme.text_color }}; }
  </style>
</head>
<body>
  <div class="title">{{ metadata.title }}</div>
  <div class="subtitle">{{ metadata.subtitle }}</div>
  <div id="heatmap_wrap">
    <svg></svg>
  </div>
  <div class="tooltip" id="hm_tooltip"></div>
<script>
const data = {{ data_json | safe }};
const CONFIG = {{ config | tojson }};
const METADATA = {{ metadata | tojson }};

// CORRELATION MATRIX STRUCTURE: Extract variable names (same for rows and columns)
const groups = Array.from(new Set(data.map(d => d.row)));
const vars = Array.from(new Set(data.map(d => d.col)));
const n = Math.max(groups.length, vars.length);

const wrap = d3.select("#heatmap_wrap");
const svg = d3.select("#heatmap_wrap svg");
const tooltip = d3.select("#hm_tooltip");

// CORRELATION COLOR SCALE: Maps correlation values to colors
const color = d3.scaleSequential()
    .interpolator(d3[CONFIG.colorInterpolator])
    .domain(CONFIG.colorDomain);  // Always -1 to +1 for correlations

// RESPONSIVE HEATMAP DRAWING FUNCTION
function draw() {
    svg.selectAll("*").remove();
    const bbox = wrap.node().getBoundingClientRect();
    const margin = { top: 30, right: 10, bottom: 80, left: 120 };
    const availableW = Math.max(120, bbox.width - margin.left - margin.right);
    
    // HEATMAP CELL SIZING: Calculate square cells that fit container
    const cellSize = Math.min(CONFIG.maxCell, Math.floor(availableW / n));
    const width = cellSize * n;
    const height = cellSize * n;  // Square matrix for correlations
    
    svg.attr("viewBox", `0 0 ${width + margin.left + margin.right} ${height + margin.top + margin.bottom}`)
       .attr("preserveAspectRatio", "xMidYMid meet");
    
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
    
    // CORRELATION MATRIX SCALES: Both axes use same variable names
    const x = d3.scaleBand().range([0, width]).domain(groups).padding(CONFIG.padding);
    const y = d3.scaleBand().range([0, height]).domain(vars).padding(CONFIG.padding);
    
    // Variable name labels on axes
    const xAxis = g.append("g")
        .attr("transform", `translate(0, ${height})`)
        .call(d3.axisBottom(x).tickSize(0))
        .selectAll("text")
        .style("text-anchor", "end")
        .attr("dx", "-0.6em")
        .attr("dy", "0.1em")
        .attr("transform", "rotate(-45)");
    
    g.append("g")
      .call(d3.axisLeft(y).tickSize(0));
    
    // CORRELATION HEATMAP CELLS: Each cell shows correlation between two variables
    g.selectAll("rect.cell")
      .data(data, d => d.row + ":" + d.col)
      .join("rect")
      .attr("class", "cell")
      .attr("x", d => x(d.row))
      .attr("y", d => y(d.col))
      .attr("rx", CONFIG.cellRadius)
      .attr("ry", CONFIG.cellRadius)
      .attr("width", Math.max(0, x.bandwidth()))
      .attr("height", Math.max(0, y.bandwidth()))
      .style("fill", d => color(d.value))  // Color encodes correlation strength
      .style("stroke", "none")
      .style("opacity", 0.95)
      .on("mousemove", function(event, d) {
          // CORRELATION TOOLTIP: Show variable pair and correlation value
          tooltip.style("opacity", 1)
                 .html(`<strong>${d.row} × ${d.col}</strong><br/>Correlation: ${d.value.toFixed(CONFIG.correlationPrecision)}`)
                 .style("left", (event.pageX + 10) + "px")
                 .style("top", (event.pageY - 28) + "px");
          d3.select(this).style("stroke", "#000").style("stroke-width", 1).style("opacity", 1);
      })
      .on("mouseout", function() {
          tooltip.style("opacity", 0);
          d3.select(this).style("stroke", "none").style("opacity", 0.95);
      });
    
    // Debug: Calculate average absolute correlation (excluding diagonal)
    const avg = d3.mean(data.filter(d => d.row !== d.col), d => Math.abs(d.value));
    console.info("Correlation Heatmap:", n, "vars — avg |corr|:", (avg || 0).toFixed(3));
}

// Initialize and make responsive
draw();
window.addEventListener("resize", draw);

// Additional responsiveness for embedded containers
if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(draw);
    ro.observe(document.getElementById("heatmap_wrap"));
}
</script>
</body>
</html>
    """)
    
    html_content = html_template.render(
        data_json=data_json,
        config=config,
        metadata=metadata,
        theme=theme
    )
    
    return common.html_to_obj(html_content)