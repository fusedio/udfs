@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: KDE (Kernel Density Estimation) + Histogram Overlay
    # WHEN TO USE: Analyze distribution shape, identify modes/peaks, compare to normal distribution, smooth noisy histograms
    # DATA REQUIREMENTS: Single continuous numeric variable, 50+ data points for reliable KDE, no missing values
    # KDE SPECIFICS: Shows empirical distribution (bars) + smooth estimate (curve), both use density scale, reveals distribution shape
    # =============================================================================
    
    import pandas as pd
    import numpy as np
    import json
    from scipy.stats import gaussian_kde
    from jinja2 import Template

    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    @fused.cache
    def load_data():
        """
        KDE + HISTOGRAM DATA REQUIREMENTS:
        - Must have continuous numeric variable (not categorical)
        - Minimum 50+ data points for reliable KDE curve
        - Remove missing values (KDE cannot handle NaN)
        - Data should have reasonable spread (not all same value)
        - Works best with 100+ points for smooth KDE curve
        """
        url = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
        df = pd.read_csv(url, sep=';')
        return df

    df = load_data()

    # KDE + HISTOGRAM ANALYSIS CONFIGURATION
    analysis_config = {
        "numeric_column": "alcohol",        # The continuous variable to analyze
        "hist_bins": 30,                   # Number of histogram bins (affects granularity)
        "kde_points": 120                  # Number of points for KDE curve (affects smoothness)
    }

    # KDE REQUIREMENT: Extract clean continuous data
    data = df[analysis_config["numeric_column"]].dropna().astype(float)
    
    # CORE KDE CALCULATION: Gaussian kernel density estimation
    kde = gaussian_kde(data)  # Creates smooth density function
    x_range = np.linspace(data.min(), data.max(), analysis_config["kde_points"])
    y_values = kde(x_range)   # Evaluate KDE at each point

    # KDE DATA STRUCTURE: Points for smooth curve
    kde_data = [{"x": float(x), "y": float(y)} for x, y in zip(x_range, y_values)]

    # HISTOGRAM CALCULATION: Empirical density (not raw counts)
    hist, bins = np.histogram(data, bins=analysis_config["hist_bins"], density=True)  # density=True for comparison with KDE
    
    # HISTOGRAM DATA STRUCTURE: Bars with density heights
    hist_data = [
        {
            "x0": float(bins[i]),           # Left edge of bin
            "x1": float(bins[i + 1]),       # Right edge of bin  
            "height": float(hist[i])        # Density value (not count)
        }
        for i in range(len(hist))
    ]

    metadata = {
        "title": "Distribution Analysis",
        "subtitle": f"{analysis_config['numeric_column']} (KDE + Histogram)",
        "x_axis_label": analysis_config["numeric_column"],
        "y_axis_label": "Density",  # Always "Density" for KDE+histogram, not "Count"
    }

    theme = {
        "bg_color": "#f6f7f9",
        "container_bg": "#ffffff",
        "hist_color": "#6baed6",           # Histogram bar color
        "kde_color": "#ff7f0e",            # KDE curve color
        "text_color": "#222"
    }

    # Serialize data for JavaScript
    kde_json = json.dumps(kde_data)
    hist_json = json.dumps(hist_data)

    html_template = Template("""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>{{ metadata.title }}</title>
<script src="https://d3js.org/d3.v7.min.js"></script>
<style>
  body { margin:0; font-family: Arial, sans-serif; background: {{ theme.bg_color }}; color: {{ theme.text_color }}; }
  .container { max-width: 920px; margin: 18px auto; background: {{ theme.container_bg }}; padding: 18px; border-radius:8px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
  .title { text-align:center; margin-bottom:12px; }
  .title h1 { margin:0; font-size: clamp(16px, 2.6vw, 22px); }
  .title p { margin:4px 0 0 0; color:#666; font-size: clamp(12px, 1.8vw, 14px); }
  .chart-container { width:100%; height:60vh; min-height:300px; }
  svg { width:100%; height:100%; display:block; }
  .bar { fill: {{ theme.hist_color }}; opacity:0.75; }
  .kde-line { fill:none; stroke: {{ theme.kde_color }}; stroke-width:2.4; }
  .axis { font-size:12px; color:#444; }
  @media (max-width:600px){ .chart-container { height:48vh; min-height:240px; } }
</style>
</head>
<body>
<div class="container">
  <div class="title">
    <h1>{{ metadata.title }}</h1>
    <p>{{ metadata.subtitle }}</p>
  </div>
  <div class="chart-container" id="chart"></div>
</div>

<script>
const kdeData = {{ kde_json | safe }};
const histData = {{ hist_json | safe }};

function render() {
  const container = document.getElementById('chart');
  const rect = container.getBoundingClientRect();
  const margin = {top: 18, right: 18, bottom: 48, left: 56};
  const width = Math.max(280, rect.width) - margin.left - margin.right;
  const height = Math.max(180, rect.height) - margin.top - margin.bottom;

  container.innerHTML = '';

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${height + margin.top + margin.bottom}`)
    .append('g')
    .attr('transform', `translate(${margin.left},${margin.top})`);

  // SHARED SCALE: Both histogram and KDE use same X domain (data range)
  const x = d3.scaleLinear()
    .domain(d3.extent(kdeData, d => d.x)).nice()
    .range([0, width]);

  // DENSITY SCALE: Y domain covers both histogram and KDE density values
  const y = d3.scaleLinear()
    .domain([0, d3.max([...kdeData.map(d=>d.y), ...histData.map(d=>d.height)]) * 1.08])
    .range([height, 0]);

  // Draw axes
  svg.append('g').attr('transform', `translate(0,${height})`).call(d3.axisBottom(x));
  svg.append('g').call(d3.axisLeft(y).ticks(5));

  // HISTOGRAM BARS: Empirical density distribution
  svg.selectAll('.bar')
    .data(histData)
    .enter()
    .append('rect')
    .attr('class','bar')
    .attr('x', d => x(d.x0))                                    // Left edge position
    .attr('width', d => Math.max(0, x(d.x1) - x(d.x0) - 1))   // Bar width minus gap
    .attr('y', d => y(d.height))                               // Top position based on density
    .attr('height', d => Math.max(0, height - y(d.height)));   // Height based on density value

  // KDE CURVE: Smooth density estimation overlaid on histogram
  const line = d3.line()
    .x(d => x(d.x))     // X position from data value
    .y(d => y(d.y))     // Y position from density estimate
    .curve(d3.curveBasis);  // Smooth curve interpolation
    
  svg.append('path')
    .datum(kdeData)
    .attr('class','kde-line')
    .attr('d', line);

  // Axis labels
  svg.append('text').attr('x', width/2).attr('y', height + 40).attr('text-anchor','middle').style('font-size','12px').text("{{ metadata.x_axis_label }}");
  svg.append('text').attr('transform','rotate(-90)').attr('x', -height/2).attr('y', -40).attr('text-anchor','middle').style('font-size','12px').text("{{ metadata.y_axis_label }}");
}

// Initialize and make responsive
render();
let t;
window.addEventListener('resize', ()=>{ clearTimeout(t); t = setTimeout(render, 200); });
</script>
</body>
</html>
""")

    html_content = html_template.render(
        kde_json=kde_json,
        hist_json=hist_json,
        metadata=metadata,
        theme=theme
    )

    return common.html_to_obj(html_content)