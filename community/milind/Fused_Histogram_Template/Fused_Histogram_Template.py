@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Histogram with Dark Theme
    # WHEN TO USE: Show distribution of ONE continuous variable, identify data shape (normal, skewed, bimodal), detect outliers
    # DATA REQUIREMENTS: Single numeric column with continuous values, 30+ data points for meaningful distribution
    # HISTOGRAM SPECIFICS: Bins continuous data into frequency counts, shows data concentration, reveals distribution patterns
    # =============================================================================
    
    import pandas as pd
    import json
    from jinja2 import Template
    
    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_data():
        """
        HISTOGRAM DATA REQUIREMENTS:
        - Must have continuous numeric variable (not categorical)
        - Remove missing values before binning
        - Need sufficient data points (30+) for meaningful bins
        - Avoid extreme outliers that skew bin ranges
        """
        url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
        column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
        return pd.read_csv(url, names=column_names)

    df = load_data()
    
    # HISTOGRAM REQUIREMENT: Select continuous numeric column
    chart_data = df[["sepal_length", "species"]]  # [numeric_col, optional_category_col]
    data_json = chart_data.to_json(orient="records")
    
    # HISTOGRAM CONFIGURATION
    config = {
        # CORE HISTOGRAM SETTINGS
        "numericField": "sepal_length",          # The continuous variable to analyze
        "categoryField": "species",              # Optional: for context (not used in single histogram)
        
        # BIN CONFIGURATION
        "numBins": 15,                          # Number of bins: affects granularity of distribution
        "domainMin": 4,                         # Fixed minimum for consistent binning
        "domainMax": 8,                         # Fixed maximum for consistent binning
        
        # LABELS
        "xAxisLabel": "Sepal Length (cm)",      # What the variable represents
        "yAxisLabel": "Count",                  # Always "Count" or "Frequency" for histograms
        "chartTitle": "Iris Sepal Length Distribution"
    }
    
    # Dark theme configuration
    theme = {
        "bg_color": "#1a1a1a",
        "text_color": "#ffffff", 
        "primary_color": "#E8FF59",             # Single color for histogram bars
        "secondary_color": "#ff7f0e",
        "accent_color": "#2ca02c",
        "grid_color": "#333333",
        "tooltip_bg": "rgba(0, 0, 0, 0.8)",
        "font_family": "-apple-system, BlinkMacSystemFont, sans-serif"
    }
    
    html_template = Template("""
<!DOCTYPE html>
<html>
<head>
    <title>{{ config.chartTitle }}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        :root {
            --bg-color: {{ theme.bg_color }};
            --text-color: {{ theme.text_color }};
            --primary-color: {{ theme.primary_color }};
            --secondary-color: {{ theme.secondary_color }};
            --accent-color: {{ theme.accent_color }};
            --grid-color: {{ theme.grid_color }};
            --tooltip-bg: {{ theme.tooltip_bg }};
            --font-family: {{ theme.font_family }};
        }
        
        body {
            margin: 0;
            background: var(--bg-color);
            color: var(--text-color);
            font-family: var(--font-family);
            height: 100vh;
            overflow: hidden;
        }
        
        svg { width: 100%; height: 100%; }
        
        .bar {
            fill: var(--primary-color);
            stroke: none;
            opacity: 1;
            cursor: pointer;
            transition: opacity 0.2s ease;
        }
        
        .bar:hover {
            opacity: 0.4;
            stroke: var(--text-color);
            stroke-width: 1px;
        }
        
        .axis { color: var(--text-color); }
        .axis text { fill: var(--text-color); font-size: 12px; }
        .axis path, .axis line { stroke: var(--text-color); }
        
        .axis-label { 
            fill: var(--text-color); 
            font-size: 14px; 
            font-weight: bold; 
            text-anchor: middle; 
        }
        
        .title { 
            fill: var(--text-color); 
            font-size: 18px; 
            font-weight: bold; 
            text-anchor: middle; 
        }
        
        .tooltip {
            position: absolute;
            padding: 10px;
            background: var(--tooltip-bg);
            color: var(--text-color);
            border-radius: 5px;
            pointer-events: none;
            font-size: 14px;
            opacity: 0;
            transition: opacity 0.2s ease;
            box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        }
    </style>
</head>
<body>
    <svg></svg>
    <div class="tooltip"></div>

    <script>
        const data = {{ data_json | safe }};
        const CONFIG = {{ config | tojson }};
        
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        const margin = { top: 60, right: 30, bottom: 60, left: 70 };
        
        function draw() {
            const width = svg.node().clientWidth - margin.left - margin.right;
            const height = svg.node().clientHeight - margin.top - margin.bottom;
            
            svg.selectAll("*").remove();
            
            const g = svg.append("g")
                .attr("transform", `translate(${margin.left},${margin.top})`);
            
            // HISTOGRAM X SCALE: Continuous scale for numeric variable
            const x = d3.scaleLinear()
                .domain([CONFIG.domainMin, CONFIG.domainMax])  // Fixed domain for consistent binning
                .range([0, width]);
                
            // HISTOGRAM BINNING: Core logic that converts continuous data to frequency counts
            const bins = d3.histogram()
                .value(d => d[CONFIG.numericField])    // Extract numeric values
                .domain(x.domain())                    // Use same domain as scale
                .thresholds(x.ticks(CONFIG.numBins))   // Create evenly-spaced bin boundaries
                (data);
            
            // FREQUENCY SCALE: Y-axis shows count of data points in each bin
            const y = d3.scaleLinear()
                .domain([0, d3.max(bins, d => d.length)])  // 0 to maximum bin count
                .range([height, 0]);
            
            // Chart title
            svg.append("text")
                .attr("class", "title")
                .attr("x", svg.node().clientWidth / 2)
                .attr("y", 30)
                .text(CONFIG.chartTitle);
            
            // X axis shows the range of the continuous variable
            g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${height})`)
                .call(d3.axisBottom(x));
            
            // Y axis shows frequency counts
            g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y));
            
            // Axis labels
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 40)
                .text(CONFIG.xAxisLabel);
            
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -40)
                .text(CONFIG.yAxisLabel);
            
            // HISTOGRAM BARS: Each bar represents frequency of data in that range
            g.selectAll("rect")
                .data(bins)
                .enter()
                .append("rect")
                .attr("class", "bar")
                .attr("x", d => x(d.x0))                           // Left edge of bin
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))  // Bin width minus gap
                .attr("y", d => y(d.length))                       // Top of bar based on count
                .attr("height", d => height - y(d.length))         // Bar height represents frequency
                .on("mouseover", function(event, d) {
                    // HISTOGRAM TOOLTIP: Show bin range, count, and percentage of total
                    const rangeText = `${d.x0.toFixed(1)} - ${d.x1.toFixed(1)} cm`;
                    tooltip
                        .style("opacity", 1)
                        .html(`
                            <strong>Range:</strong> ${rangeText}<br>
                            <strong>Count:</strong> ${d.length} flowers<br>
                            <strong>Percentage:</strong> ${(d.length/data.length*100).toFixed(1)}%
                        `)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", function() {
                    tooltip.style("opacity", 0);
                });
        }
        
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
    """)
    
    html_content = html_template.render(
        data_json=data_json,
        config=config,
        theme=theme
    )
    
    return common.html_to_obj(html_content)