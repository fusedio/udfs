@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Multi-Line Time Series Chart - GitHub Repository Stars
    # WHEN TO USE: Compare cumulative star growth across multiple repositories over time
    # DATA REQUIREMENTS: Time variable (date) + numeric value (cumulative_stars) + repository grouping variable
    # MULTI-LINE SPECIFICS: Each line represents one repository, same time scale, different colors per repo
    # =============================================================================
    
    import duckdb
    import pandas as pd
    import json
    from jinja2 import Template
    
    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_github_data():
        """
        MULTI-LINE CHART DATA REQUIREMENTS:
        - Must have temporal variable (date)
        - Must have numeric value (cumulative_stars)
        - Must have grouping variable (repository)
        - Each repository should have data points across the time range
        """
        
        df = duckdb.sql("""
            SELECT
                repository,
                make_date(year, month, 1) AS date,
                SUM(total_stars) OVER (PARTITION BY repository ORDER BY year, month) AS cumulative_stars
            FROM 's3://fused-sample/github_star_100.parquet'
            WHERE repository IN ('pola-rs/polars', 'pandas-dev/pandas', 'marimo-team/marimo')
            ORDER BY date
        """).df()
        
        return df
    
    df = load_github_data()
    
    # Convert date to year for cleaner x-axis
    df['year'] = pd.to_datetime(df['date']).dt.year + (pd.to_datetime(df['date']).dt.dayofyear / 365.25)
    
    # MULTI-LINE CHART REQUIREMENT: Select time, value, and grouping variables
    chart_data = df[['year', 'cumulative_stars', 'repository']].copy()
    data_json = chart_data.to_json(orient="records")
    
    # MULTI-LINE CHART CONFIGURATION
    config = {
        # CORE MULTI-LINE FIELDS
        "timeField": "year",                    # Temporal variable (X-axis)
        "valueField": "cumulative_stars",      # Numeric variable (Y-axis)
        "groupField": "repository",            # Grouping variable (determines line separation)
        
        # MULTI-LINE APPEARANCE
        "lineWidth": 2.5,                      # Thickness of lines
        "colorPalette": [                      # Colors for different repositories
            '#1f77b4', '#ff7f0e', '#2ca02c'
        ],
        "showDataPoints": False,               # Show/hide individual data points on lines
        "pointRadius": 2,                      # Size of data points (if shown)
        "curveType": "curveMonotoneX",       # Line interpolation
        
        # INTERACTION
        "showLegend": True,                    # Show/hide legend (essential for multi-line charts)
        "enableHover": True,                   # Enable hover effects on lines
        
        # LABELS  
        "xAxisLabel": "Year",                  # Time axis label
        "yAxisLabel": "Cumulative Stars",     # Value axis label
        "chartTitle": "GitHub Repository Star Growth",
        
        # LAYOUT
        "margin": {"top": 60, "right": 150, "bottom": 80, "left": 80}
    }
    
    # Theme configuration
    theme = {
        "bg_color": "#FAFAFA",
        "text_color": "#333333",
        "axis_color": "#666666", 
        "grid_color": "#e0e0e0",
        "tooltip_bg": "white",
        "tooltip_border": "solid 2px black",
        "font_family": "Arial, sans-serif"
    }
    
    # Metadata
    repositories = df['repository'].unique()
    metadata = {
        "title": config["chartTitle"],
        "subtitle": f"Star growth comparison for {len(repositories)} major repositories",
        "num_groups": len(repositories),
        "time_range": f"{df[config['timeField']].min():.1f} - {df[config['timeField']].max():.1f}"
    }

    html_template = Template("""
<!DOCTYPE html>
<html>
<head>
    <title>{{ metadata.title }}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body { 
            margin: 20px; 
            background: {{ theme.bg_color }}; 
            color: {{ theme.text_color }};
            font-family: {{ theme.font_family }};
        }
        svg { width: 100%; height: calc(100vh - 120px); }
        
        .line { 
            fill: none; 
            stroke-width: {{ config.lineWidth }}px;
            transition: stroke-width 0.2s ease;
        }
        
        .line:hover { 
            stroke-width: {{ config.lineWidth + 1 }}px;
        }
        
        .data-point { 
            cursor: pointer; 
            transition: all 0.2s ease; 
        }
        .data-point:hover { 
            stroke: {{ theme.text_color }}; 
            stroke-width: 2px; 
            r: {{ config.pointRadius + 2 }};
        }
        
        .axis text { 
            fill: {{ theme.text_color }}; 
            font-size: 12px; 
            font-family: {{ theme.font_family }};
        }
        .axis path, .axis line { stroke: {{ theme.axis_color }}; }
        .grid-line { 
            stroke: {{ theme.grid_color }}; 
            stroke-width: 0.5; 
            opacity: 0.7; 
        }
        
        .axis-label { 
            fill: {{ theme.text_color }}; 
            font-size: 14px; 
            text-anchor: middle; 
            font-family: {{ theme.font_family }};
        }
        
        .tooltip {
            position: absolute;
            text-align: center;
            padding: 8px;
            font-size: 12px;
            background: {{ theme.tooltip_bg }};
            border: {{ theme.tooltip_border }};
            border-radius: 5px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.2s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            font-family: {{ theme.font_family }};
        }
        
        .legend { 
            font-size: 12px; 
            font-family: {{ theme.font_family }};
        }
        .legend-item { 
            cursor: pointer; 
            transition: opacity 0.2s ease; 
        }
        .legend-item:hover { opacity: 0.7; }
        .legend text { fill: {{ theme.text_color }}; }
        
        .chart-title {
            font-size: 22px;
            font-weight: bold;
            margin-bottom: 5px;
            color: {{ theme.text_color }};
        }
        .chart-subtitle {
            font-size: 14px;
            color: {{ theme.text_color }};
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="chart-title">{{ metadata.title }}</div>
    <div class="chart-subtitle">{{ metadata.subtitle }}</div>
    <svg></svg>
    <div class="tooltip"></div>

    <script>
        const data = {{ data_json | safe }};
        const CONFIG = {{ config | tojson }};
        
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        
        function draw() {
            const svgWidth = svg.node().clientWidth;
            const svgHeight = svg.node().clientHeight;
            const width = svgWidth - CONFIG.margin.left - CONFIG.margin.right;
            const height = svgHeight - CONFIG.margin.top - CONFIG.margin.bottom;
            
            svg.selectAll("*").remove();
            
            const g = svg.append("g")
                .attr("transform", "translate(" + CONFIG.margin.left + "," + CONFIG.margin.top + ")");
            
            // MULTI-LINE DATA GROUPING: Group data by repository
            const groupedData = d3.group(data, d => d[CONFIG.groupField]);
            
            // Convert to array format for easier processing
            const nestedData = Array.from(groupedData, ([key, values]) => ({
                key: key,
                values: values.sort((a, b) => a[CONFIG.timeField] - b[CONFIG.timeField])
            }));
            
            // MULTI-LINE SCALES: Shared scales across all repositories
            const x = d3.scaleLinear()
                .domain(d3.extent(data, d => +d[CONFIG.timeField]))
                .range([0, width]);
                
            const y = d3.scaleLinear()
                .domain(d3.extent(data, d => +d[CONFIG.valueField]))
                .range([height, 0])
                .nice();
            
            // MULTI-LINE COLOR SCALE: Different color for each repository
            const repositories = Array.from(new Set(data.map(d => d[CONFIG.groupField])));
            const color = d3.scaleOrdinal()
                .domain(repositories)
                .range(CONFIG.colorPalette);
            
            // Axes
            g.append("g")
                .attr("class", "axis")
                .attr("transform", "translate(0," + height + ")")
                .call(d3.axisBottom(x).ticks(8).tickFormat(d => d.toFixed(1)));
            
            g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y).ticks(6).tickFormat(d => d.toLocaleString()));
            
            // Axis labels
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 50)
                .text(CONFIG.xAxisLabel);
            
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -50)
                .text(CONFIG.yAxisLabel);
            
            // MULTI-LINE GENERATOR: Create line function
            const line = d3.line()
                .x(d => x(+d[CONFIG.timeField]))
                .y(d => y(+d[CONFIG.valueField]))
                .curve(d3[CONFIG.curveType]);
            
            // DRAW MULTIPLE LINES: One line per repository
            const lines = g.selectAll(".line")
                .data(nestedData)
                .enter()
                .append("path")
                .attr("class", "line")
                .attr("d", d => line(d.values))
                .attr("stroke", d => color(d.key))
                .attr("fill", "none");
            
            // MULTI-LINE LEGEND: Essential for identifying different repositories
            if (CONFIG.showLegend) {
                const legend = svg.append("g")
                    .attr("class", "legend")
                    .attr("transform", "translate(" + (svgWidth - CONFIG.margin.right + 20) + "," + CONFIG.margin.top + ")");
                    
                const legendItems = legend.selectAll(".legend-item")
                    .data(repositories)
                    .enter()
                    .append("g")
                    .attr("class", "legend-item")
                    .attr("transform", (d, i) => "translate(0," + (i * 25) + ")");
                    
                // Legend lines (mini versions of actual lines)
                legendItems.append("line")
                    .attr("x1", 0)
                    .attr("x2", 20)
                    .attr("y1", 0)
                    .attr("y2", 0)
                    .attr("stroke", d => color(d))
                    .attr("stroke-width", CONFIG.lineWidth);
                    
                legendItems.append("text")
                    .attr("x", 25)
                    .attr("y", 0)
                    .attr("dy", "0.35em")
                    .style("font-size", "12px")
                    .text(d => d);
            }
            
            console.log("Multi-line GitHub stars chart created: " + nestedData.length + " repositories, " + 
                       data.length + " total data points");
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
        metadata=metadata,
        theme=theme
    )
    
    return common.html_to_obj(html_content)