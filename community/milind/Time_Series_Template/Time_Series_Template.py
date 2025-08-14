@fused.udf
def udf(year: int = 2024):
    # =============================================================================
    # CHART TYPE: Interactive Time Series Line Chart
    # WHEN TO USE: Show trends over time, identify patterns/seasonality, track changes in continuous variables across temporal periods
    # DATA REQUIREMENTS: Time-ordered data with date/time column + numeric value column, chronological sequence important
    # TIME SERIES SPECIFICS: X-axis is temporal scale, connected line shows continuity, reveals trends/cycles/anomalies over time
    # =============================================================================
    
    import pandas as pd
    import json
    from jinja2 import Template
    from datetime import datetime, timedelta
    
    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_temperature_data(path):
        """
        TIME SERIES DATA REQUIREMENTS:
        - Must have temporal column (dates, weeks, months, etc.)
        - Must have numeric value column for Y-axis
        - Data should be in chronological order
        - Handle missing time periods appropriately
        - Consistent time intervals work best (daily, weekly, monthly)
        """
        return pd.read_parquet(path)
    
    # Load temperature time series data for the specified year
    path = f"s3://fused-sample/demo_data/timeseries/{year}.pq"
    df = load_temperature_data(path)
    
    # TIME SERIES DATA PREPARATION: Convert temperature and create proper date column
    df['avg_temperature_celsius'] = df['avg_temperature'] - 273.15
    
    # TIME SERIES REQUIREMENT: Convert week numbers to actual dates for proper time scale
    df['date'] = df.apply(lambda row: datetime(int(row['year']), 1, 1) + timedelta(weeks=int(row['week'])-1), axis=1)
    df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')  # Format for JavaScript Date parsing
    
    # Sort by date to ensure proper time series ordering
    df = df.sort_values('date')
    
    # TIME SERIES DATA STRUCTURE: Select temporal and value columns
    chart_data = df[['date_str', 'avg_temperature_celsius', 'week']].copy()
    data_json = chart_data.to_json(orient="records")
    
    # TIME SERIES CONFIGURATION
    config = {
        # CORE TIME SERIES FIELDS
        "dateField": "date_str",                    # Temporal column (formatted for JavaScript)
        "valueField": "avg_temperature_celsius",    # Numeric value to plot over time
        "contextField": "week",                     # Additional context for tooltips
        
        # TIME SERIES APPEARANCE
        "lineWidth": 2,                             # Line thickness
        "useGradient": True,                        # Enable gradient coloring based on values
        "gradientColors": ["blue", "red"],          # Colors for low to high values
        "showDataPoints": False,                    # Show/hide individual data points on line
        "pointRadius": 3,                           # Size of data points (if shown)
        
        # LABELS
        "xAxisLabel": "Date",                       # Time axis label
        "yAxisLabel": "Temperature (°C)",           # Value axis label
        "chartTitle": f"Temperature Trends {year}",
        
        # LAYOUT
        "margin": {"top": 60, "right": 30, "bottom": 80, "left": 70}
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
    metadata = {
        "title": config["chartTitle"],
        "subtitle": f"Weekly average temperature data showing temporal patterns",
        "year": year,
        "data_points": len(df)
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
            
            // TIME SERIES DATA PARSING: Convert date strings to JavaScript Date objects
            data.forEach(d => {
                d.date = new Date(d[CONFIG.dateField]);
                d.value = +d[CONFIG.valueField];
            });
            
            // TIME SERIES SCALES: Time scale for X-axis, linear scale for values
            const x = d3.scaleTime()
                .domain(d3.extent(data, d => d.date))  // Time domain from earliest to latest date
                .range([0, width]);
                
            const y = d3.scaleLinear()
                .domain(d3.extent(data, d => d.value))  // Value domain from min to max
                .range([height, 0])
                .nice();
            
            // TIME SERIES AXES
            g.append("g")
                .attr("class", "axis")
                .attr("transform", "translate(0," + height + ")")
                .call(d3.axisBottom(x));
            
            g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y));
            
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
                .attr("y", -45)
                .text(CONFIG.yAxisLabel);
            
            // GRADIENT DEFINITION: Color line based on value range
            if (CONFIG.useGradient) {
                const gradient = svg.append("defs")
                    .append("linearGradient")
                    .attr("id", "line-gradient")
                    .attr("gradientUnits", "userSpaceOnUse")
                    .attr("x1", 0)
                    .attr("y1", y(d3.min(data, d => d.value)))
                    .attr("x2", 0)
                    .attr("y2", y(d3.max(data, d => d.value)));
                
                gradient.selectAll("stop")
                    .data([
                        {offset: "0%", color: CONFIG.gradientColors[0]},
                        {offset: "100%", color: CONFIG.gradientColors[1]}
                    ])
                    .enter().append("stop")
                    .attr("offset", d => d.offset)
                    .attr("stop-color", d => d.color);
            }
            
            // TIME SERIES LINE: Connect data points in chronological order
            const line = d3.line()
                .x(d => x(d.date))     // X position based on date
                .y(d => y(d.value))    // Y position based on value
                .curve(d3.curveMonotoneX);  // Smooth curve that preserves monotonicity
            
            g.append("path")
                .datum(data)
                .attr("class", "line")
                .attr("d", line)
                .attr("stroke", CONFIG.useGradient ? "url(#line-gradient)" : "#69b3a2");
            
            // OPTIONAL DATA POINTS: Individual points on the line
            if (CONFIG.showDataPoints) {
                g.selectAll(".data-point")
                    .data(data)
                    .enter()
                    .append("circle")
                    .attr("class", "data-point")
                    .attr("cx", d => x(d.date))
                    .attr("cy", d => y(d.value))
                    .attr("r", CONFIG.pointRadius)
                    .attr("fill", "#69b3a2")
                    .on("mouseover", function(event, d) {
                        // TIME SERIES TOOLTIP: Show date and value
                        const dateStr = d.date.toLocaleDateString();
                        tooltip.style("opacity", 1)
                            .html("<strong>Date:</strong> " + dateStr + "<br/>" +
                                  "<strong>Week:</strong> " + d[CONFIG.contextField] + "<br/>" +
                                  "<strong>Temperature:</strong> " + d.value.toFixed(1) + "°C")
                            .style("left", (event.pageX + 10) + "px")
                            .style("top", (event.pageY - 28) + "px");
                    })
                    .on("mouseout", function() {
                        tooltip.style("opacity", 0);
                    });
            }
            
            console.log("Time series chart created: " + data.length + " data points from " + 
                       data[0].date.getFullYear() + " week " + data[0][CONFIG.contextField] + 
                       " to week " + data[data.length-1][CONFIG.contextField]);
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