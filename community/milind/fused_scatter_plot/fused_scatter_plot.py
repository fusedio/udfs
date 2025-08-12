@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Scatter Plot with Tooltips
    # WHEN TO USE: Show relationship between TWO numeric variables with optional grouping
    # DATA REQUIREMENTS: 2 numeric columns (X, Y), optional 1 categorical column for color
    # FEATURES: Hover tooltips, color-coded categories, responsive design, professional styling
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 
    # 1. DATA ADAPTATION:
    #    - Update load_data() function with your data source
    #    - Required columns: [x_column, y_column] and optional [category_column]
    #    - Update data_json with your column names
    #    - Update all "UPDATE_DATA" marked field accessors in JavaScript
    #    - Update CONFIG object with your field names and labels
    #
    # 2. STYLING & BRANDING:
    #    - Colors: Update CSS variables in :root section
    #    - Point colors: Modify colorPalette array in CONFIG
    #    - Fonts: Change --font-family in CSS variables
    #    - Theme: Modify --fused-yellow, --fused-bg, --fused-text colors
    #    - Dimensions: Adjust margin object and point radius
    #
    # 3. INTERACTION BEHAVIOR:
    #    - Tooltip content: Update tooltip.html() with your fields
    #    - Hover effects: Modify .dot:hover styles and transitions
    #    - Point size: Change CONFIG.pointRadius
    #    - Scale domains: Update scales for custom ranges
    # =============================================================================

    import pandas as pd
    
    # Load common utilities - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    # -------------------------------------------------------------------------
    # DATA LOADING SECTION
    # AI INSTRUCTION: Replace this entire section with your data loading logic
    # -------------------------------------------------------------------------
    @fused.cache
    def load_data():
        """
        Load dataset for scatter plot analysis
        
        AI REQUIREMENTS:
        - Must return pandas DataFrame
        - Must have 2 numeric columns for X and Y axes
        - Optional: 1 categorical column for color grouping
        
        EXAMPLE REPLACEMENTS:
        - return pd.read_csv("your_file.csv")
        - return pd.read_parquet("s3://bucket/data.parquet")
        - return your_database_query()
        """
        # Load penguins dataset
        return pd.read_csv(
            "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        ).dropna(subset=["bill_length_mm", "bill_depth_mm"])

    df = load_data()
    
    # -------------------------------------------------------------------------
    # DATA PREPARATION SECTION  
    # AI INSTRUCTION: Update column names to match your dataset
    # -------------------------------------------------------------------------
    # Select only the columns needed for the chart (reduces payload size)
    chart_data = df[["bill_length_mm", "bill_depth_mm", "species"]]  # UPDATE_DATA: Your [x_col, y_col, category_col]
    data_json = chart_data.to_json(orient="records")
    
    # -------------------------------------------------------------------------
    # HTML TEMPLATE WITH STANDARDIZED STRUCTURE
    # AI INSTRUCTION: Search for "UPDATE_DATA" comments to modify field accessors
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Interactive Scatter Plot with Tooltips</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        /* =================================================================== */
        /* STYLING SECTION - AI: Modify these CSS variables for branding */
        /* =================================================================== */
        :root {{
            --fused-yellow: #E5FF44;
            --fused-bg: #141414;
            --fused-text: #FFFFFF;
            --fused-gray: #333333;
            --fused-gray-light: #666666;
            --fused-dark: #0a0a0a;
            --font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }}
        
        body {{
            margin: 0;
            background: var(--fused-bg);
            color: var(--fused-text);
            font-family: var(--font-family);
            height: 100vh;
            overflow: hidden;
        }}
        
        svg {{ width: 100%; height: 100%; }}
        
        /* Point styling with hover effects */
        .dot {{
            stroke: none;
            cursor: pointer;
            transition: all 0.2s ease;
            opacity: 0.85;
        }}
        
        .dot:hover {{
            stroke: var(--fused-yellow);
            stroke-width: 2px;
            r: 7;
            opacity: 1;
        }}
        
        /* Axis styling */
        .axis {{ color: var(--fused-text); }}
        .axis text {{ 
            fill: var(--fused-text); 
            font-size: 11px; 
            font-family: var(--font-family);
        }}
        .axis path, .axis line {{ 
            stroke: var(--fused-gray-light); 
        }}
        
        /* Grid lines */
        .grid-line {{ 
            stroke: var(--fused-gray); 
            stroke-dasharray: 2,2; 
            opacity: 0.5; 
        }}
        
        /* Labels and title */
        .axis-label {{ 
            fill: var(--fused-text); 
            font-size: 13px; 
            font-weight: 500; 
            text-anchor: middle; 
        }}
        
        .title {{ 
            fill: var(--fused-yellow); 
            font-size: 18px; 
            font-weight: bold; 
            text-anchor: middle; 
            letter-spacing: 0.5px;
        }}
        
        /* Tooltip */
        .tooltip {{
            position: absolute;
            padding: 10px 12px;
            background: rgba(0, 0, 0, 0.9);
            color: var(--fused-text);
            border-radius: 4px;
            pointer-events: none;
            font-size: 12px;
            opacity: 0;
            transition: opacity 0.2s ease;
            box-shadow: 0 0 10px rgba(229, 255, 68, 0.2);
            border: 1px solid var(--fused-gray);
            font-family: var(--font-family);
            line-height: 1.4;
        }}
        
        /* Legend */
        .legend {{
            font-size: 12px;
            font-family: var(--font-family);
        }}
        
        .legend-item {{
            cursor: pointer;
            transition: opacity 0.2s ease;
        }}
        
        .legend-item:hover {{
            opacity: 0.7;
        }}
    </style>
</head>
<body>
    <svg></svg>
    <div class="tooltip"></div>

    <script>
        // =================================================================
        // DATA AND CONFIGURATION SECTION
        // AI INSTRUCTION: Update field accessors marked with UPDATE_DATA
        // =================================================================
        
        const data = {data_json};
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        
        // Layout constants - AI: Adjust margins as needed
        const margin = {{ top: 60, right: 120, bottom: 70, left: 80 }};
        
        // Chart configuration - AI: Modify these for your data
        const CONFIG = {{
            xField: "bill_length_mm",              // UPDATE_DATA: Your X-axis column name
            yField: "bill_depth_mm",               // UPDATE_DATA: Your Y-axis column name
            categoryField: "species",              // UPDATE_DATA: Your category column (optional)
            pointRadius: 5,                        // Size of scatter plot points
            xAxisLabel: "Bill Length (mm)",        // UPDATE_DATA: Your X axis label
            yAxisLabel: "Bill Depth (mm)",         // UPDATE_DATA: Your Y axis label
            chartTitle: "Penguin Measurements Analysis", // UPDATE_DATA: Your title
            colorPalette: ["#E5FF44", "#ff7f0e", "#2ca02c", "#9467bd", "#8c564b"], // Point colors
            showGrid: true,                        // Show/hide grid lines
            showLegend: true                       // Show/hide legend
        }};
        
        // =================================================================
        // MAIN DRAWING FUNCTION
        // =================================================================
        function draw() {{
            const svgWidth = svg.node().clientWidth;
            const svgHeight = svg.node().clientHeight;
            const width = svgWidth - margin.left - margin.right;
            const height = svgHeight - margin.top - margin.bottom;
            
            // Clear previous content
            svg.selectAll("*").remove();
            
            // Create main group
            const g = svg.append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);
            
            // =============================================================
            // SCALES SETUP
            // =============================================================
            
            // Create X scale
            const x = d3.scaleLinear()
                .domain(d3.extent(data, d => d[CONFIG.xField]))  // UPDATE_DATA: X field accessor
                .range([0, width])
                .nice();
                
            // Create Y scale
            const y = d3.scaleLinear()
                .domain(d3.extent(data, d => d[CONFIG.yField]))  // UPDATE_DATA: Y field accessor
                .range([height, 0])
                .nice();
                
            // Create color scale
            const color = d3.scaleOrdinal()
                .domain([...new Set(data.map(d => d[CONFIG.categoryField]))])  // UPDATE_DATA: Category field accessor
                .range(CONFIG.colorPalette);
            
            // =============================================================
            // TITLE AND AXES
            // =============================================================
            
            // Add title
            svg.append("text")
                .attr("class", "title")
                .attr("x", svgWidth / 2)
                .attr("y", 30)
                .text(CONFIG.chartTitle);
            
            // Add X axis
            const xAxis = g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${{height}})`)
                .call(d3.axisBottom(x));
            
            // Add Y axis
            const yAxis = g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y));
            
            // Add grid lines if enabled
            if (CONFIG.showGrid) {{
                g.append("g")
                    .attr("class", "grid")
                    .attr("transform", `translate(0,${{height}})`)
                    .call(d3.axisBottom(x)
                        .tickSize(-height)
                        .tickFormat("")
                    )
                    .selectAll("line")
                    .attr("class", "grid-line");
                
                g.append("g")
                    .attr("class", "grid")
                    .call(d3.axisLeft(y)
                        .tickSize(-width)
                        .tickFormat("")
                    )
                    .selectAll("line")
                    .attr("class", "grid-line");
            }}
            
            // Add X axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 45)
                .text(CONFIG.xAxisLabel);
            
            // Add Y axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -50)
                .text(CONFIG.yAxisLabel);
            
            // =============================================================
            // SCATTER PLOT POINTS WITH INTERACTIONS
            // =============================================================
            
            // Add scatter plot points
            const dots = g.selectAll("circle")
                .data(data)
                .enter()
                .append("circle")
                .attr("class", "dot")
                .attr("cx", d => x(d[CONFIG.xField]))      // UPDATE_DATA: X field accessor
                .attr("cy", d => y(d[CONFIG.yField]))      // UPDATE_DATA: Y field accessor
                .attr("r", CONFIG.pointRadius)
                .attr("fill", d => color(d[CONFIG.categoryField]))  // UPDATE_DATA: Category field accessor
                .on("mouseover", function(event, d) {{
                    // UPDATE_DATA: Customize tooltip content for your data
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", 0.9);
                    tooltip.html(`
                        <strong>Species:</strong> ${{d[CONFIG.categoryField]}}<br/>
                        <strong>${{CONFIG.xAxisLabel}}:</strong> ${{d[CONFIG.xField]}} mm<br/>
                        <strong>${{CONFIG.yAxisLabel}}:</strong> ${{d[CONFIG.yField]}} mm
                    `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");
                }})
                .on("mouseout", function(d) {{
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                }});
            
            // =============================================================
            // LEGEND (if enabled)
            // =============================================================
            
            if (CONFIG.showLegend && CONFIG.categoryField) {{
                const legend = svg.append("g")
                    .attr("class", "legend")
                    .attr("transform", `translate(${{svgWidth - 100}}, ${{margin.top + 20}})`);
                    
                const legendItems = legend.selectAll(".legend-item")
                    .data(color.domain())
                    .enter()
                    .append("g")
                    .attr("class", "legend-item")
                    .attr("transform", (d, i) => `translate(0, ${{i * 20}})`);
                    
                legendItems.append("circle")
                    .attr("cx", 0)
                    .attr("cy", 0)
                    .attr("r", 6)
                    .attr("fill", d => color(d));
                    
                legendItems.append("text")
                    .attr("x", 15)
                    .attr("y", 0)
                    .attr("dy", "0.35em")
                    .attr("fill", "var(--fused-text)")
                    .style("font-size", "12px")
                    .text(d => d);
            }}
        }}
        
        // Initialize and handle window resize
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)