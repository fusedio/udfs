@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Histogram with Tooltips
    # WHEN TO USE: Show distribution of ONE numeric variable with optional grouping
    # DATA REQUIREMENTS: At least 1 numeric column, optional 1 categorical column
    # FEATURES: Hover tooltips, responsive design, smooth transitions
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 
    # 1. DATA ADAPTATION:
    #    - Update load_data() function with your data source
    #    - Required columns: [numeric_column] and optional [category_column]
    #    - Update data_json with your column names
    #    - Update all "UPDATE_DATA" marked field accessors in JavaScript
    #    - Update CONFIG object with your field names and labels
    #
    # 2. STYLING & BRANDING:
    #    - Colors: Update CSS variables in :root section
    #    - Fonts: Change --font-family in CSS variables
    #    - Theme: Modify --bg-color, --text-color, --primary-color
    #    - Dimensions: Adjust margin object for spacing
    #    - Bar appearance: Modify .bar styles and hover effects
    #
    # 3. INTERACTION BEHAVIOR:
    #    - Tooltip content: Update tooltip.html() with your fields
    #    - Hover effects: Modify .bar:hover styles and transitions
    #    - Bin count: Change CONFIG.numBins for more/fewer bars
    #    - Domain: Update xScale.domain() for custom ranges
    # =============================================================================

    import pandas as pd
    import numpy as np
    
    # Load common utilities - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    # -------------------------------------------------------------------------
    # DATA LOADING SECTION
    # AI INSTRUCTION: Replace this entire section with your data loading logic
    # -------------------------------------------------------------------------
    @fused.cache
    def load_data():
        """
        Load dataset for histogram analysis
        
        AI REQUIREMENTS:
        - Must return pandas DataFrame
        - Must have at least 1 numeric column for histogram
        - Optional: 1 categorical column for grouping/coloring
        
        EXAMPLE REPLACEMENTS:
        - return pd.read_csv("your_file.csv")
        - return pd.read_parquet("s3://bucket/data.parquet")
        - return your_database_query()
        """
        # Load the classic Iris dataset
        url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
        column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
        return pd.read_csv(url, names=column_names)

    df = load_data()
    
    # -------------------------------------------------------------------------
    # DATA PREPARATION SECTION  
    # AI INSTRUCTION: Update column names to match your dataset
    # -------------------------------------------------------------------------
    # Select only the columns needed for the chart (reduces payload size)
    chart_data = df[["sepal_length", "species"]]  # UPDATE_DATA: Your [numeric_col, category_col]
    data_json = chart_data.to_json(orient="records")
    
    # -------------------------------------------------------------------------
    # HTML TEMPLATE WITH STANDARDIZED STRUCTURE
    # AI INSTRUCTION: Search for "UPDATE_DATA" comments to modify field accessors
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Interactive Histogram with Tooltips</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        /* =================================================================== */
        /* STYLING SECTION - AI: Modify these CSS variables for branding */
        /* =================================================================== */
        :root {{
            --bg-color: #1a1a1a;
            --text-color: #ffffff;
            --primary-color: #E8FF59;
            --secondary-color: #ff7f0e;
            --accent-color: #2ca02c;
            --grid-color: #333333;
            --tooltip-bg: rgba(0, 0, 0, 0.8);
            --font-family: -apple-system, BlinkMacSystemFont, sans-serif;
        }}
        
        body {{
            margin: 0;
            background: var(--bg-color);
            color: var(--text-color);
            font-family: var(--font-family);
            height: 100vh;
            overflow: hidden;
        }}
        
        svg {{ width: 100%; height: 100%; }}
        
        /* Bar styling with hover effects */
        .bar {{
            fill: var(--primary-color);
            stroke: none;
            opacity: 1;
            cursor: pointer;
            transition: opacity 0.2s ease;
        }}
        
        .bar:hover {{
            opacity: 0.4;
            stroke: var(--text-color);
            stroke-width: 1px;
        }}
        
        /* Axis styling */
        .axis {{ color: var(--text-color); }}
        .axis text {{ fill: var(--text-color); font-size: 12px; }}
        .axis path, .axis line {{ stroke: var(--text-color); }}
        
        /* Labels and title */
        .axis-label {{ 
            fill: var(--text-color); 
            font-size: 14px; 
            font-weight: bold; 
            text-anchor: middle; 
        }}
        
        .title {{ 
            fill: var(--text-color); 
            font-size: 18px; 
            font-weight: bold; 
            text-anchor: middle; 
        }}
        
        /* Tooltip */
        .tooltip {{
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
        const margin = {{ top: 60, right: 30, bottom: 60, left: 70 }};
        
        // Chart configuration - AI: Modify these for your data
        const CONFIG = {{
            numericField: "sepal_length",           // UPDATE_DATA: Your numeric column name
            categoryField: "species",               // UPDATE_DATA: Your category column (optional)
            numBins: 15,                           // Number of histogram bins
            domainMin: 4,                          // UPDATE_DATA: Min value for X axis (or use d3.extent)
            domainMax: 8,                          // UPDATE_DATA: Max value for X axis
            xAxisLabel: "Sepal Length (cm)",       // UPDATE_DATA: Your X axis label
            yAxisLabel: "Count",                   // Y axis label
            chartTitle: "Iris Sepal Length Distribution" // UPDATE_DATA: Your title
        }};
        
        // =================================================================
        // MAIN DRAWING FUNCTION
        // =================================================================
        function draw() {{
            const width = svg.node().clientWidth - margin.left - margin.right;
            const height = svg.node().clientHeight - margin.top - margin.bottom;
            
            // Clear previous content
            svg.selectAll("*").remove();
            
            // Create main group
            const g = svg.append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);
            
            // =============================================================
            // SCALES AND HISTOGRAM SETUP
            // =============================================================
            
            // Create X scale
            const x = d3.scaleLinear()
                .domain([CONFIG.domainMin, CONFIG.domainMax])  // UPDATE_DATA: Adjust domain
                .range([0, width]);
                
            // Create histogram bins
            const bins = d3.histogram()
                .value(d => d[CONFIG.numericField])  // UPDATE_DATA: Your numeric field accessor
                .domain(x.domain())
                .thresholds(x.ticks(CONFIG.numBins))
                (data);
            
            // Create Y scale
            const y = d3.scaleLinear()
                .domain([0, d3.max(bins, d => d.length)])
                .range([height, 0]);
            
            // =============================================================
            // TITLE AND AXES
            // =============================================================
            
            // Add title
            svg.append("text")
                .attr("class", "title")
                .attr("x", svg.node().clientWidth / 2)
                .attr("y", 30)
                .text(CONFIG.chartTitle);
            
            // Add X axis
            g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${{height}})`)
                .call(d3.axisBottom(x));
            
            // Add Y axis
            g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y));
            
            // Add X axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 40)
                .text(CONFIG.xAxisLabel);
            
            // Add Y axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -40)
                .text(CONFIG.yAxisLabel);
            
            // =============================================================
            // HISTOGRAM BARS WITH INTERACTIONS
            // =============================================================
            
            // Add bars with tooltips
            g.selectAll("rect")
                .data(bins)
                .enter()
                .append("rect")
                .attr("class", "bar")
                .attr("x", d => x(d.x0))
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
                .attr("y", d => y(d.length))
                .attr("height", d => height - y(d.length))
                .on("mouseover", function(event, d) {{
                    // UPDATE_DATA: Customize tooltip content for your data
                    const rangeText = `${{d.x0.toFixed(1)}} - ${{d.x1.toFixed(1)}} cm`;
                    tooltip
                        .style("opacity", 1)
                        .html(`
                            <strong>Range:</strong> ${{rangeText}}<br>
                            <strong>Count:</strong> ${{d.length}} flowers<br>
                            <strong>Percentage:</strong> ${{(d.length/data.length*100).toFixed(1)}}%
                        `)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                }})
                .on("mouseout", function() {{
                    tooltip.style("opacity", 0);
                }});
        }}
        
        // Initialize and handle window resize
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)