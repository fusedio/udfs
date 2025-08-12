@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Scatter Plot with Brushing Selection
    # WHEN TO USE: Show relationship between TWO numeric variables with interactive selection
    # DATA REQUIREMENTS: 2 numeric columns (X, Y), optional 1 categorical column for color
    # FEATURES: 2D brushing selection, hover tooltips, color-coded categories, responsive design
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
    #    - Theme: Modify --bg-color, --text-color, --primary-color
    #    - Brush styling: Update brush appearance and selection colors
    #
    # 3. INTERACTION BEHAVIOR:
    #    - Brushing: Modify updateBrush() function for different selection behaviors
    #    - Tooltip content: Update tooltip.html() with your fields
    #    - Hover effects: Modify .dot:hover styles and transitions
    #    - Selection feedback: Adjust highlighted/faded point styling
    # =============================================================================

    import pandas as pd
    
    # Load common utilities - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    # -------------------------------------------------------------------------
    # DATA LOADING SECTION
    # AI INSTRUCTION: Replace this entire section with your data loading logic
    # -------------------------------------------------------------------------
    @fused.cache
    def load_data():
        """
        Load dataset for scatter plot with brushing analysis
        
        AI REQUIREMENTS:
        - Must return pandas DataFrame
        - Must have 2 numeric columns for X and Y axes
        - Optional: 1 categorical column for color grouping
        
        EXAMPLE REPLACEMENTS:
        - return pd.read_csv("your_file.csv")
        - return pd.read_parquet("s3://bucket/data.parquet")
        - return your_database_query()
        """
        # Load and clean penguin dataset
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
    <title>Interactive Scatter Plot with Brushing Selection</title>
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
            --grid-light: #666666;
            --tooltip-bg: rgba(0, 0, 0, 0.9);
            --brush-stroke: #ffffff;
            --brush-fill: rgba(255, 255, 255, 0.1);
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
        
        /* Point styles - normal, highlighted, and faded states */
        .dot {{
            cursor: pointer;
            transition: all 0.2s ease;
            opacity: 0.7;
        }}
        
        .dot.faded {{
            opacity: 0.1 !important;
            stroke: none !important;
        }}
        
        .dot.highlighted {{
            opacity: 1 !important;
            stroke: var(--text-color) !important;
            stroke-width: 2px !important;
        }}
        
        /* Axis styling */
        .axis {{ color: var(--text-color); }}
        .axis text {{ fill: var(--text-color); font-size: 11px; }}
        .axis path, .axis line {{ stroke: var(--grid-light); }}
        
        /* Grid lines */
        .grid-line {{
            stroke: var(--grid-color);
            stroke-dasharray: 2,2;
        }}
        
        /* Labels and title */
        .axis-label {{ 
            fill: var(--text-color); 
            font-size: 13px; 
            font-weight: 500; 
            text-anchor: middle;
        }}
        
        .title {{ 
            fill: var(--text-color); 
            font-size: 16px; 
            font-weight: bold; 
            text-anchor: middle;
        }}
        
        /* Brush styling */
        .brush .extent {{
            stroke: var(--brush-stroke);
            fill: var(--brush-fill);
            shape-rendering: crispEdges;
        }}
        
        /* Tooltip */
        .tooltip {{
            position: absolute;
            padding: 8px 12px;
            background: var(--tooltip-bg);
            color: var(--text-color);
            border-radius: 4px;
            pointer-events: none;
            font-size: 12px;
            border: 1px solid var(--grid-color);
            max-width: 200px;
            opacity: 0;
            transition: opacity 0.2s ease;
        }}
        
        /* Instructions panel */
        .instructions {{
            position: absolute;
            top: 10px;
            right: 10px;
            background: var(--tooltip-bg);
            color: var(--text-color);
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 11px;
            border: 1px solid var(--grid-color);
        }}
        
        /* Simple selection display under title */
        .selection-display {{
            fill: var(--text-color);
            font-size: 12px;
            text-anchor: middle;
            opacity: 0;
            transition: opacity 0.2s ease;
        }}
        
        .selection-display.visible {{
            opacity: 1;
        }}
    </style>
</head>
<body>
    <div class="instructions">
        Drag to select points • Double-click to clear selection
    </div>
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
        const margin = {{ top: 70, right: 80, bottom: 60, left: 70 }};
        
        // Chart configuration - AI: Modify these for your data
        const CONFIG = {{
            xField: "bill_length_mm",              // UPDATE_DATA: Your X-axis column name
            yField: "bill_depth_mm",               // UPDATE_DATA: Your Y-axis column name
            categoryField: "species",              // UPDATE_DATA: Your category column (optional)
            pointRadius: 4,                        // Size of scatter plot points
            pointRadiusHover: 6,                   // Size when hovering
            xAxisLabel: "Bill Length (mm)",        // UPDATE_DATA: Your X axis label
            yAxisLabel: "Bill Depth (mm)",         // UPDATE_DATA: Your Y axis label
            chartTitle: "Penguin Bill Measurements by Species (Brush to Select)", // UPDATE_DATA: Your title
            colorPalette: ["#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"], // Point colors
            showGrid: true,                        // Show/hide grid lines
            enableBrushing: true,                  // Enable/disable brush selection
            transitionDuration: 200                // Animation duration in ms
        }};
        
        // =================================================================
        // MAIN DRAWING FUNCTION
        // =================================================================
        function draw() {{
            const svgWidth = svg.node().clientWidth;
            const svgHeight = svg.node().clientHeight;
            const width = svgWidth - margin.left - margin.right;
            const height = svgHeight - margin.top - margin.bottom;
            
            // Clear previous drawing
            svg.selectAll("*").remove();
            
            // Create main group
            const g = svg.append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);
            
            // =============================================================
            // TITLE AND SELECTION DISPLAY
            // =============================================================
            svg.append("text")
                .attr("class", "title")
                .attr("x", svgWidth / 2)
                .attr("y", 25)
                .text(CONFIG.chartTitle);
            
            // Add selection display under title
            const selectionDisplay = svg.append("text")
                .attr("class", "selection-display")
                .attr("x", svgWidth / 2)
                .attr("y", 45);
            
            // =============================================================
            // SCALES SETUP
            // =============================================================
            
            // Create X scale
            const xScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d[CONFIG.xField]))  // UPDATE_DATA: X field accessor
                .range([0, width])
                .nice();
                
            // Create Y scale  
            const yScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d[CONFIG.yField]))  // UPDATE_DATA: Y field accessor
                .range([height, 0])
                .nice();
            
            // Create color scale
            const colorScale = d3.scaleOrdinal()
                .domain([...new Set(data.map(d => d[CONFIG.categoryField]))])  // UPDATE_DATA: Category field accessor
                .range(CONFIG.colorPalette);
            
            // =============================================================
            // AXES WITH OPTIONAL GRID
            // =============================================================
            
            const xAxis = g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${{height}})`)
                .call(d3.axisBottom(xScale).tickSize(CONFIG.showGrid ? -height : 0).tickSizeOuter(0));
                
            const yAxis = g.append("g")
                .attr("class", "axis")  
                .call(d3.axisLeft(yScale).tickSize(CONFIG.showGrid ? -width : 0).tickSizeOuter(0));
            
            // Style grid lines
            if (CONFIG.showGrid) {{
                g.selectAll(".axis .tick line").attr("class", "grid-line");
            }}
            
            // Axis labels
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 45)
                .text(CONFIG.xAxisLabel);
                
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -50)
                .text(CONFIG.yAxisLabel);
            
            // =============================================================
            // SCATTER PLOT POINTS WITH INTERACTIONS
            // =============================================================
            
            const dots = g.selectAll(".dot")
                .data(data)
                .enter()
                .append("circle")
                .attr("class", "dot")
                .attr("cx", d => xScale(d[CONFIG.xField]))      // UPDATE_DATA: X field accessor
                .attr("cy", d => yScale(d[CONFIG.yField]))      // UPDATE_DATA: Y field accessor
                .attr("r", CONFIG.pointRadius)
                .attr("fill", d => colorScale(d[CONFIG.categoryField]))  // UPDATE_DATA: Category field accessor
                .attr("stroke", "none");
            
            // Tooltip interactions
            dots
                .on("mouseover", function(event, d) {{
                    tooltip.transition()
                        .duration(CONFIG.transitionDuration)
                        .style("opacity", 1);
                    
                    // UPDATE_DATA: Customize tooltip content for your data
                    tooltip.html(`
                        <strong>Species:</strong> ${{d[CONFIG.categoryField]}}<br/>
                        <strong>${{CONFIG.xAxisLabel}}:</strong> ${{d[CONFIG.xField]}} mm<br/>
                        <strong>${{CONFIG.yAxisLabel}}:</strong> ${{d[CONFIG.yField]}} mm
                    `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");
                    
                    // Enlarge point on hover
                    d3.select(this)
                        .transition()
                        .duration(100)
                        .attr("r", CONFIG.pointRadiusHover)
                        .attr("stroke", "#fff")
                        .attr("stroke-width", 1);
                }})
                .on("mouseout", function(d) {{
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                    
                    // Reset point size if not highlighted by brush
                    if (!d3.select(this).classed("highlighted")) {{
                        d3.select(this)
                            .transition()
                            .duration(100)
                            .attr("r", CONFIG.pointRadius)
                            .attr("stroke", "none");
                    }}
                }});
            
            // =============================================================
            // BRUSHING FUNCTIONALITY
            // AI INSTRUCTION: Modify updateBrush() to change selection behavior
            // =============================================================
            
            if (CONFIG.enableBrushing) {{
                const brush = d3.brush()
                    .extent([[0, 0], [width, height]])
                    .on("start brush end", updateBrush);
                
                const brushGroup = g.append("g")
                    .attr("class", "brush")
                    .call(brush);
                
                // Brush event handler
                function updateBrush(event) {{
                    const selection = event.selection;
                    
                    if (!selection) {{
                        // No selection - reset all points and hide selection display
                        dots
                            .classed("highlighted", false)
                            .classed("faded", false)
                            .transition()
                            .duration(CONFIG.transitionDuration)
                            .attr("opacity", 0.7);
                        
                        selectionDisplay.classed("visible", false);
                        return;
                    }}
                    
                    // Get selected points
                    let selectedCount = 0;
                    
                    dots.each(function(d) {{
                        const x = xScale(d[CONFIG.xField]);     // UPDATE_DATA: X field accessor
                        const y = yScale(d[CONFIG.yField]);     // UPDATE_DATA: Y field accessor
                        
                        const isSelected = x >= selection[0][0] && x <= selection[1][0] && 
                                         y >= selection[0][1] && y <= selection[1][1];
                        
                        if (isSelected) selectedCount++;
                        
                        d3.select(this)
                            .classed("highlighted", isSelected)
                            .classed("faded", !isSelected);
                    }});
                    
                    // Update simple selection display
                    selectionDisplay
                        .text(`${{selectedCount}} points selected`)
                        .classed("visible", true);
                }}
                
                // Double click to clear selection
                svg.on("dblclick", function() {{
                    brushGroup.call(brush.clear);
                    selectionDisplay.classed("visible", false);
                    updateBrush({{ selection: null }});
                }});
            }}
        }}
        
        // Initialize and handle window resize
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
"""

    # Return HTML object using common helper
    return common.html_to_obj(html_content)