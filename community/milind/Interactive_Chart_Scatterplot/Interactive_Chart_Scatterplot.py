@fused.udf
def udf():
    # -----------------------------------------------------------------------------
    # Interactive D3 Scatter Plot with Brushing UDF
    #
    # WHAT THIS UDF DOES:
    # - Creates a responsive dark-themed scatter plot using D3.js
    # - Shows Penguin bill length vs bill depth, colored by species  
    # - Includes interactive brushing to select and highlight data points
    # - Points inside brush selection are highlighted, others fade out
    #
    # AI ADAPTATION INSTRUCTIONS:
    # 1. DATA CHANGES: To use different data, update these sections:
    #    - Change the @fused.cache function to load your dataset
    #    - Update data_json field list to match your column names
    #    - Update JavaScript field accessors (d.bill_length_mm, d.bill_depth_mm, d.species)
    #    - Update axis labels and chart title
    #
    # 2. STYLING CHANGES: To modify appearance, update these sections:
    #    - CSS styles in the <style> block (colors, fonts, sizes)
    #    - Color palette in the d3.scaleOrdinal().range() array
    #    - Point radius, margins, and chart dimensions
    #
    # 3. BRUSHING BEHAVIOR: Current setup highlights selected points and fades others.
    #    To modify: change the updateBrush() function logic
    #
    # 4. PERFORMANCE: For large datasets (10k+ points), consider:
    #    - Reducing point radius and removing hover effects
    #    - Sampling the data or using canvas instead of SVG
    # -----------------------------------------------------------------------------

    import pandas as pd
    
    # Load the common helper - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    # -------------------------------------------------------------------------
    # DATA LOADING: Cached function for performance
    # AI INSTRUCTION: Replace this function to load your own dataset
    # Your dataset should have at least 2 numeric columns and 1 categorical column
    # -------------------------------------------------------------------------
    @fused.cache
    def load_penguin_data():
        """
        Load and clean penguin dataset
        AI NOTE: Replace this with your data loading logic
        Example alternatives:
        - pd.read_csv("your_file.csv")
        - pd.read_parquet("s3://bucket/file.parquet") 
        - Database query results
        """
        return pd.read_csv(
            "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        ).dropna(subset=["bill_length_mm", "bill_depth_mm"])

    df = load_penguin_data()

    # -------------------------------------------------------------------------
    # PREPARE JSON DATA FOR CHART
    # AI INSTRUCTION: Update the column names in brackets to match your data
    # Only include columns needed for the chart to minimize payload size
    # -------------------------------------------------------------------------
    data_json = df[["bill_length_mm", "bill_depth_mm", "species"]].to_json(orient="records")

    # -------------------------------------------------------------------------
    # HTML TEMPLATE WITH EMBEDDED D3.js AND BRUSHING
    # AI INSTRUCTION: The JavaScript field accessors need to match your column names
    # Look for comments marked "UPDATE FOR YOUR DATA" below
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Interactive Penguin Scatter Plot with Brushing</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        /* STYLING - AI can modify these for different themes */
        body {{
            margin: 0;
            background: #1a1a1a;
            color: #fff;
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            height: 100vh;
            overflow: hidden;
        }}
        
        svg {{ width: 100%; height: 100%; }}
        
        /* Point styles - normal and selected states */
        .dot {{
            cursor: pointer;
            transition: all 0.2s ease;
        }}
        
        .dot.faded {{
            opacity: 0.1 !important;
            stroke: none !important;
        }}
        
        .dot.highlighted {{
            opacity: 1 !important;
            stroke: #fff !important;
            stroke-width: 2px !important;
        }}
        
        /* Axis styling */
        .axis {{ color: #fff; }}
        .axis text {{ fill: #fff; font-size: 11px; }}
        .axis path, .axis line {{ stroke: #666; }}
        
        /* Labels and title */
        .axis-label {{ fill: #fff; font-size: 13px; font-weight: 500; }}
        .title {{ fill: #fff; font-size: 16px; font-weight: bold; }}
        
        /* Brush styling */
        .brush .extent {{
            stroke: #fff;
            fill: rgba(255, 255, 255, 0.1);
            shape-rendering: crispEdges;
        }}
        
        /* Tooltip */
        .tooltip {{
            position: absolute;
            padding: 8px 12px;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            border-radius: 4px;
            pointer-events: none;
            font-size: 12px;
            border: 1px solid #333;
            max-width: 200px;
        }}
        
        /* Instructions */
        .instructions {{
            position: absolute;
            top: 10px;
            right: 10px;
            background: rgba(0, 0, 0, 0.8);
            color: white;
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 11px;
            border: 1px solid #333;
        }}
    </style>
</head>
<body>
    <div class="instructions">
        Drag to select points • Double-click to clear selection
    </div>
    <svg></svg>
    <div class="tooltip" style="opacity: 0;"></div>

    <script>
        // DATA INJECTION - AI NOTE: This comes from Python data_json variable
        const data = {data_json};
        
        // UI ELEMENTS
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        
        // LAYOUT CONSTANTS - AI can adjust these
        const margin = {{ top: 50, right: 80, bottom: 60, left: 70 }};
        
        // MAIN DRAWING FUNCTION
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
            
            // TITLE - AI UPDATE: Change title text for your data
            svg.append("text")
                .attr("class", "title")
                .attr("x", svgWidth / 2)
                .attr("y", 25)
                .style("text-anchor", "middle")
                .text("Penguin Bill Measurements by Species (Brush to Select)");
            
            // SCALES - AI UPDATE: Change field accessors for your data
            const xScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d.bill_length_mm))  // UPDATE FOR YOUR DATA
                .range([0, width])
                .nice();
                
            const yScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d.bill_depth_mm))   // UPDATE FOR YOUR DATA  
                .range([height, 0])
                .nice();
            
            // COLOR SCALE - AI UPDATE: Adjust colors and category field
            const colorScale = d3.scaleOrdinal()
                .domain([...new Set(data.map(d => d.species))])  // UPDATE FOR YOUR DATA
                .range(["#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]); // AI: Add more colors if needed
            
            // AXES
            const xAxis = g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${{height}})`)
                .call(d3.axisBottom(xScale).tickSize(-height).tickSizeOuter(0));
                
            const yAxis = g.append("g")
                .attr("class", "axis")  
                .call(d3.axisLeft(yScale).tickSize(-width).tickSizeOuter(0));
            
            // Make grid lines lighter
            g.selectAll(".axis .tick line")
                .style("stroke", "#333")
                .style("stroke-dasharray", "2,2");
            
            // AXIS LABELS - AI UPDATE: Change labels for your data
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 45)
                .style("text-anchor", "middle")
                .text("Bill Length (mm)");  // UPDATE FOR YOUR DATA
                
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -50)
                .style("text-anchor", "middle")
                .text("Bill Depth (mm)");   // UPDATE FOR YOUR DATA
            
            // CREATE POINTS - AI UPDATE: Change field accessors
            const dots = g.selectAll(".dot")
                .data(data)
                .enter()
                .append("circle")
                .attr("class", "dot")
                .attr("cx", d => xScale(d.bill_length_mm))      // UPDATE FOR YOUR DATA
                .attr("cy", d => yScale(d.bill_depth_mm))       // UPDATE FOR YOUR DATA
                .attr("r", 4)
                .attr("fill", d => colorScale(d.species))       // UPDATE FOR YOUR DATA
                .attr("opacity", 0.7)
                .attr("stroke", "none");
            
            // TOOLTIP INTERACTIONS - AI UPDATE: Modify tooltip content
            dots
                .on("mouseover", function(event, d) {{
                    tooltip.transition().duration(200).style("opacity", 1);
                    tooltip.html(`
                        <strong>Species:</strong> ${{d.species}}<br/>
                        <strong>Bill Length:</strong> ${{d.bill_length_mm}} mm<br/>
                        <strong>Bill Depth:</strong> ${{d.bill_depth_mm}} mm
                    `)  // UPDATE FOR YOUR DATA: Change tooltip fields
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");
                    
                    d3.select(this)
                        .transition().duration(100)
                        .attr("r", 6)
                        .attr("stroke", "#fff")
                        .attr("stroke-width", 1);
                }})
                .on("mouseout", function(d) {{
                    tooltip.transition().duration(500).style("opacity", 0);
                    
                    if (!d3.select(this).classed("highlighted")) {{
                        d3.select(this)
                            .transition().duration(100)
                            .attr("r", 4)
                            .attr("stroke", "none");
                    }}
                }});
            
            // BRUSHING SETUP
            const brush = d3.brush()
                .extent([[0, 0], [width, height]])
                .on("start brush end", updateBrush);
            
            const brushGroup = g.append("g")
                .attr("class", "brush")
                .call(brush);
            
            // BRUSH EVENT HANDLER
            function updateBrush(event) {{
                const selection = event.selection;
                
                if (!selection) {{
                    // No selection - reset all points
                    dots
                        .classed("highlighted", false)
                        .classed("faded", false)
                        .transition().duration(200)
                        .attr("opacity", 0.7);
                    return;
                }}
                
                // Check which points are in selection
                dots.each(function(d) {{
                    const x = xScale(d.bill_length_mm);     // UPDATE FOR YOUR DATA
                    const y = yScale(d.bill_depth_mm);      // UPDATE FOR YOUR DATA
                    
                    const isSelected = x >= selection[0][0] && x <= selection[1][0] && 
                                     y >= selection[0][1] && y <= selection[1][1];
                    
                    d3.select(this)
                        .classed("highlighted", isSelected)
                        .classed("faded", !isSelected);
                }});
            }}
            
            // DOUBLE CLICK TO CLEAR SELECTION
            svg.on("dblclick", function() {{
                brushGroup.call(brush.clear);
                updateBrush({{ selection: null }});
            }});
        }}
        
        // INITIALIZE AND HANDLE RESIZE
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
"""

    # Return HTML object using common helper
    return common.html_to_obj(html_content)