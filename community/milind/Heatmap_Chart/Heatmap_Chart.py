@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Simple Correlation Heatmap (D3 Gallery Style)
    # WHEN TO USE: Show correlations between numeric variables in datasets
    # DATA REQUIREMENTS: Dataset with multiple numeric columns
    # FEATURES: Simple heatmap with hover tooltips, clean D3 gallery aesthetic
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 1. DATA ADAPTATION: Update load_data() function with your dataset
    # 2. COLUMN SELECTION: Modify numeric_cols selection for your variables
    # 3. STYLING: Change color scheme in myColor scale
    # 4. TITLES: Update chart title and subtitle in HTML
    # =============================================================================

    import pandas as pd
    import numpy as np
    
    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_data():
        """Load dataset for correlation analysis"""
        # UPDATE_DATA: Replace with your data source
        from sklearn.datasets import load_wine
        wine = load_wine()
        df = pd.DataFrame(wine.data, columns=wine.feature_names)
        return df
    
    # Load and prepare data
    df = load_data()
    numeric_cols = df.select_dtypes(include=[np.number]).columns[:10]  # Take first 10 columns
    correlation_matrix = df[numeric_cols].corr()
    
    # Convert to D3 format
    heatmap_data = []
    for i, row_var in enumerate(correlation_matrix.columns):
        for j, col_var in enumerate(correlation_matrix.columns):
            heatmap_data.append({
                'group': row_var,
                'variable': col_var, 
                'value': correlation_matrix.iloc[i, j]
            })
    
    data_json = pd.DataFrame(heatmap_data).to_json(orient="records")
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Correlation Heatmap</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
        }}
        .tooltip {{
            position: absolute;
            text-align: center;
            padding: 8px;
            font-size: 12px;
            background: white;
            border: solid 2px black;
            border-radius: 5px;
            pointer-events: none;
            opacity: 0;
        }}
    </style>
</head>
<body>
    <div id="my_dataviz"></div>

    <script>
        // Set dimensions and margins
        var margin = {{top: 80, right: 25, bottom: 100, left: 100}},
            width = 450 - margin.left - margin.right,
            height = 450 - margin.top - margin.bottom;

        // Append SVG
        var svg = d3.select("#my_dataviz")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // Load data
        var data = {data_json};
        
        // Get unique groups and variables
        var myGroups = [...new Set(data.map(d => d.group))];
        var myVars = [...new Set(data.map(d => d.variable))];

        // Build X scales and axis
        var x = d3.scaleBand()
            .range([0, width])
            .domain(myGroups)
            .padding(0.05);
            
        svg.append("g")
            .style("font-size", 12)
            .attr("transform", "translate(0," + height + ")")
            .call(d3.axisBottom(x).tickSize(0))
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(-45)");
            
        svg.select(".domain").remove();

        // Build Y scales and axis  
        var y = d3.scaleBand()
            .range([height, 0])
            .domain(myVars)
            .padding(0.05);
            
        svg.append("g")
            .style("font-size", 12)
            .call(d3.axisLeft(y).tickSize(0))
            .select(".domain").remove();

        // Build color scale
        var myColor = d3.scaleSequential()
            .interpolator(d3.interpolateRdYlBu)
            .domain([1, -1]);

        // Create tooltip
        var tooltip = d3.select("#my_dataviz")
            .append("div")
            .attr("class", "tooltip");

        // Tooltip functions
        var mouseover = function(event, d) {{
            tooltip.style("opacity", 1);
            d3.select(this)
                .style("stroke", "black")
                .style("opacity", 1);
        }}

        var mousemove = function(event, d) {{
            tooltip
                .html("Variables: " + d.group + " × " + d.variable + "<br/>Correlation: " + d.value.toFixed(3))
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY) + "px");
        }}

        var mouseleave = function(event, d) {{
            tooltip.style("opacity", 0);
            d3.select(this)
                .style("stroke", "none")
                .style("opacity", 0.8);
        }}

        // Add rectangles
        svg.selectAll()
            .data(data, function(d) {{ return d.group + ':' + d.variable; }})
            .enter()
            .append("rect")
            .attr("x", function(d) {{ return x(d.group); }})
            .attr("y", function(d) {{ return y(d.variable); }})
            .attr("rx", 4)
            .attr("ry", 4)
            .attr("width", x.bandwidth())
            .attr("height", y.bandwidth())
            .style("fill", function(d) {{ return myColor(d.value); }})
            .style("stroke-width", 4)
            .style("stroke", "none")
            .style("opacity", 0.8)
            .on("mouseover", mouseover)
            .on("mousemove", mousemove)
            .on("mouseleave", mouseleave);

        // Add title
        svg.append("text")
            .attr("x", 0)
            .attr("y", -50)
            .attr("text-anchor", "left")
            .style("font-size", "22px")
            .text("Wine Dataset Correlation Heatmap");

        // Add subtitle  
        svg.append("text")
            .attr("x", 0)
            .attr("y", -20)
            .attr("text-anchor", "left")
            .style("font-size", "14px")
            .style("fill", "grey")
            .text("Correlations between chemical properties of wine samples");
    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)