@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Categorical Data Pie Chart (Observable D3 Style)
    # WHEN TO USE: Show composition/proportions of any categorical dataset
    # DATA REQUIREMENTS: Dataset with categorical column for grouping and counting
    # FEATURES: Clean Observable-style pie chart with built-in tooltips and legend
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 
    # 1. DATA ADAPTATION:
    #    - Update load_data() function with your data source (CSV, Parquet, API, etc.)
    #    - Change category_column to your categorical field name
    #    - Update data_url or replace with your data loading logic
    #    - Modify chart title and subtitle in HTML section
    #    - Update all "UPDATE_DATA" marked sections
    #
    # 2. DATA SOURCE EXAMPLES:
    #    - CSV files: pd.read_csv("your_file.csv")
    #    - Parquet: pd.read_parquet("s3://bucket/data.parquet")
    #    - Database: your_database_query()
    #    - API endpoints: pd.read_json("api_url")
    #    - Local files: pd.read_csv("/path/to/file.csv")
    #
    # 3. CATEGORY COLUMN EXAMPLES:
    #    - 'species' (for biological data)
    #    - 'category' (for general classifications)
    #    - 'region' (for geographical data)
    #    - 'department' (for organizational data)
    #    - 'product_type' (for sales data)
    #
    # 4. STYLING CUSTOMIZATION:
    #    - Colors: Modify d3 color scheme in PieChart function
    #    - Dimensions: Update CONFIG width/height values
    #    - Legend: Toggle showLegend true/false
    #    - Labels: Toggle showLabels true/false for on-slice labels
    # =============================================================================

    import pandas as pd
    import json
    
    # Load common utilities - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    # -------------------------------------------------------------------------
    # DATA LOADING SECTION
    # AI INSTRUCTION: Replace this entire section with your data loading logic
    # -------------------------------------------------------------------------
    @fused.cache
    def load_data():
        """
        Load dataset for categorical pie chart visualization
        
        AI REQUIREMENTS:
        - Must return pandas DataFrame
        - Must have at least one categorical column for grouping
        - Can be any data source: CSV, Parquet, Database, API, etc.
        
        EXAMPLE REPLACEMENTS:
        - return pd.read_csv("your_file.csv")
        - return pd.read_parquet("s3://bucket/data.parquet")
        - return your_database_query()
        - return pd.read_json("api_endpoint")
        """
        # UPDATE_DATA: Replace with your data source
        data_url = 'https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv'
        df = pd.read_csv(data_url)
        
        print(f"Loaded dataset with {len(df)} rows and columns: {list(df.columns)}")
        return df
    
    # Load your data
    df = load_data()
    
    # -------------------------------------------------------------------------
    # DATA PREPARATION SECTION  
    # AI INSTRUCTION: Update category_column to match your dataset
    # -------------------------------------------------------------------------
    
    # Configuration - UPDATE_DATA: Change to your categorical column name
    category_column = 'species'  # UPDATE_DATA: Your categorical column (e.g., 'category', 'region', 'type')
    
    # Verify column exists
    if category_column not in df.columns:
        available_cols = list(df.columns)
        raise ValueError(f"Column '{category_column}' not found. Available columns: {available_cols}")
    
    # Count occurrences by category
    category_counts = df[category_column].value_counts()
    
    print(f"Category counts for '{category_column}':")
    for cat, count in category_counts.items():
        print(f"  {cat}: {count}")
    
    # Prepare data for pie chart in [name, value] format
    chart_data = []
    for category, count in category_counts.items():
        if pd.notna(category) and count > 0:  # Filter out NaN and zero values
            chart_data.append([str(category), int(count)])
    
    # Sort by count for better visualization (largest first)
    chart_data.sort(key=lambda x: x[1], reverse=True)
    
    # Use proper JSON formatting for JavaScript
    data_json = json.dumps(chart_data)
    
    print(f"Final chart data: {chart_data}")
    
    # -------------------------------------------------------------------------
    # HTML TEMPLATE - EXACT OBSERVABLE D3 STYLE
    # AI INSTRUCTION: Update titles and labels in the HTML section
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Categorical Data Pie Chart</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: white;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
            text-align: center;
        }}
        h1 {{
            font-size: 24px;
            margin-bottom: 10px;
            color: #333;
        }}
        .subtitle {{
            font-size: 14px;
            color: #666;
            margin-bottom: 30px;
        }}
        svg {{
            font: 10px sans-serif;
        }}
        .legend {{
            margin-top: 30px;
            display: inline-block;
            text-align: left;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 4px 0;
            font-size: 13px;
        }}
        .legend-color {{
            width: 18px;
            height: 18px;
            margin-right: 8px;
            border: 1px solid #ccc;
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- UPDATE_DATA: Customize these titles for your dataset -->
        <h1>Palmer Penguins Species Distribution</h1>
        <p class="subtitle">Antarctic Peninsula Research Data</p>
        <div id="chart"></div>
        <div id="legend" class="legend"></div>
    </div>

    <script>
        // =================================================================
        // DATA AND CONFIGURATION
        // AI INSTRUCTION: Update these values for your specific use case
        // =================================================================
        
        const data = {data_json};
        
        // Chart configuration - UPDATE_DATA: Modify for your needs
        const CONFIG = {{
            width: 640,                          // UPDATE_DATA: Chart width in pixels
            height: 400,                         // UPDATE_DATA: Chart height in pixels
            innerRadius: 0,                      // UPDATE_DATA: Inner radius (0 = pie, >0 = donut)
            stroke: "white",                     // UPDATE_DATA: Stroke color between slices
            strokeWidth: 1,                      // UPDATE_DATA: Stroke width
            strokeLinejoin: "round",             // Stroke line join style
            padAngle: 0,                         // UPDATE_DATA: Angular separation between wedges
            format: ",",                         // Number format for values
            showLabels: false,                   // UPDATE_DATA: Show/hide labels on slices
            showLegend: true,                    // UPDATE_DATA: Show/hide legend
            colorScheme: "Spectral"              // UPDATE_DATA: D3 color scheme (Spectral, Category10, Set3, etc.)
        }};
        
        // =================================================================
        // PIE CHART FUNCTION (Based on Observable D3)
        // =================================================================
        
        function PieChart(data, {{
            name = ([x]) => x,                  // given d in data, returns the (ordinal) label
            value = ([, y]) => y,               // given d in data, returns the (quantitative) value
            title,                              // given d in data, returns the title text
            width = CONFIG.width,               // outer width, in pixels
            height = CONFIG.height,             // outer height, in pixels
            innerRadius = CONFIG.innerRadius,   // inner radius of pie, in pixels (non-zero for donut)
            outerRadius = Math.min(width, height) / 2, // outer radius of pie, in pixels
            labelRadius = (innerRadius * 0.2 + outerRadius * 0.8), // center radius of labels
            format = CONFIG.format,             // a format specifier for values (in the label)
            names,                              // array of names (the domain of the color scale)
            colors,                             // array of colors for names
            stroke = CONFIG.stroke,             // stroke separating widths
            strokeWidth = CONFIG.strokeWidth,   // width of stroke separating wedges
            strokeLinejoin = CONFIG.strokeLinejoin, // line join of stroke separating wedges
            padAngle = CONFIG.padAngle         // angular separation between wedges, in radians
        }} = {{}}) {{
            
            // Compute values.
            const N = d3.map(data, name);
            const V = d3.map(data, value);
            const I = d3.range(N.length).filter(i => !isNaN(V[i]));
            
            // Unique the names.
            if (names === undefined) names = N;
            names = new d3.InternSet(names);
            
            // Choose a default color scheme based on cardinality.
            // UPDATE_DATA: Customize color scheme here
            if (colors === undefined) {{
                if (CONFIG.colorScheme === "Spectral") {{
                    colors = d3.schemeSpectral[names.size];
                    if (colors === undefined) colors = d3.quantize(t => d3.interpolateSpectral(t * 0.8 + 0.1), names.size);
                }} else if (CONFIG.colorScheme === "Category10") {{
                    colors = d3.schemeCategory10;
                }} else if (CONFIG.colorScheme === "Set3") {{
                    colors = d3.schemeSet3;
                }} else {{
                    // Fallback to Spectral
                    colors = d3.schemeSpectral[names.size] || d3.quantize(t => d3.interpolateSpectral(t * 0.8 + 0.1), names.size);
                }}
            }}
            
            // Construct scales.
            const color = d3.scaleOrdinal(names, colors);
            
            // Compute titles.
            if (title === undefined) {{
                const formatValue = d3.format(format);
                // UPDATE_DATA: Customize tooltip text format
                title = i => `${{N[i]}}\\n${{formatValue(V[i])}} items`;
            }} else {{
                const O = d3.map(data, d => d);
                const T = title;
                title = i => T(O[i], i, data);
            }}
            
            // Construct arcs.
            const arcs = d3.pie().padAngle(padAngle).sort(null).value(i => V[i])(I);
            const arc = d3.arc().innerRadius(innerRadius).outerRadius(outerRadius);
            const arcLabel = d3.arc().innerRadius(labelRadius).outerRadius(labelRadius);
            
            const svg = d3.create("svg")
                .attr("width", width)
                .attr("height", height)
                .attr("viewBox", [-width / 2, -height / 2, width, height])
                .attr("style", "max-width: 100%; height: auto; height: intrinsic;");
            
            // Create pie slices
            svg.append("g")
                .attr("stroke", stroke)
                .attr("stroke-width", strokeWidth)
                .attr("stroke-linejoin", strokeLinejoin)
                .selectAll("path")
                .data(arcs)
                .join("path")
                .attr("fill", d => color(N[d.data]))
                .attr("d", arc)
                .append("title")
                .text(d => title(d.data));
            
            // Add labels if enabled
            if (CONFIG.showLabels) {{
                svg.append("g")
                    .attr("font-family", "sans-serif")
                    .attr("font-size", 10)
                    .attr("text-anchor", "middle")
                    .selectAll("text")
                    .data(arcs)
                    .join("text")
                    .attr("transform", d => `translate(${{arcLabel.centroid(d)}})`)
                    .selectAll("tspan")
                    .data(d => {{
                        const lines = `${{title(d.data)}}`.split(/\\n/);
                        return (d.endAngle - d.startAngle) > 0.25 ? lines : lines.slice(0, 1);
                    }})
                    .join("tspan")
                    .attr("x", 0)
                    .attr("y", (_, i) => `${{i * 1.1}}em`)
                    .attr("font-weight", (_, i) => i ? null : "bold")
                    .text(d => d);
            }}
            
            return Object.assign(svg.node(), {{scales: {{color}}}});
        }}
        
        // =================================================================
        // LEGEND FUNCTION
        // =================================================================
        
        function createLegend(data, colorScale) {{
            const legend = document.getElementById("legend");
            legend.innerHTML = ""; // Clear existing content
            
            data.forEach(([name, value]) => {{
                const item = document.createElement("div");
                item.className = "legend-item";
                
                const colorBox = document.createElement("div");
                colorBox.className = "legend-color";
                colorBox.style.backgroundColor = colorScale(name);
                
                const label = document.createElement("span");
                const percentage = ((value / data.reduce((sum, [, v]) => sum + v, 0)) * 100).toFixed(1);
                // UPDATE_DATA: Customize legend text format
                label.textContent = `${{name}} (${{percentage}}% • ${{value.toLocaleString()}} items)`;
                
                item.appendChild(colorBox);
                item.appendChild(label);
                legend.appendChild(item);
            }});
        }}
        
        // =================================================================
        // INITIALIZE CHART
        // =================================================================
        
        // Create and display the pie chart
        const chart = PieChart(data);
        document.getElementById("chart").appendChild(chart);
        
        // Create legend if enabled
        if (CONFIG.showLegend) {{
            createLegend(data, chart.scales.color);
        }}
        
        console.log("Categorical pie chart created with data:", data);
    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)