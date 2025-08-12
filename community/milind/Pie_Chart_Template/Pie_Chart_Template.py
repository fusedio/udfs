@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Simple ACS Census Pie Chart (Observable D3 Style)
    # WHEN TO USE: Show composition/proportions of US Census ACS demographic data
    # DATA REQUIREMENTS: ACS table with categories and population counts
    # FEATURES: Clean Observable-style pie chart with built-in tooltips and labels
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 
    # 1. DATA ADAPTATION:
    #    - Update table_id in load_acs_data() for different census tables
    #    - Change year parameter for different census years
    #    - Update category_mapping dictionary with your table's column structure
    #    - Update all "UPDATE_DATA" marked field accessors in JavaScript
    #
    # 2. ACS TABLE EXAMPLES:
    #    - 'B02001': Race and Ethnicity
    #    - 'B19001': Household Income
    #    - 'B08303': Travel Time to Work
    #    - 'B15003': Educational Attainment
    #    - 'B25024': Housing Units in Structure
    #
    # 3. STYLING CUSTOMIZATION:
    #    - Colors: Update colors array in CONFIG (default: d3.schemeSpectral)
    #    - Dimensions: Modify width/height in CONFIG
    #    - Stroke: Change stroke color and width
    #    - Labels: Adjust font size and positioning
    # =============================================================================

    import pandas as pd
    
    # Load common utilities - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    # -------------------------------------------------------------------------
    # DATA LOADING SECTION
    # AI INSTRUCTION: Replace table_id and category_mapping for your ACS table
    # -------------------------------------------------------------------------
    @fused.cache
    def load_acs_data():
        """
        Load US Census ACS data directly from census.gov
        
        AI REQUIREMENTS:
        - Uses Census ACS 5-year data from official source
        - Loads a specific table by ID
        - Returns cleaned data suitable for pie chart (categories with counts)
        
        EXAMPLE TABLE IDS:
        - 'B02001': Race and Ethnicity
        - 'B19001': Household Income by brackets
        - 'B08303': Travel Time to Work
        - 'B15003': Educational Attainment
        - 'B25024': Housing Units in Structure
        """
        # Configuration - UPDATE_DATA: Change these for your analysis
        table_id = 'B02001'  # UPDATE_DATA: ACS table ID
        year = 2022          # UPDATE_DATA: Census year
        
        # Load ACS table from official census.gov source
        url = f'https://www2.census.gov/programs-surveys/acs/summary_file/{year}/table-based-SF/data/5YRData/acsdt5y{year}-{table_id.lower()}.dat'
        df = pd.read_csv(url, delimiter='|')
        
        print(f"Loaded ACS table {table_id} for {year}")
        print("Available columns:", [col for col in df.columns if col.startswith(f'{table_id}_E')][:10])
        
        return df
    
    # Load ACS data
    df = load_acs_data()
    
    # -------------------------------------------------------------------------
    # DATA PREPARATION SECTION  
    # AI INSTRUCTION: Update category_mapping to match your ACS table structure
    # -------------------------------------------------------------------------
    
    # Category mapping for ACS table B02001 (Race/Ethnicity)
    # UPDATE_DATA: Replace this mapping with your table's column structure
    category_mapping = {
        'B02001_E002': 'White alone',
        'B02001_E003': 'Black or African American alone', 
        'B02001_E004': 'American Indian and Alaska Native alone',
        'B02001_E005': 'Asian alone',
        'B02001_E006': 'Native Hawaiian and Other Pacific Islander alone',
        'B02001_E007': 'Some other race alone',
        'B02001_E008': 'Two or more races'
    }
    
    # Extract data for pie chart
    chart_data = []
    for col, category in category_mapping.items():
        if col in df.columns:
            value = df[col].iloc[0]  # Get first row (usually US total)
            if value > 0:  # Only include non-zero values
                chart_data.append([category, int(value)])  # [name, value] format for D3
    
    # Sort by value for better visualization
    chart_data.sort(key=lambda x: x[1], reverse=True)
    
    # Convert to JSON for D3
    data_json = str(chart_data).replace("'", '"')  # Simple JSON conversion
    
    print("Chart data prepared:")
    for name, value in chart_data:
        print(f"  {name}: {value:,}")
    
    # -------------------------------------------------------------------------
    # HTML TEMPLATE - EXACT OBSERVABLE D3 STYLE
    # AI INSTRUCTION: Minimal changes needed - just update data and titles
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>US Census ACS Pie Chart</title>
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
        <h1>US Population by Race/Ethnicity</h1>
        <p class="subtitle">American Community Survey 2022</p>
        <div id="chart"></div>
        <div id="legend" class="legend"></div>
    </div>

    <script>
        // =================================================================
        // DATA AND CONFIGURATION
        // AI INSTRUCTION: Update these values for your ACS table
        // =================================================================
        
        const data = {data_json};
        
        // Chart configuration - UPDATE_DATA: Modify for your needs
        const CONFIG = {{
            width: 640,                          // UPDATE_DATA: Chart width
            height: 400,                         // UPDATE_DATA: Chart height
            innerRadius: 0,                      // UPDATE_DATA: Inner radius (0 = pie, >0 = donut)
            stroke: "white",                     // UPDATE_DATA: Stroke color between slices
            strokeWidth: 1,                      // UPDATE_DATA: Stroke width
            strokeLinejoin: "round",             // Stroke line join style
            padAngle: 0,                         // UPDATE_DATA: Angular separation between wedges
            format: ",",                         // Number format for labels
            showLabels: false,                   // UPDATE_DATA: Show/hide labels on slices (false for cleaner look with legend)
            showLegend: true                     // UPDATE_DATA: Show/hide legend
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
            // UPDATE_DATA: Change color scheme here
            if (colors === undefined) colors = d3.schemeSpectral[names.size];
            if (colors === undefined) colors = d3.quantize(t => d3.interpolateSpectral(t * 0.8 + 0.1), names.size);
            
            // Construct scales.
            const color = d3.scaleOrdinal(names, colors);
            
            // Compute titles.
            if (title === undefined) {{
                const formatValue = d3.format(format);
                // UPDATE_DATA: Customize tooltip text format
                title = i => `${{N[i]}}\\n${{formatValue(V[i])}}`;
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
        // INITIALIZE CHART
        // =================================================================
        
        // Create and display the pie chart
        const chart = PieChart(data);
        document.getElementById("chart").appendChild(chart);
        
        // Create legend if enabled
        if (CONFIG.showLegend) {{
            createLegend(data, chart.scales.color);
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
                label.textContent = `${{name}} (${{percentage}}%)`;
                
                item.appendChild(colorBox);
                item.appendChild(label);
                legend.appendChild(item);
            }});
        }}
        
        console.log("ACS Census Pie Chart created with data:", data);
    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)