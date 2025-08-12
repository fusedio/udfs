@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Simple Word Cloud with D3
    # WHEN TO USE: Show frequency/importance of text data or categorical variables
    # DATA REQUIREMENTS: 1 text column (words), 1 numeric column (frequency/size)
    # FEATURES: Size-coded words, color themes, responsive design
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 
    # 1. DATA ADAPTATION:
    #    - Update load_data() function with your data source
    #    - Required columns: [text_column, frequency_column]
    #    - Update data_json with your column names
    #    - Update all "UPDATE_DATA" marked field accessors in JavaScript
    #    - Update CONFIG object with your field names
    #
    # 2. STYLING & BRANDING:
    #    - Colors: Update fillColor in CONFIG or use colorScale
    #    - Fonts: Change fontFamily in CONFIG
    #    - Theme: Modify --bg-color in CSS variables
    #    - Word sizing: Adjust fontSizeMultiplier in CONFIG
    #
    # 3. INTERACTION BEHAVIOR:
    #    - Rotation: Modify rotation function in layout
    #    - Spacing: Change padding in CONFIG
    #    - Layout: Switch between "archimedean" and "rectangular" spiral
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
        Load dataset for word cloud analysis
        
        AI REQUIREMENTS:
        - Must return pandas DataFrame
        - Must have 1 text column (words/categories) 
        - Must have 1 numeric column (frequency/size/importance)
        
        EXAMPLE REPLACEMENTS:
        - return pd.read_csv("your_file.csv")
        - return pd.read_parquet("s3://bucket/data.parquet") 
        - return your_database_query()
        """
        # Create simple word frequency data
        return pd.DataFrame({
            'word': ['Running', 'Surfing', 'Climbing', 'Kiting', 'Sailing', 'Snowboarding', 
                    'Swimming', 'Cycling', 'Hiking', 'Skiing', 'Diving', 'Rowing'],
            'size': [10, 20, 50, 30, 20, 60, 25, 15, 35, 40, 18, 12]
        })

    df = load_data()

    # -------------------------------------------------------------------------
    # DATA PREPARATION SECTION  
    # AI INSTRUCTION: Update column names to match your dataset
    # -------------------------------------------------------------------------
    # Select only the columns needed for the word cloud
    chart_data = df[["word", "size"]]  # UPDATE_DATA: Your [text_col, frequency_col]
    data_json = chart_data.to_json(orient="records")

    # -------------------------------------------------------------------------
    # HTML TEMPLATE WITH STANDARDIZED STRUCTURE
    # AI INSTRUCTION: Search for "UPDATE_DATA" comments to modify field accessors
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Simple Word Cloud</title>
    <script src="https://d3js.org/d3.v4.js"></script>
    <script src="https://cdn.jsdelivr.net/gh/holtzy/D3-graph-gallery@master/LIB/d3.layout.cloud.js"></script>
    <style>
        /* =================================================================== */
        /* STYLING SECTION - AI: Modify these CSS variables for branding */
        /* =================================================================== */
        :root {{
            --bg-color: #f8f9fa;
            --text-color: #69b3a2;
            --font-family: Impact, Arial, sans-serif;
        }}
        
        body {{
            margin: 0;
            background: var(--bg-color);
            font-family: var(--font-family);
            height: 100vh;
            overflow: hidden;
        }}
        
        #wordcloud {{
            width: 100%;
            height: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        
        .word {{
            font-family: var(--font-family);
            fill: var(--text-color);
            text-anchor: middle;
            cursor: pointer;
        }}
        
        .word:hover {{
            opacity: 0.7;
        }}
    </style>
</head>
<body>
    <div id="wordcloud"></div>

    <script>
        // =================================================================
        // DATA AND CONFIGURATION SECTION
        // AI INSTRUCTION: Update field accessors marked with UPDATE_DATA
        // =================================================================
        
        const data = {data_json};
        
        // Chart configuration - AI: Modify these for your data
        const CONFIG = {{
            textField: "word",                    // UPDATE_DATA: Your text column name
            sizeField: "size",                    // UPDATE_DATA: Your size column name
            fontSizeMultiplier: 1,                // Multiply font sizes by this factor
            padding: 5,                           // Space between words
            fontFamily: "Impact",                 // Font family
            fillColor: "#69b3a2",                // Single color for all words
            useColorScale: false,                 // Set to true for multi-color
            spiralType: "archimedean",            // "archimedean" or "rectangular"
            rotationDegrees: 90                   // 0, 45, 90 for rotation options
        }};
        
        // =================================================================
        // WORD CLOUD SETUP
        // =================================================================
        
        // Set dimensions
        const margin = {{top: 10, right: 10, bottom: 10, left: 10}};
        const width = window.innerWidth - margin.left - margin.right;
        const height = window.innerHeight - margin.top - margin.bottom;
        
        // Create SVG
        const svg = d3.select("#wordcloud")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
        
        // Optional color scale
        let colorScale;
        if (CONFIG.useColorScale) {{
            colorScale = d3.scaleOrdinal(d3.schemeCategory10);
        }}
        
        // =================================================================
        // D3-CLOUD LAYOUT
        // =================================================================
        
        const layout = d3.layout.cloud()
            .size([width, height])
            .words(data.map(function(d) {{ 
                return {{
                    text: d[CONFIG.textField],       // UPDATE_DATA: Text field accessor
                    size: d[CONFIG.sizeField] * CONFIG.fontSizeMultiplier  // UPDATE_DATA: Size field accessor
                }}; 
            }}))
            .padding(CONFIG.padding)
            .rotate(function() {{ 
                // Random rotation: 0, 45, or 90 degrees
                return ~~(Math.random() * 3) * CONFIG.rotationDegrees / 2; 
            }})
            .fontSize(function(d) {{ return d.size; }})
            .spiral(CONFIG.spiralType)
            .on("end", draw);
        
        layout.start();
        
        // =================================================================
        // DRAW FUNCTION
        // =================================================================
        
        function draw(words) {{
            svg
                .append("g")
                .attr("transform", "translate(" + layout.size()[0] / 2 + "," + layout.size()[1] / 2 + ")")
                .selectAll("text")
                .data(words)
                .enter().append("text")
                .attr("class", "word")
                .style("font-size", function(d) {{ return d.size + "px"; }})
                .style("font-family", CONFIG.fontFamily)
                .style("fill", function(d, i) {{
                    // Use color scale or single color
                    return CONFIG.useColorScale ? colorScale(i) : CONFIG.fillColor;
                }})
                .attr("text-anchor", "middle")
                .attr("transform", function(d) {{
                    return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
                }})
                .text(function(d) {{ return d.text; }})
                .on("click", function(d) {{
                    // UPDATE_DATA: Add click interactions here
                    console.log("Clicked:", d.text, "Size:", d.size);
                }});
        }}
        
        // =================================================================
        // RESPONSIVE RESIZE
        // =================================================================
        
        window.addEventListener("resize", function() {{
            // Simple reload on resize
            location.reload();
        }});
    </script>
</body>
</html>
"""

    return common.html_to_obj(html_content)