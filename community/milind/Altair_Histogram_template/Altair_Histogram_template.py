@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Histogram with Altair/Vega-Lite
    # WHEN TO USE: Show distribution of ONE numeric variable with optional grouping
    # DATA REQUIREMENTS: At least 1 numeric column, optional 1 categorical column
    # FEATURES: Interactive tooltips, zoom/pan, responsive design, professional styling
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 
    # 1. DATA ADAPTATION:
    #    - Update load_data() function with your data source
    #    - Required columns: [numeric_column] and optional [category_column]
    #    - Update CONFIG section with your column names and parameters
    #    - Update all "UPDATE_DATA" marked field references in chart encoding
    #    - Modify chart title and axis labels
    #
    # 2. STYLING & BRANDING:
    #    - Colors: Update color scheme in .configure_* methods
    #    - Theme: Use alt.themes.register() for custom themes
    #    - Fonts: Modify configure_title(), configure_axis() font properties
    #    - Layout: Adjust chart properties (width, height, padding)
    #    - Background: Update configure_view() background color
    #
    # 3. INTERACTION BEHAVIOR:
    #    - Tooltips: Modify tooltip encoding or add custom tooltip
    #    - Selection: Add alt.selection_interval() for brushing
    #    - Binning: Adjust bin parameters (maxbins, step, extent)
    #    - Interactivity: Add .add_selection() for zoom/pan/brush
    # =============================================================================

    import pandas as pd
    import altair as alt
    
    # Load common utilities - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    # Force Altair to use default data transformer and remove row limit
    alt.data_transformers.enable("default", max_rows=None)
    
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
        # Palmer Penguins dataset
        url = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        return pd.read_csv(url)

    df = load_data()
    
    # -------------------------------------------------------------------------
    # CHART CONFIGURATION SECTION  
    # AI INSTRUCTION: Update these settings to match your dataset
    # -------------------------------------------------------------------------
    CONFIG = {
        'numeric_field': 'bill_length_mm',           # UPDATE_DATA: Your numeric column name
        'category_field': 'species',                 # UPDATE_DATA: Your category column (optional)
        'max_bins': 30,                             # Number of histogram bins
        'x_axis_label': 'Bill Length (mm)',         # UPDATE_DATA: Your X axis label  
        'y_axis_label': 'Count',                    # Y axis label
        'chart_title': 'Distribution of Penguin Bill Lengths', # UPDATE_DATA: Your chart title
        'chart_width': 'container',                 # Chart width ('container' for responsive)
        'chart_height': 'container',                # Chart height ('container' for responsive)
        'color_scheme': 'category10',               # Altair color scheme
        'background_color': '#f8f9fa'               # Chart background color
    }
    
    # -------------------------------------------------------------------------
    # ALTAIR CHART CREATION SECTION
    # AI INSTRUCTION: Search for "UPDATE_DATA" comments to modify field references
    # -------------------------------------------------------------------------
    
    # Create the base chart
    base_chart = alt.Chart(df)
    
    # Create histogram with configuration
    chart = (
        base_chart
        .mark_bar(
            opacity=0.8,
            stroke='white',
            strokeWidth=1
        )
        .encode(
            x=alt.X(
                f"{CONFIG['numeric_field']}:Q",      # UPDATE_DATA: Your numeric field
                bin=alt.Bin(maxbins=CONFIG['max_bins']),
                title=CONFIG['x_axis_label']
            ),
            y=alt.Y(
                "count()", 
                title=CONFIG['y_axis_label']
            ),
            color=alt.Color(
                f"{CONFIG['category_field']}:N",     # UPDATE_DATA: Your category field (optional)
                scale=alt.Scale(scheme=CONFIG['color_scheme']),
                legend=alt.Legend(
                    title="Category",                # UPDATE_DATA: Legend title
                    orient="right"
                )
            ),
            tooltip=[
                alt.Tooltip(f"{CONFIG['numeric_field']}:Q", title=CONFIG['x_axis_label']),  # UPDATE_DATA
                alt.Tooltip(f"{CONFIG['category_field']}:N", title="Category"),             # UPDATE_DATA
                alt.Tooltip("count()", title="Count")
            ]
        )
        .properties(
            width=CONFIG['chart_width'],
            height=CONFIG['chart_height'],
            title=alt.TitleParams(
                text=CONFIG['chart_title'],
                fontSize=16,
                fontWeight='bold',
                anchor='start',
                color='#333333'
            )
        )
        .configure_view(
            strokeWidth=0,
            fill=CONFIG['background_color']
        )
        .configure_axis(
            grid=True,
            gridColor='#e0e0e0',
            gridOpacity=0.5,
            labelFontSize=11,
            titleFontSize=13,
            titleFontWeight='normal',
            titleColor='#555555'
        )
        .configure_legend(
            titleFontSize=12,
            labelFontSize=11,
            symbolSize=100
        )
        .interactive()  # Enable zoom and pan interactions
    )
    
    # Convert chart to HTML
    chart_html = chart.to_html()
    
    # -------------------------------------------------------------------------
    # RESPONSIVE HTML WRAPPER SECTION
    # AI INSTRUCTION: Modify styling as needed for your design requirements
    # -------------------------------------------------------------------------
    responsive_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{CONFIG['chart_title']}</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            /* =================================================================== */
            /* RESPONSIVE STYLING - AI: Modify these for custom layouts */
            /* =================================================================== */
            html, body {{
                margin: 0;
                padding: 0;
                height: 100%;
                width: 100%;
                overflow: hidden;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background-color: #f8f9fa;
            }}
            
            .chart-container {{
                width: 100vw;
                height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
                box-sizing: border-box;
            }}
            
            .vega-embed {{
                width: 100%;
                height: 100%;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                background: white;
            }}
            
            .vega-embed > .vega-visualization {{
                width: 100% !important;
                height: 100% !important;
            }}
            
            /* Custom scrollbar for better UX */
            .vega-embed::-webkit-scrollbar {{
                width: 8px;
                height: 8px;
            }}
            
            .vega-embed::-webkit-scrollbar-track {{
                background: #f1f1f1;
                border-radius: 4px;
            }}
            
            .vega-embed::-webkit-scrollbar-thumb {{
                background: #c1c1c1;
                border-radius: 4px;
            }}
            
            .vega-embed::-webkit-scrollbar-thumb:hover {{
                background: #a8a8a8;
            }}
            
            /* Responsive breakpoints */
            @media (max-width: 768px) {{
                .chart-container {{
                    padding: 10px;
                }}
            }}
            
            @media (max-width: 480px) {{
                .chart-container {{
                    padding: 5px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="chart-container">
            {chart_html}
        </div>
    </body>
    </html>
    """
    
    # Return the chart as an HTML object
    return common.html_to_obj(responsive_html)