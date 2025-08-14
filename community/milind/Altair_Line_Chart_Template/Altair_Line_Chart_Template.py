@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Altair Interactive Line Chart
    # WHEN TO USE: Time series analysis, trend visualization, continuous data over time/sequence
    # DATA REQUIREMENTS: 1 continuous X variable (often temporal), 1 continuous Y variable, optional categorical for multiple lines
    # ALTAIR LINE SPECIFICS: Automatic temporal parsing, built-in interpolation, easy multi-series grouping, hover interactions
    # =============================================================================
    
    import pandas as pd
    import altair as alt
    from jinja2 import Template

    # REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    # ALTAIR REQUIREMENT: Enable processing of larger datasets
    alt.data_transformers.enable("default", max_rows=None)

    @fused.cache
    def load_data():
        """
        ALTAIR LINE CHART DATA REQUIREMENTS:
        - DataFrame with temporal or sequential X column
        - Continuous Y values for line plotting
        - Optional categorical column for multiple line series
        - Altair automatically handles date parsing and missing values
        """
        # Using stock price data for demonstration
        url = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/stocks.csv"
        return pd.read_csv(url)

    df = load_data()

    # ALTAIR LINE CHART CONFIGURATION
    config = {
        # ALTAIR FIELD SPECIFICATIONS: Use exact column names from DataFrame
        "x_field": "date",           # Temporal or continuous X-axis
        "y_field": "price",          # Continuous Y-axis values
        "category_field": "symbol",  # Optional: Group lines by category (None to disable)
        
        # ALTAIR TEMPORAL HANDLING
        "x_type": "T",               # T=temporal, Q=quantitative, O=ordinal
        "date_format": "%Y",         # Format for temporal axis labels
        
        # ALTAIR VISUAL ENCODING
        "color_scheme": "category10", # Altair color scheme for multiple lines
        "stroke_width": 2.5,         # Line thickness
        "point_size": 50,            # Point markers (0 to disable)
        "opacity": 0.8,              # Line transparency
        
        # ALTAIR INTERACTIVITY
        "interactive": True,         # Enable zoom/pan
        "tooltip_enabled": True,     # Show hover tooltips
        
        # LABELS AND STYLING
        "title": "Stock Price Trends Over Time",
        "x_label": "Year",
        "y_label": "Stock Price ($)",
        "legend_title": "Company",
    }

    # ALTAIR CHART CONSTRUCTION: Declarative approach
    base = alt.Chart(df)

    # ALTAIR ENCODING: Define visual mappings
    x_enc = alt.X(
        f"{config['x_field']}:{config['x_type']}", 
        title=config["x_label"],
        axis=alt.Axis(format=config["date_format"]) if config["x_type"] == "T" else alt.Axis()
    )
    y_enc = alt.Y(f"{config['y_field']}:Q", title=config["y_label"])

    # CONDITIONAL MULTI-SERIES: Color and grouping based on category
    if config["category_field"] and config["category_field"] in df.columns:
        color_enc = alt.Color(
            f"{config['category_field']}:N",
            scale=alt.Scale(scheme=config["color_scheme"]),
            legend=alt.Legend(title=config["legend_title"])
        )
        
        # ALTAIR TOOLTIP: Multi-series interactive tooltips
        if config["tooltip_enabled"]:
            tooltip = [
                alt.Tooltip(f"{config['x_field']}:{config['x_type']}", title=config["x_label"]),
                alt.Tooltip(f"{config['y_field']}:Q", title=config["y_label"]),
                alt.Tooltip(f"{config['category_field']}:N", title=config["legend_title"]),
            ]
        else:
            tooltip = alt.Undefined
            
        # ALTAIR LINE + POINTS: Combined mark for better visibility
        if config["point_size"] > 0:
            line = base.mark_line(
                strokeWidth=config["stroke_width"], 
                opacity=config["opacity"]
            ).encode(x=x_enc, y=y_enc, color=color_enc)
            
            points = base.mark_circle(
                size=config["point_size"], 
                opacity=config["opacity"] + 0.2
            ).encode(x=x_enc, y=y_enc, color=color_enc, tooltip=tooltip)
            
            chart = line + points
        else:
            chart = base.mark_line(
                strokeWidth=config["stroke_width"], 
                opacity=config["opacity"]
            ).encode(x=x_enc, y=y_enc, color=color_enc, tooltip=tooltip)
    else:
        # SINGLE LINE: No categorical grouping
        if config["tooltip_enabled"]:
            tooltip = [
                alt.Tooltip(f"{config['x_field']}:{config['x_type']}", title=config["x_label"]),
                alt.Tooltip(f"{config['y_field']}:Q", title=config["y_label"]),
            ]
        else:
            tooltip = alt.Undefined
            
        chart = base.mark_line(
            strokeWidth=config["stroke_width"], 
            opacity=config["opacity"], 
            color="steelblue"
        ).encode(x=x_enc, y=y_enc, tooltip=tooltip)

    # ALTAIR RESPONSIVE PROPERTIES
    chart = chart.properties(
        width="container",
        height="container", 
        title=alt.TitleParams(text=config["title"], anchor="start", fontSize=16)
    )

    # ALTAIR STYLING
    chart = chart.configure_view(
        strokeWidth=0
    ).configure_axis(
        labelFontSize=11, 
        titleFontSize=12
    ).configure_legend(
        labelFontSize=11,
        titleFontSize=12
    )

    # ALTAIR INTERACTIVITY
    if config.get("interactive"):
        chart = chart.interactive()

    # ALTAIR HTML GENERATION
    chart_html = chart.to_html()

    # RESPONSIVE HTML WRAPPER
    html_template = Template("""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <title>{{ title }}</title>
      <style>
        :root { --padding: 16px; --bg: #f8f9fa; --card-bg: #ffffff; --radius: 10px; }
        html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:var(--bg); }
        .wrap { box-sizing:border-box; padding:var(--padding); height:100%; display:flex; align-items:center; justify-content:center; }
        .card { width:100%; max-width:1200px; height:calc(100% - 2*var(--padding)); background:var(--card-bg); border-radius:var(--radius); box-shadow:0 4px 20px rgba(0,0,0,0.08); overflow:hidden; }
        .card .vega-embed, .card .vega-embed > .vega-visualization { width:100% !important; height:100% !important; }
        @media (max-width:640px) { :root { --padding:8px; } .card { border-radius:6px; } }
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="card">
          {{ chart_html | safe }}
        </div>
      </div>
    </body>
    </html>
    """)

    rendered = html_template.render(title=config["title"], chart_html=chart_html)
    return common.html_to_obj(rendered)