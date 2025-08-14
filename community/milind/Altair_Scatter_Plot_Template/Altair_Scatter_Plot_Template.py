@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Altair Interactive Scatter Plot - Penguin Dataset
    # WHEN TO USE: Correlation analysis, relationship between 2 continuous variables, outlier detection, clustering visualization
    # DATA REQUIREMENTS: 2 continuous numeric variables (X,Y), optional categorical for color/shape grouping, optional size variable
    # ALTAIR SCATTER SPECIFICS: Automatic scale optimization, multi-dimensional encoding, brush selection, regression lines
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
        ALTAIR SCATTER PLOT DATA REQUIREMENTS:
        - DataFrame with 2+ continuous numeric columns for X/Y coordinates
        - Optional categorical column for color/shape grouping
        - Optional continuous column for point size encoding
        - Altair handles missing values and outliers automatically
        """
        # Using Palmer Penguins dataset for multi-dimensional analysis
        url = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        return pd.read_csv(url)

    df = load_data()

    # ALTAIR SCATTER PLOT CONFIGURATION
    config = {
        # ALTAIR FIELD SPECIFICATIONS: Core X/Y mapping
        "x_field": "bill_length_mm",        # Continuous X-axis variable
        "y_field": "bill_depth_mm",           # Continuous Y-axis variable  
        "category_field": "species",          # Optional: Color/shape grouping (None to disable)
        "size_field": "body_mass_g",          # Optional: Point size encoding (None for uniform size)
        
        # ALTAIR VISUAL ENCODING
        "color_scheme": "category10",       # Color scheme for categorical grouping
        "shape_scheme": ["circle", "square", "triangle-up"],  # Shapes for categories
        "point_size": 100,                  # Base point size (when size_field is None)
        "opacity": 0.7,                     # Point transparency
        "stroke_width": 1,                  # Point border width
        
        # ALTAIR STATISTICAL OVERLAYS
        "show_regression": True,              # Add regression line per category
        "show_confidence": False,           # Show confidence intervals
        
        # ALTAIR INTERACTIVITY
        "interactive": True,                # Enable zoom/pan
        "brush_selection": True,            # Enable brush selection
        "tooltip_enabled": True,            # Show hover tooltips
        
        # LABELS AND STYLING
        "title": "Palmer Penguins: Bill Dimensions Analysis",
        "x_label": "Bill Length (mm)",
        "y_label": "Bill Depth (mm)",
        "legend_title": "Species",
    }

    # ALTAIR CHART CONSTRUCTION
    base = alt.Chart(df)

    # ALTAIR ENCODING: Multi-dimensional visual mapping
    x_enc = alt.X(f"{config['x_field']}:Q", title=config["x_label"])
    y_enc = alt.Y(f"{config['y_field']}:Q", title=config["y_label"])

    # CONDITIONAL CATEGORICAL ENCODING
    if config["category_field"] and config["category_field"] in df.columns:
        color_enc = alt.Color(
            f"{config['category_field']}:N",
            scale=alt.Scale(scheme=config["color_scheme"]),
            legend=alt.Legend(title=config["legend_title"])
        )
        
        # ALTAIR SHAPE ENCODING: Additional categorical dimension
        shape_enc = alt.Shape(
            f"{config['category_field']}:N",
            scale=alt.Scale(range=config["shape_scheme"]),
            legend=alt.Legend(title=config["legend_title"])
        )
    else:
        color_enc = alt.value("steelblue")
        shape_enc = alt.value("circle")

    # ALTAIR SIZE ENCODING: Optional continuous size mapping
    if config["size_field"] and config["size_field"] in df.columns:
        size_enc = alt.Size(
            f"{config['size_field']}:Q",
            scale=alt.Scale(range=[50, 300]),
            legend=alt.Legend(title=config["size_field"])
        )
    else:
        size_enc = alt.value(config["point_size"])

    # ALTAIR TOOLTIP SYSTEM
    if config["tooltip_enabled"]:
        tooltip_fields = [
            alt.Tooltip(f"{config['x_field']}:Q", title=config["x_label"]),
            alt.Tooltip(f"{config['y_field']}:Q", title=config["y_label"]),
        ]
        if config["category_field"] and config["category_field"] in df.columns:
            tooltip_fields.append(alt.Tooltip(f"{config['category_field']}:N", title=config["legend_title"]))
        if config["size_field"] and config["size_field"] in df.columns:
            tooltip_fields.append(alt.Tooltip(f"{config['size_field']}:Q", title=config["size_field"]))
        tooltip = tooltip_fields
    else:
        tooltip = alt.Undefined

    # ALTAIR BRUSH SELECTION: Interactive data filtering
    if config["brush_selection"]:
        brush = alt.selection_interval()
        scatter_base = base.add_selection(brush)
    else:
        scatter_base = base
        brush = None

    # ALTAIR SCATTER POINTS: Main visualization layer
    points = scatter_base.mark_circle(
        opacity=config["opacity"],
        stroke="white",
        strokeWidth=config["stroke_width"]
    ).encode(
        x=x_enc, 
        y=y_enc, 
        color=color_enc, 
        shape=shape_enc,
        size=size_enc, 
        tooltip=tooltip
    )

    # ALTAIR CONDITIONAL HIGHLIGHTING: Brush selection feedback
    if brush:
        points = points.encode(
            opacity=alt.condition(brush, alt.value(config["opacity"]), alt.value(0.1))
        )

    chart = points

    # ALTAIR REGRESSION OVERLAY: Statistical trend lines
    if config["show_regression"] and config["category_field"] and config["category_field"] in df.columns:
        regression = base.mark_line(size=3, opacity=0.8).transform_regression(
            config["x_field"], config["y_field"], groupby=[config["category_field"]]
        ).encode(
            x=x_enc,
            y=y_enc,
            color=color_enc
        )
        chart = chart + regression

    # ALTAIR RESPONSIVE PROPERTIES
    chart = chart.properties(
        width="container",
        height="container",
        title=alt.TitleParams(text=config["title"], anchor="start", fontSize=16)
    )

    # ALTAIR STYLING CONFIGURATION
    chart = chart.configure_view(
        strokeWidth=0
    ).configure_axis(
        labelFontSize=11, 
        titleFontSize=12,
        grid=True,
        gridOpacity=0.3
    ).configure_legend(
        labelFontSize=11,
        titleFontSize=12,
        orient="right"
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
        :root { --padding: 16px; --bg: #f5f7fa; --card-bg: #ffffff; --radius: 12px; }
        html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:var(--bg); }
        .wrap { box-sizing:border-box; padding:var(--padding); height:100%; display:flex; align-items:center; justify-content:center; }
        .card { width:100%; max-width:1200px; height:calc(100% - 2*var(--padding)); background:var(--card-bg); border-radius:var(--radius); box-shadow:0 6px 25px rgba(0,0,0,0.08); overflow:hidden; }
        .card .vega-embed, .card .vega-embed > .vega-visualization { width:100% !important; height:100% !important; }
        @media (max-width:640px) { :root { --padding:8px; } .card { border-radius:8px; } }
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