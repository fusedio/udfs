@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Altair Interactive Histogram
    # WHEN TO USE: Distribution analysis with built-in interactivity (zoom/pan), automatic binning, easy categorical grouping
    # DATA REQUIREMENTS: 1 continuous numeric variable, optional categorical variable for color grouping
    # ALTAIR HISTOGRAM SPECIFICS: Declarative grammar, automatic binning, built-in tooltips, responsive containers, interactive by default
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
        ALTAIR HISTOGRAM DATA REQUIREMENTS:
        - Must return pandas DataFrame (Altair's native input format)
        - Numeric column should be continuous values
        - Optional categorical column for color-coded grouping
        - Altair handles missing values automatically
        - No manual data cleaning required (unlike D3 implementations)
        """
        url = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        return pd.read_csv(url)

    df = load_data()

    # ALTAIR HISTOGRAM CONFIGURATION
    config = {
        # ALTAIR FIELD SPECIFICATIONS: Use exact column names from DataFrame
        "numeric_field": "bill_length_mm",   # Column for X-axis binning
        "category_field": "species",         # Column for color grouping (None to disable)
        
        # ALTAIR BINNING: Automatic bin calculation
        "max_bins": 30,                      # Altair automatically determines optimal bins up to this max
        
        # ALTAIR VISUAL ENCODING
        "color_scheme": "category10",        # Altair color scheme name (category10, tableau10, etc.)
        "interactive": True,                 # Enable Altair's built-in zoom/pan interactions
        
        # LABELS
        "title": "Distribution of Penguin Bill Lengths",
        "x_label": "Bill Length (mm)",
        "y_label": "Count",
    }

    # ALTAIR CHART CONSTRUCTION: Declarative grammar approach
    base = alt.Chart(df)

    # ALTAIR ENCODING SYSTEM: Define how data maps to visual properties
    x_enc = alt.X(
        f"{config['numeric_field']}:Q",               # :Q indicates quantitative (continuous) data
        bin=alt.Bin(maxbins=config["max_bins"]),      # Automatic binning with max limit
        title=config["x_label"]
    )
    y_enc = alt.Y("count()", title=config["y_label"])  # Automatic count aggregation

    # CONDITIONAL CATEGORICAL GROUPING: Color encoding based on category presence
    if config["category_field"] and config["category_field"] in df.columns:
        # ALTAIR COLOR ENCODING: Automatic categorical color assignment
        color_enc = alt.Color(
            f"{config['category_field']}:N",             # :N indicates nominal (categorical) data
            scale=alt.Scale(scheme=config["color_scheme"]), # Altair color scheme
            legend=alt.Legend(title=config.get("legend_title", config["category_field"]))
        )
        
        # ALTAIR TOOLTIP SYSTEM: Automatic interactive tooltips
        tooltip = [
            alt.Tooltip(f"{config['numeric_field']}:Q", title=config["x_label"]),
            alt.Tooltip(f"{config['category_field']}:N", title=config["category_field"]),
            alt.Tooltip("count()", title="Count"),
        ]
        
        chart = base.mark_bar(opacity=0.85, stroke="white", strokeWidth=1).encode(
            x=x_enc, y=y_enc, color=color_enc, tooltip=tooltip
        )
    else:
        # UNGROUPED HISTOGRAM: Single color, simpler tooltip
        tooltip = [
            alt.Tooltip(f"{config['numeric_field']}:Q", title=config["x_label"]),
            alt.Tooltip("count()", title="Count"),
        ]
        chart = base.mark_bar(opacity=0.9, stroke="white", strokeWidth=1).encode(
            x=x_enc, y=y_enc, tooltip=tooltip
        )

    # ALTAIR RESPONSIVE CONTAINER: Auto-sizing within parent container
    chart = chart.properties(
        width="container",      # Fills container width
        height="container",     # Fills container height  
        title=alt.TitleParams(text=config["title"], anchor="start")
    )

    # ALTAIR STYLING CONFIGURATION
    chart = chart.configure_view(strokeWidth=0).configure_axis(labelFontSize=11, titleFontSize=12)

    # ALTAIR INTERACTIVITY: Built-in zoom and pan
    if config.get("interactive"):
        chart = chart.interactive()  # Adds zoom/pan without custom code

    # ALTAIR HTML GENERATION: Converts chart to standalone HTML
    chart_html = chart.to_html()

    # RESPONSIVE WRAPPER: Embeds Altair output in responsive container
    html_template = Template("""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <title>{{ title }}</title>
      <style>
        /* Responsive layout for Altair embedding */
        :root { --padding: 16px; --bg: #f6f7f9; --card-bg: #ffffff; --radius: 8px; }
        html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial; background:var(--bg); }
        .wrap { box-sizing:border-box; padding:var(--padding); height:100%; display:flex; align-items:center; justify-content:center; }
        .card { width:100%; max-width:1100px; height:calc(100% - 2*var(--padding)); background:var(--card-bg); border-radius:var(--radius); box-shadow:0 2px 10px rgba(0,0,0,0.06); overflow:hidden; }
        
        /* ALTAIR RESPONSIVE INTEGRATION: Ensure Vega-Lite fills container */
        .card .vega-embed, .card .vega-embed > .vega-visualization { width:100% !important; height:100% !important; }
        
        @media (max-width:640px) {
          :root { --padding:8px; }
          .card { border-radius:6px; }
        }
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