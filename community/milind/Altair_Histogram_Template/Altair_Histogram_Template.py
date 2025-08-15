@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Altair Interactive Histogram (simplified single-series histogram)
    # WHEN TO USE: Quick distribution overview of one numeric variable
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
        """
        url = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        return pd.read_csv(url)

    df = load_data()

    # ALTAIR HISTOGRAM CONFIGURATION
    config = {
        # Use a single numeric field and no categorical grouping for a simple histogram
        "numeric_field": "bill_length_mm",
        "category_field": None,                # <-- set to None for a simple single-series histogram

        "max_bins": 30,                       # Automatic bin calculation upper bound

        # Simple single-color styling
        "color_scheme": "category10",
        "interactive": False,                 # disable interactive zoom/pan for a simpler chart

        # LABELS
        "title": "Distribution of Penguin Bill Lengths (Simple Histogram)",
        "x_label": "Bill Length (mm)",
        "y_label": "Count",
    }

    # ALTAIR CHART CONSTRUCTION
    base = alt.Chart(df)

    x_enc = alt.X(
        f"{config['numeric_field']}:Q",
        bin=alt.Bin(maxbins=config["max_bins"]),
        title=config["x_label"]
    )
    y_enc = alt.Y("count()", title=config["y_label"])

    # SIMPLE HISTOGRAM (no categorical grouping)
    tooltip = [
        alt.Tooltip(f"{config['numeric_field']}:Q", title=config["x_label"]),
        alt.Tooltip("count()", title="Count"),
    ]
    # Use a single clean color and remove stroke to avoid "brick" visuals
    chart = base.mark_bar(color="#4c78a8", opacity=1).encode(
        x=x_enc, y=y_enc, tooltip=tooltip
    )

    # RESPONSIVE CONTAINER
    chart = chart.properties(
        width="container",
        height="container",
        title=alt.TitleParams(text=config["title"], anchor="start")
    )

    # STYLING
    chart = chart.configure_view(strokeWidth=0).configure_axis(labelFontSize=11, titleFontSize=12)

    # Keep interaction off for a simple static histogram
    if config.get("interactive"):
        chart = chart.interactive()

    # Convert to standalone HTML
    chart_html = chart.to_html()

    # RESPONSIVE WRAPPER
    html_template = Template("""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <title>{{ title }}</title>
      <style>
        :root { --padding: 16px; --bg: #f6f7f9; --card-bg: #ffffff; --radius: 8px; }
        html,body { height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial; background:var(--bg); }
        .wrap { box-sizing:border-box; padding:var(--padding); height:100%; display:flex; align-items:center; justify-content:center; }
        .card { width:100%; max-width:1100px; height:calc(100% - 2*var(--padding)); background:var(--card-bg); border-radius:var(--radius); box-shadow:0 2px 10px rgba(0,0,0,0.06); overflow:hidden; }
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