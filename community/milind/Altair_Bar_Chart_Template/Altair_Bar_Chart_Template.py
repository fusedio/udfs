@fused.udf
def udf():
    """
    Load the Google Mobility City‑Daily dataset, cache the load,
    print column data‑types, and return an interactive Altair bar chart.
    """
    import pandas as pd
    import altair as alt
    from jinja2 import Template

    # --------------------------------------------------------------------
    # Load data (cached)
    # --------------------------------------------------------------------
    @fused.cache
    def load_data():
        url = (
            "https://raw.githubusercontent.com/OpportunityInsights/"
            "EconomicTracker/main/data/Google%20Mobility%20-%20City%20-%20Daily.csv"
        )
        return pd.read_csv(url)

    df = load_data()

    # --------------------------------------------------------------------
    # Show column data‑types (useful for exploration)
    # --------------------------------------------------------------------
    print("=== Column data types ===")
    print(df.dtypes)

    # --------------------------------------------------------------------
    # Choose fields for the bar chart
    # --------------------------------------------------------------------
    numeric_field = "gps_retail_and_recreation"   # continuous numeric column
    category_field = "cityid"                    # grouping column (optional)

    # --------------------------------------------------------------------
    # Altair bar‑chart configuration
    # --------------------------------------------------------------------
    config = {
        "numeric_field": numeric_field,
        "category_field": category_field,
        "color_scheme": "category10",
        "interactive": True,
        "tooltip_enabled": True,
        "title": "Average Retail & Recreation Mobility by City",
        "x_label": "City ID",
        "y_label": "Average Mobility Change",
        "legend_title": "City ID",
    }

    # Enable large‑dataset handling for Altair
    alt.data_transformers.enable("default", max_rows=None)

    base = alt.Chart(df)

    # X‑axis: categorical city ID
    x_enc = alt.X(
        f"{config['category_field']}:N",
        title=config["x_label"],
        sort=alt.SortField(field=config["category_field"], order="ascending"),
    )
    # Y‑axis: mean of the numeric field
    y_enc = alt.Y(
        f"mean({config['numeric_field']}):Q",
        title=config["y_label"],
    )

    # Colour encoding (optional)
    if config["category_field"] and config["category_field"] in df.columns:
        color_enc = alt.Color(
            f"{config['category_field']}:N",
            scale=alt.Scale(scheme=config["color_scheme"]),
            legend=alt.Legend(title=config["legend_title"]),
        )
        # Tooltip
        if config["tooltip_enabled"]:
            tooltip = [
                alt.Tooltip(f"{config['category_field']}:N", title=config["legend_title"]),
                alt.Tooltip(f"mean({config['numeric_field']}):Q", title=config["y_label"]),
            ]
        else:
            tooltip = alt.Undefined

        chart = (
            base.mark_bar(opacity=0.85, stroke="white", strokeWidth=1)
            .encode(x=x_enc, y=y_enc, color=color_enc, tooltip=tooltip)
        )
    else:
        # No grouping (single bar)
        if config["tooltip_enabled"]:
            tooltip = [
                Alt.Tooltip(f"mean({config['numeric_field']}):Q", title=config["y_label"]),
            ]
        else:
            tooltip = Alt.Undefined

        chart = (
            base.mark_bar(opacity=0.85, stroke="white", strokeWidth=1)
            .encode(x=x_enc, y=y_enc, tooltip=tooltip)
        )

    # Responsive container & styling
    chart = (
        chart.properties(
            width="container",
            height="container",
            title=alt.TitleParams(text=config["title"], anchor="start", fontSize=16),
        )
        .configure_view(strokeWidth=0)
        .configure_axis(labelFontSize=11, titleFontSize=12)
        .configure_legend(labelFontSize=11, titleFontSize=12)
    )

    # Interactivity
    if config["interactive"]:
        chart = chart.interactive()

    # Convert to HTML
    chart_html = chart.to_html()

    # Responsive HTML wrapper
    html_template = Template(
        """
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
        """
    )
    rendered = html_template.render(title=config["title"], chart_html=chart_html)

    # Load common utilities and return the HTML object
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    return common.html_to_obj(rendered)