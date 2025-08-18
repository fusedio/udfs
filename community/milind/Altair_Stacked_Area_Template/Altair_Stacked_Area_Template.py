@fused.udf
def udf(
    # ──────────────────────────────────────────────────────────────────────
    # 👉 1️⃣  DATA SOURCE – change the default URL to point at your own file
    # ──────────────────────────────────────────────────────────────────────
    url: str = "https://carbonmajors.org/evoke/391/get_cm_file?type=Basic&file=emissions_medium_granularity.csv"
):
    """
    Stacked‑area‑chart template (Altair).

    • Loads a CSV (with tolerant encoding) and prints the column dtypes.
    • Performs minimal cleaning – forces numeric columns to numbers.
    • Aggregates the *y* field (default: sum) per *x*‑*category* pair.
    • Renders an interactive Altair stacked area chart wrapped in a responsive
      HTML page for the Fused Workbench.

    To reuse:
    1️⃣ Replace the `url` default (or pass a different URL when calling the UDF).
    2️⃣ Edit the `config` dictionary below:
       - `x_field`   – column for the horizontal axis (usually a date/year).
       - `y_field`   – numeric column to aggregate (e.g. sales, emissions).
       - `agg_func`  – Altair aggregation verb: "sum", "mean", "count", …
       - `category_field` – column that defines the stack groups.
       - Titles, colours, opacity, etc.
    3️⃣ If your data uses a different date format, adjust `x_type` and
       `date_format` accordingly.
    """

    # ------------------------------------------------------------------
    # 2️⃣ IMPORTS & COMMON UTILITIES
    # ------------------------------------------------------------------
    import pandas as pd
    import altair as alt          # always use the lowercase alias `alt`
    from jinja2 import Template

    # Load the HTML helper that turns a string into a Fused object
    common = fused.load(
        "https://github.com/fusedio/udfs/tree/b672adc/public/common/"
    )

    # Allow Altair to handle large tables (no row limit)
    alt.data_transformers.enable("default", max_rows=None)

    # ------------------------------------------------------------------
    # 3️⃣ DATA LOADING (cached + tolerant encoding)
    # ------------------------------------------------------------------
    @fused.cache
    def load_data(csv_url: str) -> pd.DataFrame:
        """Read a CSV from `csv_url` using utf‑8‑sig or latin1 fallback."""
        try:
            return pd.read_csv(csv_url, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(csv_url, encoding="latin1")

    df = load_data(url)

    # ------------------------------------------------------------------
    # 4️⃣ DIAGNOSTIC: print column data‑types (helps you discover the schema)
    # ------------------------------------------------------------------
    print("=== Column data types ===")
    print(df.dtypes)

    # ------------------------------------------------------------------
    # 5️⃣ BASIC CLEANING – force numeric columns to proper dtypes
    # ------------------------------------------------------------------
    # Add any additional numeric columns you need to coerce here
    numeric_cols = ["production_value", "total_emissions_MtCO2e"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ------------------------------------------------------------------
    # 6️⃣ CONFIGURATION – edit the values below for each new dataset
    # ------------------------------------------------------------------
    config = {
        # ────── AXIS & AGGREGATION ──────
        "x_field": "year",                     # horizontal axis column
        "x_type": "T",                         # "T"=temporal, "Q"=quantitative, "N"=nominal
        "date_format": "%Y",                   # format for temporal axis (ignored if x_type != "T")
        "y_field": "total_emissions_MtCO2e",   # numeric column to aggregate
        "agg_func": "sum",                     # Altair aggregation verb (sum, mean, count, …)
        "category_field": "commodity",         # column that defines the stack groups

        # ────── VISUAL ENCODING ──────
        "color_scheme": "category10",
        "opacity": 0.85,
        "stroke_width": 2.5,

        # ────── INTERACTIVITY ──────
        "interactive": True,
        "tooltip_enabled": True,

        # ────── LABELS & TITLE ──────
        "title": "Carbon Majors – Total Emissions by Commodity (Stacked Area)",
        "x_label": "Year",
        "y_label": "Total Emissions (Mt CO₂e)",
        "legend_title": "Commodity",
    }

    # ------------------------------------------------------------------
    # 7️⃣ BUILD THE ALTair CHART
    # ------------------------------------------------------------------
    # Encode the aggregation dynamically using the config values
    y_encoding = f"{config['agg_func']}({config['y_field']}):Q"

    chart = (
        alt.Chart(df)
        .mark_area(opacity=config["opacity"])
        .encode(
            x=alt.X(
                f"{config['x_field']}:{config['x_type']}",
                title=config["x_label"],
                axis=alt.Axis(format=config["date_format"])
                if config["x_type"] == "T"
                else alt.Axis(),
            ),
            y=alt.Y(
                y_encoding,
                stack="zero",
                title=config["y_label"],
            ),
            color=alt.Color(
                f"{config['category_field']}:N",
                scale=alt.Scale(scheme=config["color_scheme"]),
                legend=alt.Legend(title=config["legend_title"]),
            ),
            tooltip=(
                [
                    alt.Tooltip(f"{config['x_field']}:{config['x_type']}", title=config["x_label"]),
                    alt.Tooltip(f"{config['category_field']}:N", title=config["legend_title"]),
                    alt.Tooltip(y_encoding, title=config["y_label"]),
                ]
                if config["tooltip_enabled"]
                else alt.Undefined
            ),
        )
        .properties(
            width="container",
            height="container",
            title=alt.TitleParams(text=config["title"], anchor="start", fontSize=16),
        )
        .configure_view(strokeWidth=0)
        .configure_axis(labelFontSize=11, titleFontSize=12)
        .configure_legend(labelFontSize=11, titleFontSize=12)
    )

    # Add pan/zoom if requested
    if config.get("interactive"):
        chart = chart.interactive()

    # ------------------------------------------------------------------
    # 8️⃣ CONVERT TO HTML & WRAP FOR FUSED
    # ------------------------------------------------------------------
    chart_html = chart.to_html()

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
            .card .vega-embed, .card .vega-visualization { width:100% !important; height:100% !important; }
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
    return common.html_to_obj(rendered)