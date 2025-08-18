@fused.udf
def udf(
    filename: str = "cheeses.csv",
    max_bars: int = 20,
    label_angle: int = -45,
    label_limit: int = 150,
):
    """
    =============================================================================
    CHART TYPE: Altair Grouped Bar Chart – Cheese Count by Country & Milk Type
    WHEN TO USE: Compare counts across two categorical dimensions (country + milk)
    DATA REQUIREMENTS: Two categorical columns (country, milk) and one numeric count
    BAR CHART SPECIFICS: Top-N countries sorted by total count, grouped bars by milk type
    =============================================================================
    """

    import pandas as pd
    import altair as alt
    from jinja2 import Template

    # REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    # ALTAIR REQUIREMENT: Enable processing of larger datasets
    alt.data_transformers.enable("default", max_rows=None)

    # ------------------------------------------------------------------
    # 1️⃣ DATA LOADING (cached, tolerant encoding)
    # ------------------------------------------------------------------
    @fused.cache
    def load_data(file_name: str) -> pd.DataFrame:
        base_url = (
            "https://raw.githubusercontent.com/rfordatascience/tidytuesday/"
            "main/data/2024/2024-06-04/"
        )
        raw_url = f"{base_url}{file_name}"
        print(f"Loading data from: {raw_url}")
        df = pd.read_csv(raw_url, encoding="utf-8-sig", low_memory=False)
        print(f"Loaded data shape: {df.shape}")
        return df

    df = load_data(filename)

    # ------------------------------------------------------------------
    # 2️⃣ DIAGNOSTICS – print column data-types & sample rows
    # ------------------------------------------------------------------
    print("=== Column data types ===")
    print(df.dtypes)
    print("\n=== First few rows ===")
    print(df.head())
    print("\n=== Available columns ===")
    print(df.columns.tolist())

    # ------------------------------------------------------------------
    # 3️⃣ CONFIGURATION – edit these values for quick reuse
    # ------------------------------------------------------------------
    config = {
        # AXIS & AGGREGATION
        "x_field": "country",          # categorical axis
        "y_field": "cheese_count",     # aggregated count
        "group_field": "milk",         # grouping / stacking field
        "max_bars": max_bars,          # limit to top-N countries
        "agg_func": "sum",              # Altair aggregation verb

        # VISUAL ENCODING
        "color_scheme": "category10",
        "opacity": 0.85,
        "stroke_width": 1,

        # INTERACTIVITY
        "interactive": True,
        "tooltip_enabled": True,

        # LABELS & TITLE
        "title": f"Top {max_bars} Countries – Cheeses per Country (Grouped by Milk Type)",
        "x_label": "Country",
        "y_label": "Number of Cheeses",
        "legend_title": "Milk Type",
    }

    # ------------------------------------------------------------------
    # 4️⃣ DATA CLEANING – drop rows with missing country or milk
    # ------------------------------------------------------------------
    print("\n=== Data cleaning ===")
    print(f"Original shape: {df.shape}")
    print(f"Missing 'country': {df['country'].isna().sum()}")
    print(f"Missing 'milk': {df['milk'].isna().sum()}")
    df_clean = df.dropna(subset=["country", "milk"])
    print(f"Cleaned shape: {df_clean.shape}")

    if df_clean.empty:
        return "<div>No data after cleaning missing values</div>"

    # ------------------------------------------------------------------
    # 5️⃣ AGGREGATE – count of cheeses per country, grouped by milk type
    # ------------------------------------------------------------------
    df_counts = (
        df_clean.groupby(["country", "milk"])
        .size()
        .reset_index(name=config["y_field"])
    )

    # Keep only the top-N countries by total cheese count
    top_countries = (
        df_counts.groupby("country")[config["y_field"]]
        .sum()
        .reset_index(name="total")
        .sort_values("total", ascending=False)
        .head(config["max_bars"])["country"]
        .tolist()
    )
    df_counts = df_counts[df_counts["country"].isin(top_countries)]

    # Order the x-axis by total count (descending)
    country_order = (
        df_counts.groupby("country")[config["y_field"]]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    # ------------------------------------------------------------------
    # 6️⃣ BUILD THE ALTair CHART
    # ------------------------------------------------------------------
    chart = (
        alt.Chart(df_counts)
        .mark_bar(opacity=config["opacity"], stroke="white", strokeWidth=config["stroke_width"])
        .encode(
            x=alt.X(
                f"{config['x_field']}:N",
                title=config["x_label"],
                sort=country_order,
                axis=alt.Axis(
                    labelAngle=label_angle,
                    labelFontSize=11,
                    labelLimit=label_limit,
                    titleFontSize=12,
                ),
            ),
            y=alt.Y(f"{config['y_field']}:Q", title=config["y_label"]),
            color=alt.Color(
                f"{config['group_field']}:N",
                scale=alt.Scale(scheme=config["color_scheme"]),
                legend=alt.Legend(title=config["legend_title"]),
            ),
            tooltip=[
                alt.Tooltip(f"{config['x_field']}:N", title=config["x_label"]),
                alt.Tooltip(f"{config['group_field']}:N", title=config["legend_title"]),
                alt.Tooltip(f"{config['y_field']}:Q", title=config["y_label"]),
            ],
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

    # Add interactivity if requested
    if config.get("interactive"):
        chart = chart.interactive()

    # ------------------------------------------------------------------
    # 7️⃣ CONVERT TO HTML & WRAP FOR FUSED
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

    return common.html_to_obj(rendered)