@fused.udf
def udf(
    correlation_threshold: float = 0.0,
    color_scheme: str = "redblue",
    show_text_labels: bool = True,
    text_threshold: float = 0.5,
):
    """
    =============================================================================
    CHART TYPE: Altair Correlation Heatmap – Wine Dataset Feature Correlation Matrix
    WHEN TO USE: Visualize correlations between wine dataset features
    DATA REQUIREMENTS: Wine dataset with multiple numeric columns for correlation analysis
    HEATMAP SPECIFICS: Color-coded correlation values with optional text labels
    =============================================================================
    """
    import pandas as pd
    import altair as alt
    import numpy as np
    from sklearn.datasets import load_wine
    from jinja2 import Template

    # REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    # ALTAIR REQUIREMENT: Enable processing of larger datasets
    alt.data_transformers.enable("default", max_rows=None)

    # ------------------------------------------------------------------
    # 1️⃣ DATA LOADING (wine dataset)
    # ------------------------------------------------------------------
    @fused.cache
    def load_wine_data() -> pd.DataFrame:
        """Load wine dataset and return as DataFrame with target info"""
        print("Loading wine dataset...")

        wine = load_wine()
        df = pd.DataFrame(wine.data, columns=wine.feature_names)
        df["target"] = wine.target
        df["target_name"] = df["target"].map(
            {i: name for i, name in enumerate(wine.target_names)}
        )

        print(f"Loaded wine dataset shape: {df.shape}")
        return df

    df = load_wine_data()

    # ------------------------------------------------------------------
    # 2️⃣ DIAGNOSTICS – print column data‑types & sample rows
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
        # CORRELATION SETTINGS
        "correlation_threshold": correlation_threshold,  # filter correlations above/below this value
        "method": "pearson",  # correlation method

        # VISUAL ENCODING
        "color_scheme": color_scheme,  # color scheme for heatmap
        "color_domain": [-1, 1],  # correlation range
        "cell_size": 40,  # size of each cell

        # TEXT LABELS
        "show_text": show_text_labels,  # show correlation values as text
        "text_threshold": text_threshold,  # threshold for white vs black text
        "text_format": ".2f",  # number format for text labels
        "text_size": 9,  # font size for text

        # LAYOUT & STYLING
        "width": "container",
        "height": "container",
        "title": "Wine Dataset: Feature Correlation Heatmap",

        # INTERACTIVITY
        "tooltip_enabled": True,
    }

    # ------------------------------------------------------------------
    # 4️⃣ DATA PREPARATION – extract numeric columns and compute correlations
    # ------------------------------------------------------------------
    print("\n=== Data preparation ===")

    # Get only numeric columns (exclude target columns for cleaner correlation matrix)
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    feature_cols = [col for col in numeric_cols if not col.startswith("target")]

    print(f"Numeric columns: {len(numeric_cols)}")
    print(f"Feature columns for correlation: {len(feature_cols)}")

    if len(feature_cols) < 2:
        return "<div>Need at least 2 numeric features for correlation analysis</div>"

    # Calculate correlation matrix
    correlation_matrix = df[feature_cols].corr(method=config["method"])
    print(f"Correlation matrix shape: {correlation_matrix.shape}")

    # ------------------------------------------------------------------
    # 5️⃣ DATA TRANSFORMATION – convert correlation matrix to long format
    # ------------------------------------------------------------------
    corr_data = correlation_matrix.reset_index().melt(id_vars="index")
    corr_data = corr_data.rename(
        columns={"index": "variable1", "variable": "variable2", "value": "correlation"}
    )

    # Apply correlation threshold filter if specified
    if config["correlation_threshold"] > 0:
        corr_data = corr_data[
            abs(corr_data["correlation"]) >= config["correlation_threshold"]
        ]
        print(
            f"Filtered correlations >= {config['correlation_threshold']}: {len(corr_data)} pairs"
        )

    # ------------------------------------------------------------------
    # 6️⃣ BUILD THE ALTAIR CHART
    # ------------------------------------------------------------------
    # Base heatmap
    heatmap = (
        alt.Chart(corr_data)
        .mark_rect()
        .encode(
            x=alt.X(
                "variable1:N",
                title=None,
                sort=feature_cols,
                axis=alt.Axis(labelAngle=-45, labelLimit=150),
            ),
            y=alt.Y(
                "variable2:N",
                title=None,
                sort=feature_cols,
            ),
            color=alt.Color(
                "correlation:Q",
                scale=alt.Scale(scheme=config["color_scheme"], domain=config["color_domain"]),
                title="Correlation",
            ),
            tooltip=[
                alt.Tooltip("variable1:N", title="Variable 1"),
                alt.Tooltip("variable2:N", title="Variable 2"),
                alt.Tooltip("correlation:Q", title="Correlation", format=".3f"),
            ]
            if config["tooltip_enabled"]
            else alt.Undefined,
        )
        .properties(
            width=config["width"],
            height=config["height"],
            title=alt.TitleParams(text=config["title"], anchor="start", fontSize=16),
        )
    )

    # Add text labels if enabled
    if config["show_text"]:
        text_labels = (
            alt.Chart(corr_data)
            .mark_text(baseline="middle", fontSize=config["text_size"])
            .encode(
                x=alt.X("variable1:N", sort=feature_cols),
                y=alt.Y("variable2:N", sort=feature_cols),
                text=alt.Text("correlation:Q", format=config["text_format"]),
                color=alt.condition(
                    alt.datum.correlation > config["text_threshold"],
                    alt.value("white"),
                    alt.value("black"),
                ),
            )
        )
        chart = heatmap + text_labels
    else:
        chart = heatmap

    # Configure chart appearance
    chart = (
        chart.configure_view(strokeWidth=0)
        .configure_axis(labelFontSize=10, titleFontSize=12)
        .configure_legend(labelFontSize=11, titleFontSize=12)
    )

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
        """
    )

    rendered = html_template.render(title=config["title"], chart_html=chart_html)
    return common.html_to_obj(rendered)