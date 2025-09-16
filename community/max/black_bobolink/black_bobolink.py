@fused.udf
def udf():
    """
    Load the Palmer Penguins dataset, show summary statistics for body_mass_g,
    and display a histogram of penguin body mass (proxy for height).
    """
    import pandas as pd
    import altair as alt
    import json

    # Load common HTML helper
    common = fused.load(
        "https://github.com/fusedio/udfs/tree/fbf5682/public/common/"
    )

    # ------------------------------------------------------------------
    # Cached data loading (will be reused across runs)
    # ------------------------------------------------------------------
    @fused.cache
    def load_penguins():
        url = (
            "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/"
            "master/inst/extdata/penguins.csv"
        )
        # Robust CSV reading with multiple encodings
        try:
            df = pd.read_csv(url, encoding="utf-8", on_bad_lines="skip")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(url, encoding="utf-8-sig", on_bad_lines="skip")
            except UnicodeDecodeError:
                df = pd.read_csv(url, encoding="latin1", on_bad_lines="skip")
        return df

    df = load_penguins()

    # ------------------------------------------------------------------
    # Data preparation: drop rows without body_mass_g
    # ------------------------------------------------------------------
    df_clean = df.dropna(subset=["body_mass_g"])

    # ------------------------------------------------------------------
    # Summary statistics (as a small HTML table)
    # ------------------------------------------------------------------
    stats_series = df_clean["body_mass_g"].describe()
    # Convert to DataFrame for nicer HTML rendering
    stats_df = stats_series.to_frame(name="value").reset_index()
    stats_df.columns = ["statistic", "value"]
    # Ensure numeric formatting with one decimal place where appropriate
    stats_df["value"] = stats_df["value"].apply(
        lambda x: f"{x:,.1f}" if isinstance(x, (int, float)) else x
    )
    stats_html = stats_df.to_html(index=False, border=0, classes="summary-stats")
    # Simple CSS to make the table look tidy
    stats_css = """
    <style>
    .summary-stats {
        font-family: Arial, Helvetica, sans-serif;
        border-collapse: collapse;
        width: 50%;
        margin-top: 10px;
    }
    .summary-stats th, .summary-stats td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: right;
    }
    .summary-stats th {
        background-color: #f2f2f2;
        font-weight: bold;
    }
    </style>
    """

    # ------------------------------------------------------------------
    # Histogram chart (Altair)
    # ------------------------------------------------------------------
    histogram = (
        alt.Chart(df_clean)
        .mark_bar(color="#FF0000")  # Changed color from blue to red
        .encode(
            x=alt.X(
                "body_mass_g",
                bin=alt.Bin(maxbins=30),
                title="Body Mass (g)",
            ),
            y=alt.Y("count()", title="Number of Penguins"),
            tooltip=[
                alt.Tooltip("count()", title="Count"),
                alt.Tooltip("body_mass_g:Q", bin=alt.Bin(maxbins=30), title="Body Mass (g)"),
            ],
        )
        .properties(
            width=600,
            height=400,
            title="Penguin Body Mass Distribution (Antarctica Research Stations)",
        )
    )
    chart_html = histogram.to_html()

    # ------------------------------------------------------------------
    # Assemble final HTML (title, stats, chart)
    # ------------------------------------------------------------------
    final_html = f"""
    <h2>Penguin Size Distribution</h2>
    <p>This histogram visualizes the distribution of <b>body_mass_g</b>, used as a proxy for penguin height/size, from the Palmer Penguins dataset collected at Antarctic research stations.</p>
    {stats_css}
    <h3>Summary Statistics for Body Mass (g)</h3>
    {stats_html}
    <h3>Histogram</h3>
    {chart_html}
    """

    return common.html_to_obj(final_html)