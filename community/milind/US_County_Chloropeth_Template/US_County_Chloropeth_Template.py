@fused.udf
def udf():
    # ------------------------------------------------------------------
    # Helper utilities – HTML wrapper
    # ------------------------------------------------------------------
    common = fused.load(
        "https://github.com/fusedio/udfs/tree/fbf5682/public/common/"
    )

    # ------------------------------------------------------------------
    # Load & cache the dataset
    # ------------------------------------------------------------------
    import pandas as pd

    @fused.cache
    def load_covid_data():
        # NY Times US‑county COVID‑19 time series (CSV)
        url = (
            "https://raw.githubusercontent.com/nytimes/covid-19-data/master/"
            "us-counties.csv"
        )
        # Robust encoding for public CSVs
        df = pd.read_csv(url, encoding="utf-8-sig")
        # Clean column names
        df.columns = [
            col.strip()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .lower()
            for col in df.columns
        ]
        # Convert date column to datetime
        df["date"] = pd.to_datetime(df["date"])
        return df

    covid_df = load_covid_data()

    # ------------------------------------------------------------------
    # Inspect schema (useful for debugging)
    # ------------------------------------------------------------------
    print("=== Column data types ===")
    print(covid_df.dtypes)
    print("\n=== Sample rows ===")
    print(covid_df.head())

    # ------------------------------------------------------------------
    # Prepare data for the choropleth
    # ------------------------------------------------------------------
    # Keep only the most recent date for each county
    latest_date = covid_df["date"].max()
    latest_df = (
        covid_df[covid_df["date"] == latest_date]
        .groupby(["fips", "county", "state"], as_index=False)
        .agg({"cases": "sum", "deaths": "sum"})
    )
    # Ensure FIPS is a zero‑padded 5‑character string (required for lookup)
    latest_df["fips"] = latest_df["fips"].apply(lambda x: f"{int(x):05d}")

    # ------------------------------------------------------------------
    # Build the Altair choropleth
    # ------------------------------------------------------------------
    import altair as alt
    from jinja2 import Template

    # TopoJSON source for US counties (public CDN)
    counties_topojson = (
        "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json"
    )

    # Base map of counties
    base = (
        alt.Chart(alt.Data(url=counties_topojson, format=alt.DataFormat(type="topojson", feature="counties")))
        .mark_geoshape(stroke="gray", strokeWidth=0.2)
        .encode(
            tooltip=[
                alt.Tooltip("county:N", title="County"),
                alt.Tooltip("state:N", title="State"),
                alt.Tooltip("cases:Q", title="Cases", format=","),
                alt.Tooltip("deaths:Q", title="Deaths", format=","),
            ]
        )
        .transform_lookup(
            lookup="id",
            from_=alt.LookupData(
                data=latest_df,
                key="fips",
                fields=["county", "state", "cases", "deaths"],
            )
        )
        .transform_calculate(
            # Altair expects the geometry id field to be called "id"
            id="datum.id"
        )
        .properties(
            width="container",
            height="container",
            title=f"COVID‑19 Cases by US County (as of {latest_date.date()})",
        )
    )

    # Color encoding based on case count (log scale for better visual contrast)
    choropleth = base.encode(
        color=alt.Color(
            "cases:Q",
            scale=alt.Scale(scheme="reds", type="log"),
            legend=alt.Legend(title="Total Cases", format=","),
        )
    ).project(type="albersUsa")

    chart = choropleth

    # ------------------------------------------------------------------
    # Convert chart → HTML and return it
    # ------------------------------------------------------------------
    chart_html = chart.to_html()

    # Responsive wrapper – CSS forces the Vega‑Embed chart to fill its container
    html_template = Template(
        """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body { margin:0; padding:16px; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
                .chart-container { width:100%; height:700px; }
                .chart-container .vega-embed { width:100% !important; height:100% !important; }
            </style>
        </head>
        <body>
            <div class="chart-container">
                {{ chart_html | safe }}
            </div>
        </body>
        </html>
        """
    )

    rendered_html = html_template.render(chart_html=chart_html)

    return common.html_to_obj(rendered_html)