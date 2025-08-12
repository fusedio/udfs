@fused.udf
def udf():
    # Load the common helper for returning HTML
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    import pandas as pd
    import altair as alt

    # Force Altair to use the default data transformer and remove the 5k row limit.
    alt.data_transformers.enable("default", max_rows=None)

    # Cache the data-loading step (fast subsequent runs)
    @fused.cache
    def load_data():
        # Palmer Penguins dataset
        url = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        return pd.read_csv(url)

    # Load the dataframe
    df = load_data()

    # Create a histogram of penguin bill lengths
    chart_html = (
        alt.Chart(df)
        
        .mark_bar()
        .encode(
            x=alt.X("bill_length_mm:Q", bin=alt.Bin(maxbins=30), title="Bill Length (mm)"),
            y=alt.Y("count()", title="Count")
        )
        .properties(
            width="container",
            height="container",
            title="Distribution of Penguin Bill Lengths"
        )
        .configure_view(
            strokeWidth=0
        )
        .to_html()
    )

    # Create a fully responsive container that fills viewport
    responsive_html = f"""
    <style>
        html, body {{
            margin: 0;
            padding: 0;
            height: 100%;
            width: 100%;
            overflow: hidden;
        }}
        .chart-container {{
            width: 100vw;
            height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .vega-embed {{
            width: 100%;
            height: 100%;
        }}
        .vega-embed > .vega-visualization {{
            width: 100% !important;
            height: 100% !important;
        }}
    </style>
    <div class="chart-container">
        {chart_html}
    </div>
    """

    # Return the chart as an HTML object that the Workbench can render
    return common.html_to_obj(responsive_html)