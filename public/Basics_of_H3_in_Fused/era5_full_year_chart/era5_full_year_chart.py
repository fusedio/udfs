@fused.udf
def udf(bounds: fused.types.Bounds = [-125, 32, -114, 42]):
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")
    import altair as alt
    alt.data_transformers.enable('default')

    full_year_udf = fused.load('era5_full_year')
    df = full_year_udf(bounds=bounds, cache_max_age='0s')

    print(df.head())
    print(f"{df.shape=}")

    chart = (
        alt.Chart(df)
        .mark_line(color='steelblue', strokeWidth=2)
        .encode(
            x=alt.X('date:T', title='Date'),
            y=alt.Y('daily_avg_temp:Q', title='Avg Temperature (\u00b0C)', scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip('date:T', title='Date'),
                alt.Tooltip('daily_avg_temp:Q', title='Temp (\u00b0C)', format='.2f'),
            ]
        )
        .properties(
            width='container',
            height=350,
            title='Daily Average Temperature \u2014 2019\u20132024'
        )
        .configure(background='white')
    )

    return chart.to_html(embed_options={"renderer": "svg"})
