@fused.udf
def udf(month='2024-05', bounds: fused.types.Bounds=[-125, 32, -114, 42]):
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")
    import altair as alt
    alt.data_transformers.enable('default')

    parent_udf = fused.load('era5_temp_monthly_average')
    df = parent_udf(month=month, bounds=bounds)

    print(df.head())

    chart = (
        alt.Chart(df)
        .mark_line(point=True, color='steelblue', strokeWidth=2)
        .encode(
            x=alt.X('date:T', title='Date'),
            y=alt.Y('daily_avg_temp:Q', title='Avg Temperature (\u00b0C)', scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip('date:T', title='Date'),
                alt.Tooltip('daily_avg_temp:Q', title='Temp (°C)', format='.2f'),
            ]
        )
        .properties(
            width='container',
            height=300,
            title=f'Daily Average Temperature — {month}'
        )
        .configure(background='white')
    )

    return chart.to_html(embed_options={"renderer": "svg"})