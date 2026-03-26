@fused.udf
def udf():
    import altair as alt
    alt.data_transformers.enable('default')

    # Load timeseries data (cached from previous runs)
    timeseries_udf = fused.load('ndvi_yearly_timeseries')
    df = timeseries_udf() # Just loading default values from above UDF
    df = df.reset_index(drop=True)

    # Drop months with no data
    df = df.dropna(subset=['mean_ndvi'])
    print(df)

    # Build line chart with mean NDVI only
    base = alt.Chart(df).encode(
        x=alt.X('month:O', title='Month', axis=alt.Axis(labelAngle=0)),
    )

    line = base.mark_line(color='green', strokeWidth=2.5).encode(
        y=alt.Y('mean_ndvi:Q', title='NDVI', scale=alt.Scale(domain=[0, 1])),
        tooltip=[
            alt.Tooltip('month:O', title='Month'),
            alt.Tooltip('mean_ndvi:Q', title='Mean NDVI', format='.3f'),
        ]
    )

    points = base.mark_circle(color='green', size=50).encode(
        y='mean_ndvi:Q',
        tooltip=[
            alt.Tooltip('month:O', title='Month'),
            alt.Tooltip('mean_ndvi:Q', title='Mean NDVI', format='.3f'),
        ]
    )

    chart = (line + points).properties(
        width='container',
        height=400,
        title=f'NDVI Time Series',
        background='white',
    ).configure_axis(
        labelColor='#333',
        titleColor='#333',
    ).configure_title(
        color='#333',
    ).configure_view(
        strokeWidth=0,
    )

    return chart.to_html(embed_options={"renderer": "svg"})