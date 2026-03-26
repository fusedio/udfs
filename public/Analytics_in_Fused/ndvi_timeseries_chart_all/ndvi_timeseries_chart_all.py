@fused.udf
def udf():
    import altair as alt
    alt.data_transformers.enable('default')

    # Load timeseries data
    timeseries_udf = fused.load('ndvi_yearly_timeseries_all_aois')
    df = timeseries_udf() # Just loading default values from above UDF
    df = df.reset_index(drop=True)

    # Drop months with no data
    df = df.dropna(subset=['mean_ndvi'])

    # Create a readable county label for each AOI
    county_names = {
        '53_055': 'San Juan, WA',
        '04_023': 'Santa Cruz, AZ',
        '19_053': 'Decatur, IA',
        '01_087': 'Macon, AL',
        '23_013': 'Knox, ME',
    }
    df['county'] = (df['state_fips'].astype(str) + '_' + df['county_fips'].astype(str)).map(county_names)
    print(df)

    # Build line chart with separate lines per AOI (mean only)
    color_scale = alt.Color('county:N', title='County',
        scale=alt.Scale(scheme='tableau10'))

    base = alt.Chart(df).encode(
        x=alt.X('month:O', title='Month', axis=alt.Axis(labelAngle=0)),
    )

    line = base.mark_line(strokeWidth=2.5).encode(
        y=alt.Y('mean_ndvi:Q', title='NDVI', scale=alt.Scale(domain=[0, 1])),
        color=color_scale,
        tooltip=[
            alt.Tooltip('county:N', title='County'),
            alt.Tooltip('month:O', title='Month'),
            alt.Tooltip('mean_ndvi:Q', title='Mean NDVI', format='.3f'),
        ]
    )

    points = base.mark_circle(size=50).encode(
        y='mean_ndvi:Q',
        color=color_scale,
        tooltip=[
            alt.Tooltip('county:N', title='County'),
            alt.Tooltip('month:O', title='Month'),
            alt.Tooltip('mean_ndvi:Q', title='Mean NDVI', format='.3f'),
        ]
    )

    chart = (line + points).properties(
        width='container',
        height=400,
        title='NDVI Time Series — All AOIs',
        background='white',
    ).configure_legend(
        titleFontSize=14,
        labelFontSize=13,
        symbolSize=150,
        symbolStrokeWidth=3,
    ).configure_axis(
        labelColor='#333',
        titleColor='#333',
    ).configure_title(
        color='#333',
    ).configure_view(
        strokeWidth=0,
    )

    return chart.to_html(embed_options={"renderer": "svg"})