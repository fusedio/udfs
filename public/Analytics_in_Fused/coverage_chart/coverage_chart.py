@fused.udf(cache_max_age=0)
def udf():
    import altair as alt
    alt.data_transformers.enable('default')

    # Load timeseries data (same source as NDVI chart)
    timeseries_udf = fused.load('ndvi_yearly_timeseries_all_aois')
    df = timeseries_udf()
    df = df.reset_index(drop=True)
    df = df.dropna(subset=['mean_ndvi'])

    # Create readable county labels
    county_names = {
        '53_055': 'San Juan, WA',
        '04_023': 'Santa Cruz, AZ',
        '19_053': 'Decatur, IA',
        '01_087': 'Macon, AL',
        '23_013': 'Knox, ME',
    }
    df['county'] = (df['state_fips'].astype(str) + '_' + df['county_fips'].astype(str)).map(county_names)

    # Compute coverage percentage
    df['coverage_pct'] = (df['valid_pixels'] / df['total_pixels']) * 100
    print(df[['county', 'month', 'valid_pixels', 'total_pixels', 'coverage_pct']])

    # Build line chart matching NDVI chart style
    color_scale = alt.Color('county:N', title='County',
        scale=alt.Scale(scheme='tableau10'))

    base = alt.Chart(df).encode(
        x=alt.X('month:O', title='Month', axis=alt.Axis(labelAngle=0)),
    )

    line = base.mark_line(strokeWidth=2.5).encode(
        y=alt.Y('coverage_pct:Q', title='Coverage %', scale=alt.Scale(domain=[0, 100])),
        color=color_scale,
        tooltip=[
            alt.Tooltip('county:N', title='County'),
            alt.Tooltip('month:O', title='Month'),
            alt.Tooltip('coverage_pct:Q', title='Coverage %', format='.1f'),
            alt.Tooltip('valid_pixels:Q', title='Valid Pixels', format=','),
            alt.Tooltip('total_pixels:Q', title='Total Pixels', format=','),
        ]
    )

    points = base.mark_circle(size=50).encode(
        y='coverage_pct:Q',
        color=color_scale,
        tooltip=[
            alt.Tooltip('county:N', title='County'),
            alt.Tooltip('month:O', title='Month'),
            alt.Tooltip('coverage_pct:Q', title='Coverage %', format='.1f'),
            alt.Tooltip('valid_pixels:Q', title='Valid Pixels', format=','),
            alt.Tooltip('total_pixels:Q', title='Total Pixels', format=','),
        ]
    )

    # Add a reference line at 50% coverage
    rule = alt.Chart({'values': [{'y': 50}]}).mark_rule(
        strokeDash=[4, 4], color='red', opacity=0.5
    ).encode(y='y:Q')

    chart = (line + points + rule).properties(
        width='container',
        height=400,
        title='Pixel Coverage % — All AOIs (valid / total pixels)',
        background='white',
    ).configure_legend(
        titleFontSize=14,
        labelFontSize=13,
        symbolSize=150,
        symbolStrokeWidth=3,
    )

    return chart.to_html(embed_options={"renderer": "svg"})
