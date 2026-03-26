@fused.udf(cache_max_age=0)
def udf(
    bounds: fused.types.Bounds = [-125, 32, -114, 42],
    res: int = 4,
    month: str = '2024-05',
):
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")
    import pandas as pd
    import altair as alt
    alt.data_transformers.enable('default')

    # Load joined elevation + temperature data
    parent_udf = fused.load('temp_elev_corr')
    df = parent_udf(bounds=bounds, res=res, month=month)

    print(f"{df.T=}")

    # Compute Pearson correlation
    corr_val = df['monthly_mean_temp'].corr(df['elevation_avg'])
    print(f"Pearson r = {corr_val:.4f}")

    # Stratified sample for chart (bin by elevation, sample within each bin)
    df['elev_bin'] = pd.cut(df['elevation_avg'], bins=50)
    df_sample = df.groupby('elev_bin', observed=True).apply(
        lambda g: g.sample(min(len(g), 80), random_state=42)
    ).reset_index(drop=True).drop(columns='elev_bin')
    print(f"Sampled {len(df_sample)} rows from {len(df)} for chart")

    # Scatter plot: Temperature vs Elevation
    scatter = (
        alt.Chart(df_sample)
        .mark_circle(size=40, opacity=0.5)
        .encode(
            x=alt.X('elevation_avg:Q', title='Elevation (m)'),
            y=alt.Y('monthly_mean_temp:Q', title='Temperature (&deg;K)'),
            tooltip=[
                alt.Tooltip('elevation_avg:Q', title='Elevation', format='.1f'),
                alt.Tooltip('monthly_mean_temp:Q', title='Temp (K)', format='.2f'),
            ]
        )
    )

    # Add regression line
    regression = scatter.transform_regression(
        'elevation_avg', 'monthly_mean_temp'
    ).mark_line(color='red', strokeWidth=2)

    chart = (
        (scatter + regression)
        .properties(
            width="container",
            height=450,
            title=f'Temperature vs Elevation (Pearson r = {corr_val:.3f})'
        )
        .configure(background='white')
    )

    return chart.to_html(embed_options={"renderer": "svg"})
