@fused.udf
def udf(
    bounds: fused.types.Bounds=[-74.556, 40.4, -73.374, 41.029], 
    res: int=8
):
    import altair as alt
    # Load data
    zonal_stats = fused.run('county_pop_housing', bounds=bounds, res=res)
    print(zonal_stats.T)
    # Prepare data for chart
    df_plot = zonal_stats[['county', 'avg_elevation']].copy()
    # Ensure county is string for axis
    df_plot['county'] = df_plot['county'].astype(str)
    chart = alt.Chart(df_plot).mark_line(color='blue').encode(
        x=alt.X('county:N', sort='-y', title='County'),
        y=alt.Y('avg_elevation:Q', title='Avg Elevation'),
        tooltip=['county', 'avg_elevation']
    ).properties(
        width=800,
        height=400,
        title='Average Elevation per County TEST'
    ) 
    # Return as HTML 
    return chart.to_html()