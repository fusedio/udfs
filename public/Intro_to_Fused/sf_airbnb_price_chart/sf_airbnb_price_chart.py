@fused.udf(cache_max_age=0)
def udf():
    import altair as alt
    import pandas as pd

    # This loads the previous UDF and calls it
    parent_udf = fused.load('sf_airbnb_listings')
    df = parent_udf(cache_max_age=0)
    print(df.T)

    plot_df = df.dropna(subset=['price_in_dollar', 'number_of_reviews'])
    plot_df = plot_df[plot_df['price_in_dollar'] <= 1000]
    plot_df = plot_df[plot_df['number_of_reviews'] > 0]

    plot_df = plot_df.sample(n=min(4999, len(plot_df)), random_state=42)

    chart = alt.Chart(plot_df).mark_circle(size=200, opacity=0.9).encode(
        x=alt.X('number_of_reviews:Q', title='Number of Reviews'),
        y=alt.Y('price_in_dollar:Q', title='Price ($)'),
        color=alt.Color('room_type:N', title='Room Type'),
        tooltip=['name:N', 'neighbourhood_cleansed:N', 'room_type:N', 'price_in_dollar:Q', 'number_of_reviews:Q']
    ).properties(
        title='SF Airbnb: Price vs Number of Reviews',
        width='container',
        height=700,
        background='white'
    ).configure_title(
        fontSize=28,
        fontWeight='bold'
    ).configure_axis(
        titleFontSize=18,
        labelFontSize=14
    ).configure_legend(
        titleFontSize=16,
        labelFontSize=14
    ).to_html(embed_options={"renderer": "svg"})

    return chart