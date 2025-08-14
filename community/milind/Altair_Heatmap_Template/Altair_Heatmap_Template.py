@fused.udf
def udf():
    import pandas as pd
    import altair as alt
    import numpy as np
    from sklearn.datasets import load_wine
    
    # Load the wine dataset
    wine = load_wine()
    df = pd.DataFrame(wine.data, columns=wine.feature_names)
    df['target'] = wine.target
    
    # Add target names for clarity
    target_names = {0: 'class_0', 1: 'class_1', 2: 'class_2'}
    df['target_name'] = df['target'].map({i: name for i, name in enumerate(wine.target_names)})
    
    # REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    # ALTAIR REQUIREMENT: Enable processing of larger datasets
    alt.data_transformers.enable("default", max_rows=None)
    
    # Calculate correlation matrix
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    correlation_matrix = df[numeric_cols].corr()
    
    # Convert correlation matrix to long format for Altair
    corr_data = correlation_matrix.reset_index().melt(id_vars='index')
    corr_data = corr_data.rename(columns={'index': 'variable1', 'variable': 'variable2', 'value': 'correlation'})
    
    # Create the heatmap
    chart = alt.Chart(corr_data).mark_rect().encode(
        x=alt.X('variable1:N', title=None, sort=None),
        y=alt.Y('variable2:N', title=None, sort=None),
        color=alt.Color('correlation:Q', 
                       scale=alt.Scale(scheme='redblue', domain=[-1, 1]),
                       title='Correlation'),
        tooltip=[
            alt.Tooltip('variable1:N', title='Variable 1'),
            alt.Tooltip('variable2:N', title='Variable 2'),
            alt.Tooltip('correlation:Q', title='Correlation', format='.3f')
        ]
    ).properties(
        width='container',
        height='container',
        title=alt.TitleParams(text='Wine Dataset: Feature Correlation Heatmap', anchor='start', fontSize=16)
    )
    
    # Add text labels for correlation values
    text = alt.Chart(corr_data).mark_text(baseline='middle', fontSize=9).encode(
        x=alt.X('variable1:N', sort=None),
        y=alt.Y('variable2:N', sort=None),
        text=alt.Text('correlation:Q', format='.2f'),
        color=alt.condition(
            alt.datum.correlation > 0.5,
            alt.value('white'),
            alt.value('black')
        )
    )
    
    # Combine heatmap and text
    final_chart = chart + text
    
    # Generate HTML
    chart_html = final_chart.to_html()
    
    # Create responsive HTML wrapper
    html_content = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width,initial-scale=1" />
      <title>Wine Dataset Correlation Heatmap</title>
      <style>
        :root {{ --padding: 16px; --bg: #f5f7fa; --card-bg: #ffffff; --radius: 12px; }}
        html,body {{ height:100%; margin:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:var(--bg); }}
        .wrap {{ box-sizing:border-box; padding:var(--padding); height:100%; display:flex; align-items:center; justify-content:center; }}
        .card {{ width:100%; max-width:1200px; height:calc(100% - 2*var(--padding)); background:var(--card-bg); border-radius:var(--radius); box-shadow:0 6px 25px rgba(0,0,0,0.08); overflow:hidden; }}
        .card .vega-embed, .card .vega-embed > .vega-visualization {{ width:100% !important; height:100% !important; }}
        @media (max-width:640px) {{ :root {{ --padding:8px; }} .card {{ border-radius:8px; }} }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="card">
          {chart_html}
        </div>
      </div>
    </body>
    </html>
    """
    
    return common.html_to_obj(html_content)