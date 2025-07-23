@fused.udf
def udf():
    import pandas as pd
    import altair as alt
    import duckdb
    
    common = fused.load("https://github.com/fusedio/udfs/tree/7918aff/public/common/").utils
    
    files = [
        f"s3://fused-sample/demo_data/ERA5/climate_data/{year:04d}-{month:02d}.pq"
        for year in range(2005, 2025)
        for month in range(1, 13)
    ]
    files = [f for f in files if (int(f.split('/')[-1].split('-')[0]) < 2024 or int(f.split('/')[-1].split('-')[1].split('.')[0]) <= 8)]
    
    conn = duckdb.connect()
    
    query = """
    SELECT 
        DATE_TRUNC('month', CAST(datestr AS DATE)) as month,
        AVG(daily_mean_temp - 273.15) as avg_temp_celsius
    FROM read_parquet([{}])
    GROUP BY DATE_TRUNC('month', CAST(datestr AS DATE))
    ORDER BY month
    """.format(','.join([f"'{f}'" for f in files]))
    
    df = conn.execute(query).df()
    conn.close()
    
    df['month_str'] = pd.to_datetime(df['month']).dt.strftime('%Y-%m')
    
    chart = alt.Chart(df).mark_line(
        color='#1f77b4',
        strokeWidth=2
    ).encode(
        x=alt.X('month:T', 
                title='Month',
                axis=alt.Axis(format='%Y-%m')),
        y=alt.Y('avg_temp_celsius:Q', 
                title='Average Temperature (°C)',
                scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip('month_str:N', title='Month'),
            alt.Tooltip('avg_temp_celsius:Q', title='Avg Temp (°C)', format='.1f')
        ]
    ).properties(
        title='Global Average Monthly Temperature (ERA5 Data)',
        width=800,
        height=400
    ).interactive()
    
    chart_html = chart.to_html()
    
    dashboard_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Temperature Dashboard</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
            }}
            .dashboard-container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                padding: 30px;
            }}
            h1 {{
                color: #333;
                margin-bottom: 10px;
            }}
            .subtitle {{
                color: #666;
                margin-bottom: 30px;
            }}
            .subtitle a {{
                color: #1f77b4;
                text-decoration: none;
                font-weight: 500;
            }}
            .subtitle a:hover {{
                text-decoration: underline;
            }}
            .chart-container {{
                display: flex;
                justify-content: center;
            }}
            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin-top: 30px;
            }}
            .stat-card {{
                background: #f8f9fa;
                padding: 20px;
                border-radius: 6px;
                text-align: center;
            }}
            .stat-value {{
                font-size: 24px;
                font-weight: bold;
            }}
            .stat-label {{
                color: #666;
                margin-top: 5px;
            }}
            .average-temp {{
                color: black;
            }}
            .warmest-month {{
                color: #d62728;
            }}
            .coldest-month {{
                color: #1f77b4;
            }}
        </style>
    </head>
    <body>
        <div class="dashboard-container">
            <h1>Worldwide Global Temperature</h1>
            <p class="subtitle"><a href="https://www.fused.io/workbench/files?path=s3%3A%2F%2Ffused-sample%2Fdemo_data%2FERA5%2Fclimate_data%2F" target="_blank">Based on ERA 5 Weather data</a></p>
            
            <div class="chart-container">
                {chart_html}
            </div>
            
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-value average-temp">{df['avg_temp_celsius'].mean():.1f}°C</div>
                    <div class="stat-label">Average Temperature</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value coldest-month">{df['avg_temp_celsius'].min():.1f}°C</div>
                    <div class="stat-label">Coldest Month</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value warmest-month">{df['avg_temp_celsius'].max():.1f}°C</div>
                    <div class="stat-label">Warmest Month</div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    return common.html_to_obj(dashboard_html)