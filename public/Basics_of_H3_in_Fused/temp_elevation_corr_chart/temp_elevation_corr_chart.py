@fused.udf(cache_max_age=0)
def udf(
    bounds: fused.types.Bounds = [-125, 32, -114, 42],
    res: int = 4,
    month: str = '2024-05',
):
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")
    import pandas as pd
    import json

    # Load joined elevation + temperature data
    parent_udf = fused.load('temp_elev_corr')
    df = parent_udf(bounds=bounds, res=res, month=month)

    # Compute Pearson correlation
    corr_val = df['monthly_mean_temp'].corr(df['elevation_avg'])

    # Stratified sample for chart (bin by elevation, sample within each bin)
    df['elev_bin'] = pd.cut(df['elevation_avg'], bins=50)
    df_sample = df.groupby('elev_bin', observed=True).apply(
        lambda g: g.sample(min(len(g), 80), random_state=42)
    ).reset_index(drop=True).drop(columns='elev_bin')

    # Prepare data points for JavaScript
    chart_data = df_sample[['elevation_avg', 'monthly_mean_temp']].rename(
        columns={'elevation_avg': 'x', 'monthly_mean_temp': 'y'}
    ).to_dict(orient='records')

    # Calculate basic linear regression parameters on the sample for the trendline
    x_mean = df_sample['elevation_avg'].mean()
    y_mean = df_sample['monthly_mean_temp'].mean()
    num = ((df_sample['elevation_avg'] - x_mean) * (df_sample['monthly_mean_temp'] - y_mean)).sum()
    den = ((df_sample['elevation_avg'] - x_mean) ** 2).sum()
    slope = num / den if den != 0 else 0
    intercept = y_mean - slope * x_mean

    min_x = float(df_sample['elevation_avg'].min())
    max_x = float(df_sample['elevation_avg'].max())
    trendline_data = [
        {"x": min_x, "y": slope * min_x + intercept},
        {"x": max_x, "y": slope * max_x + intercept}
    ]

    # Generate visual client-side Chart.js HTML
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                margin: 20px;
                background-color: #ffffff;
            }}
            .chart-container {{
                width: 100%;
                max-width: 800px;
                height: 450px;
                margin: auto;
            }}
            h3 {{
                text-align: center;
                color: #333;
            }}
        </style>
    </head>
    <body>
        <h3>Temperature vs Elevation (Pearson r = {corr_val:.3f})</h3>
        <div class="chart-container">
            <canvas id="scatterChart"></canvas>
        </div>
        <script>
            const ctx = document.getElementById('scatterChart').getContext('2d');
            new Chart(ctx, {{
                type: 'scatter',
                data: {{
                    datasets: [{{
                        label: 'Data Points',
                        data: {json.dumps(chart_data)},
                        backgroundColor: 'rgba(54, 162, 235, 0.5)',
                        pointRadius: 4
                    }}, {{
                        label: 'Regression Line',
                        data: {json.dumps(trendline_data)},
                        type: 'line',
                        borderColor: 'red',
                        borderWidth: 2,
                        fill: false,
                        pointRadius: 0
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        x: {{
                            title: {{
                                display: true,
                                text: 'Elevation (m)'
                            }}
                        }},
                        y: {{
                            title: {{
                                display: true,
                                text: 'Temperature (&deg;C)'
                            }}
                        }}
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """
    return html_content