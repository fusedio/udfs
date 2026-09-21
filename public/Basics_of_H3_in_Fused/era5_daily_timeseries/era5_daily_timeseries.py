@fused.udf
def udf(month='2024-05', bounds: fused.types.Bounds=[-125, 32, -114, 42]):
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")

    parent_udf = fused.load('era5_temp_monthly_average')
    df = parent_udf(month=month, bounds=bounds)

    # Convert Timestamp objects to string to ensure JSON serializability
    df['date'] = df['date'].astype(str)

    # Convert dataframe to records for JS consumption
    data_points = df[['date', 'daily_avg_temp']].to_dict(orient='records')
    import json
    data_json = json.dumps(data_points)

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
                position: relative;
                height: 350px;
                width: 100%;
                max-width: 800px;
                margin: auto;
            }}
            h2 {{
                text-align: center;
                color: #333;
                font-size: 1.2rem;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <h2>Daily Average Temperature &mdash; {month}</h2>
        <div class="chart-container">
            <canvas id="tempChart"></canvas>
        </div>
        <script>
            const rawData = {data_json};
            const labels = rawData.map(d => d.date.split(' ')[0]);
            const temps = rawData.map(d => d.daily_avg_temp);

            const ctx = document.getElementById('tempChart').getContext('2d');
            new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: labels,
                    datasets: [{{
                        label: 'Avg Temperature (&deg;C)',
                        data: temps,
                        borderColor: '#4682b4',
                        backgroundColor: 'rgba(70, 130, 180, 0.1)',
                        borderWidth: 2,
                        pointRadius: 3,
                        pointBackgroundColor: '#4682b4',
                        tension: 0.1,
                        fill: true
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    return context.parsed.y.toFixed(2) + ' &deg;C';
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{
                            grid: {{
                                display: false
                            }},
                            title: {{
                                display: true,
                                text: 'Date'
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