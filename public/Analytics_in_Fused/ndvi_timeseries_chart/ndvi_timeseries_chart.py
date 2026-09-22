@fused.udf
def udf():
    # Load timeseries data (cached from previous runs)
    timeseries_udf = fused.load('ndvi_yearly_timeseries')
    df = timeseries_udf() # Just loading default values from above UDF
    df = df.reset_index(drop=True)

    # Drop months with no data
    df = df.dropna(subset=['mean_ndvi'])
    
    # Convert data for the chart
    months = df['month'].tolist()
    ndvi_values = df['mean_ndvi'].tolist()

    # Generate HTML with Chart.js to replace Altair
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                background-color: white;
                margin: 0;
                padding: 16px;
            }}
            .chart-container {{
                position: relative;
                width: 100%;
                height: 400px;
            }}
        </style>
    </head>
    <body>
        <div class="chart-container">
            <canvas id="ndviChart"></canvas>
        </div>
        <script>
            const ctx = document.getElementById('ndviChart').getContext('2d');
            new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: {months},
                    datasets: [{{
                        label: 'Mean NDVI',
                        data: {ndvi_values},
                        borderColor: 'green',
                        backgroundColor: 'rgba(0, 128, 0, 0.1)',
                        borderWidth: 2.5,
                        pointBackgroundColor: 'green',
                        pointRadius: 4,
                        tension: 0.1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        title: {{
                            display: true,
                            text: 'NDVI Time Series',
                            font: {{
                                size: 16,
                                weight: 'bold'
                            }},
                            color: '#333'
                        }},
                        legend: {{
                            display: false
                        }}
                    }},
                    scales: {{
                        y: {{
                            min: 0,
                            max: 1,
                            title: {{
                                display: true,
                                text: 'NDVI',
                                color: '#333'
                            }},
                            ticks: {{
                                color: '#333'
                            }},
                            grid: {{
                                color: '#f0f0f0'
                            }}
                        }},
                        x: {{
                            title: {{
                                display: true,
                                text: 'Month',
                                color: '#333'
                            }},
                            ticks: {{
                                color: '#333'
                            }},
                            grid: {{
                                display: false
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