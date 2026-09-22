@fused.udf
def udf():
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
    df = df.dropna(subset=['county'])

    # Prepare data for Chart.js
    import json
    
    # Sort by month to ensure line order
    df = df.sort_values('month')
    months = sorted(df['month'].unique().tolist())
    
    # Generate colors for each unique county
    colors = {
        'San Juan, WA': 'rgba(31, 119, 180, 1)',
        'Santa Cruz, AZ': 'rgba(255, 127, 14, 1)',
        'Decatur, IA': 'rgba(44, 160, 44, 1)',
        'Macon, AL': 'rgba(214, 39, 40, 1)',
        'Knox, ME': 'rgba(148, 103, 189, 1)'
    }
    
    datasets = []
    for county, group in df.groupby('county'):
        # Map monthly values, fill missing with None
        month_to_val = dict(zip(group['month'], group['mean_ndvi']))
        data = [month_to_val.get(m, None) for m in months]
        
        color = colors.get(county, 'rgba(128, 128, 128, 1)')
        datasets.append({
            "label": county,
            "data": data,
            "borderColor": color,
            "backgroundColor": color.replace(', 1)', ', 0.1)'),
            "borderWidth": 2.5,
            "tension": 0.1,
            "spanGaps": True
        })

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
                height: 400px;
                width: 100%;
                max-width: 800px;
                margin: 0 auto;
            }}
            h2 {{
                text-align: center;
                color: #333;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <h2>NDVI Time Series — All AOIs</h2>
        <div class="chart-container">
            <canvas id="ndviChart"></canvas>
        </div>
        <script>
            const ctx = document.getElementById('ndviChart').getContext('2d');
            new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: {json.dumps(months)},
                    datasets: {json.dumps(datasets)}
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        x: {{
                            title: {{
                                display: true,
                                text: 'Month',
                                color: '#333'
                            }},
                            grid: {{
                                display: false
                            }}
                        }},
                        y: {{
                            min: 0,
                            max: 1,
                            title: {{
                                display: true,
                                text: 'NDVI',
                                color: '#333'
                            }}
                        }}
                    }},
                    plugins: {{
                        legend: {{
                            position: 'top',
                            labels: {{
                                font: {{
                                    size: 13
                                }}
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