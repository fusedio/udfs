@fused.udf
def udf():
    import json
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
    
    # Prepare data for Javascript ingestion
    chart_data = df[['county', 'month', 'coverage_pct', 'valid_pixels', 'total_pixels']].to_dict(orient='records')
    chart_data_json = json.dumps(chart_data)

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Pixel Coverage Chart</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                margin: 20px;
                background-color: #ffffff;
                color: #333333;
            }}
            .chart-container {{
                position: relative;
                height: 400px;
                width: 100%;
                max-width: 800px;
                margin: auto;
            }}
            h2 {{
                text-align: center;
                font-size: 18px;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <h2>Pixel Coverage % — All AOIs (valid / total pixels)</h2>
        <div class="chart-container">
            <canvas id="coverageChart"></canvas>
        </div>
        <script>
            const rawData = {chart_data_json};
            
            // Extract distinct counties and months
            const counties = [...new Set(rawData.map(d => d.county).filter(Boolean))];
            const months = [...new Set(rawData.map(d => d.month))].sort((a, b) => a - b);
            
            const colors = [
                'rgba(75, 192, 192, 1)',
                'rgba(255, 99, 132, 1)',
                'rgba(54, 162, 235, 1)',
                'rgba(255, 206, 86, 1)',
                'rgba(153, 102, 255, 1)'
            ];

            const datasets = counties.map((county, index) => {{
                const countyData = months.map(m => {{
                    const point = rawData.find(d => d.county === county && d.month === m);
                    return point ? point.coverage_pct : null;
                }});
                
                return {{
                    label: county,
                    data: countyData,
                    borderColor: colors[index % colors.length],
                    backgroundColor: colors[index % colors.length],
                    borderWidth: 2.5,
                    tension: 0.1,
                    pointRadius: 4
                }};
            }});

            const ctx = document.getElementById('coverageChart').getContext('2d');
            new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: months,
                    datasets: datasets
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        y: {{
                            min: 0,
                            max: 100,
                            title: {{
                                display: true,
                                text: 'Coverage %'
                            }}
                        }},
                        x: {{
                            title: {{
                                display: true,
                                text: 'Month'
                            }}
                        }}
                    }},
                    plugins: {{
                        legend: {{
                            position: 'top',
                        }}
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """
    return html_content