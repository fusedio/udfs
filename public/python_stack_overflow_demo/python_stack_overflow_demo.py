@fused.udf
def udf():
    import pandas as pd
    import zipfile
    import requests
    import io
    import json
    
    def get_python_stats(year):
        """Get Python usage statistics for a given year"""
        try:
            url = f"https://survey.stackoverflow.co/datasets/stack-overflow-developer-survey-{year}.zip"
            response = requests.get(url)
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                # Try different possible filenames
                possible_files = ['survey_results_public.csv', 'survey_results.csv']
                for filename in possible_files:
                    try:
                        with zip_file.open(filename) as file:
                            df = pd.read_csv(file)
                        break
                    except KeyError:
                        continue
                
            total_responses = len(df)
            
            # Try different column names for language data
            language_cols = ['LanguageHaveWorkedWith', 'LanguageWorkedWith', 'HaveWorkedLanguage']
            language_col = None
            for col in language_cols:
                if col in df.columns:
                    language_col = col
                    break
            
            if language_col is None:
                return None
                
            python_users = df[language_col].str.contains('Python', na=False).sum()
            python_percentage = (python_users / total_responses) * 100
            
            return {
                'year': year,
                'total_responses': total_responses,
                'python_users': python_users,
                'python_percentage': python_percentage
            }
        except Exception as e:
            print(f"Error processing {year}: {e}")
            return None
    
    # Get data for last 5 years
    years = [2020, 2021, 2022, 2023, 2024]
    stats = []
    
    for year in years:
        year_stats = get_python_stats(year)
        if year_stats:
            stats.append(year_stats)
    
    # Create evolution dataframe
    evolution_df = pd.DataFrame(stats)
    
    # Prepare data for visualization
    chart_data = evolution_df.to_dict('records')
    
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Python Popularity Evolution</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 40px 20px;
            }
            
            .container {
                max-width: 1000px;
                margin: 0 auto;
            }
            
            .header {
                text-align: center;
                margin-bottom: 50px;
                color: white;
            }
            
            .header h1 {
                font-size: 2.5rem;
                font-weight: 300;
                margin-bottom: 10px;
                letter-spacing: -0.5px;
            }
            
            .header p {
                font-size: 1.2rem;
                opacity: 0.9;
                font-weight: 300;
            }
            
            .chart-card {
                background: rgba(255, 255, 255, 0.95);
                border-radius: 20px;
                padding: 40px;
                box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
                backdrop-filter: blur(10px);
                transition: transform 0.3s ease;
                max-width: 800px;
                margin: 0 auto;
            }
            
            .chart-card:hover {
                transform: translateY(-5px);
            }
            
            .chart-title {
                font-size: 1.5rem;
                font-weight: 300;
                color: #333;
                margin-bottom: 30px;
                text-align: center;
            }
            
            .chart-container {
                position: relative;
                height: 500px;
            }
            
            @media (max-width: 767px) {
                body {
                    padding: 20px 10px;
                }
                
                .chart-card {
                    padding: 30px 20px;
                }
                
                .header h1 {
                    font-size: 2rem;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Python Popularity Evolution</h1>
                <p>Stack Overflow Developer Survey 2020-2024</p>
            </div>
            
            <div class="chart-card">
                <h2 class="chart-title">Python Usage Trends</h2>
                <div class="chart-container">
                    <canvas id="combinedChart"></canvas>
                </div>
            </div>
        </div>

        <script>
            const data = """ + json.dumps(chart_data) + """;
            
            // Format numbers
            function formatNumber(num) {
                return num.toLocaleString();
            }
            
            // Combined Chart
            const ctx = document.getElementById('combinedChart').getContext('2d');
            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.map(d => d.year),
                    datasets: [
                        {
                            label: 'Python Usage %',
                            data: data.map(d => d.python_percentage),
                            borderColor: '#3776ab',
                            backgroundColor: 'rgba(55, 118, 171, 0.1)',
                            borderWidth: 3,
                            fill: false,
                            tension: 0.4,
                            pointBackgroundColor: '#3776ab',
                            pointBorderColor: '#fff',
                            pointBorderWidth: 2,
                            pointRadius: 5,
                            pointHoverRadius: 7,
                            yAxisID: 'y'
                        },
                        {
                            label: 'Total Python Users',
                            data: data.map(d => d.python_users),
                            borderColor: '#ff6b6b',
                            backgroundColor: 'rgba(255, 107, 107, 0.1)',
                            borderWidth: 3,
                            fill: false,
                            tension: 0.4,
                            pointBackgroundColor: '#ff6b6b',
                            pointBorderColor: '#fff',
                            pointBorderWidth: 2,
                            pointRadius: 5,
                            pointHoverRadius: 7,
                            yAxisID: 'y1'
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        mode: 'index',
                        intersect: false,
                    },
                    plugins: {
                        legend: {
                            position: 'top',
                            labels: {
                                usePointStyle: true,
                                padding: 20,
                                font: {
                                    size: 14
                                }
                            }
                        },
                        tooltip: {
                            callbacks: {
                                label: function(context) {
                                    let label = context.dataset.label || '';
                                    if (label) {
                                        label += ': ';
                                    }
                                    if (context.datasetIndex === 0) {
                                        label += context.parsed.y.toFixed(1) + '%';
                                    } else {
                                        label += formatNumber(context.parsed.y);
                                    }
                                    return label;
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            display: true,
                            title: {
                                display: true,
                                text: 'Year',
                                font: {
                                    size: 14
                                }
                            },
                            grid: {
                                display: false
                            }
                        },
                        y: {
                            type: 'linear',
                            display: true,
                            position: 'left',
                            title: {
                                display: true,
                                text: 'Usage Percentage (%)',
                                font: {
                                    size: 14
                                }
                            },
                            beginAtZero: true,
                            grid: {
                                color: 'rgba(0, 0, 0, 0.05)'
                            },
                            ticks: {
                                callback: function(value) {
                                    return value.toFixed(1) + '%';
                                },
                                font: {
                                    size: 12
                                }
                            }
                        },
                        y1: {
                            type: 'linear',
                            display: true,
                            position: 'right',
                            title: {
                                display: true,
                                text: 'Total Users',
                                font: {
                                    size: 14
                                }
                            },
                            beginAtZero: true,
                            grid: {
                                drawOnChartArea: false,
                            },
                            ticks: {
                                callback: function(value) {
                                    return formatNumber(value);
                                },
                                font: {
                                    size: 12
                                }
                            }
                        }
                    }
                }
            });
        </script>
    </body>
    </html>
    """
    
    return common.html_to_obj(html_content)