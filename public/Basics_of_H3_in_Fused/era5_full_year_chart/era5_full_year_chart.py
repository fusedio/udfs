@fused.udf
def udf(bounds: fused.types.Bounds = [-125, 32, -114, 42]):
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")
    import json

    full_year_udf = fused.load('era5_full_year')
    df = full_year_udf(bounds=bounds, cache_max_age='0s')

    print(df.head())
    print(f"{df.shape=}")

    # Prepare data for Chart.js
    dates = df['date'].astype(str).tolist()
    temps = df['daily_avg_temp'].round(2).tolist()

    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body style="margin:0; padding:10px; font-family: sans-serif; background-color: white;">
      <div style="width: 100%; height: 350px;">
        <canvas id="tempChart"></canvas>
      </div>
      <script>
        const ctx = document.getElementById('tempChart').getContext('2d');
        new Chart(ctx, {
          type: 'line',
          data: {
            labels: JSON_DATES,
            datasets: [{
              label: 'Avg Temperature (&deg;C)',
              data: JSON_TEMPS,
              borderColor: 'steelblue',
              borderWidth: 2,
              fill: false,
              pointRadius: 1
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              title: {
                display: true,
                text: 'Daily Average Temperature &mdash; 2019&ndash;2024'
              }
            },
            scales: {
              x: {
                title: {
                  display: true,
                  text: 'Date'
                }
              },
              y: {
                title: {
                  display: true,
                  text: 'Temperature (&deg;C)'
                }
              }
            }
          }
        });
      </script>
    </body>
    </html>
    """

    html_content = html_template.replace("JSON_DATES", json.dumps(dates)).replace("JSON_TEMPS", json.dumps(temps))
    return html_content