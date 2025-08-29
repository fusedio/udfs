@fused.udf
def udf(url: str = "https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/refs/heads/main/data/Google%20Mobility%20-%20City%20-%20Daily.csv"):
    
    import fused
    import pandas as pd
    from jinja2 import Template

    # Load utilities for returning HTML objects
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    @fused.cache
    def load_data(url):
        return pd.read_csv(url, encoding="utf-8-sig")

    df = load_data(url)

    # Build a proper datetime column from year, month, day
    df["date"] = pd.to_datetime(df[["year", "month", "day"]])

    # Keep only a single city for a clear line chart
    city_id = df["cityid"].iloc[0]
    city_df = df[df["cityid"] == city_id].copy()
    city_df.sort_values("date", inplace=True)

    # Choose a mobility metric to plot
    metric_col = "gps_retail_and_recreation"

    # Prepare data for D3
    chart_data = city_df[["date", metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records")
    for row in chart_data:
        row["date"] = row["date"].strftime("%Y-%m-%d")

    html_template = Template("""
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width,initial-scale=1" />
          <title>Google Mobility – City {{ city_id }}</title>
          <script src="https://d3js.org/d3.v7.min.js"></script>
          <style>
            html, body { 
              height: 100%; 
              margin: 0; 
              font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; 
              background: #f5f7fa; 
            }
            
            .container { 
              height: 100vh; 
              width: 100vw; 
              padding: 16px; 
              box-sizing: border-box;
              display: flex;
              flex-direction: column;
            }
            
            .title { 
              margin: 0 0 16px 0; 
              font-size: 18px; 
              font-weight: 600; 
              color: #333;
              flex-shrink: 0;
            }
            
            #chart { 
              flex: 1;
              min-height: 300px;
              min-width: 300px;
              background: white;
              border-radius: 8px;
              box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            
            .tooltip { 
              position: absolute; 
              padding: 8px; 
              background: rgba(0,0,0,0.8); 
              color: white;
              border-radius: 4px; 
              pointer-events: none; 
              font-size: 12px; 
              opacity: 0; 
              transition: opacity 0.2s; 
            }
          </style>
        </head>
        <body>
          <div class="container">
            <h1 class="title">Google Mobility – City {{ city_id }}</h1>
            <div id="chart"></div>
          </div>

          <script>
            const data = {{ chart_data | safe }};

            function drawChart() {
              // Clear previous chart
              d3.select("#chart").selectAll("*").remove();

              // Get actual container dimensions with fallbacks
              const container = document.getElementById('chart');
              const containerRect = container.getBoundingClientRect();
              
              // Use viewport dimensions as fallback if container is collapsed
              const containerWidth = Math.max(containerRect.width, 400);
              const containerHeight = Math.max(containerRect.height, 300);
              
              const margin = {top: 20, right: 30, bottom: 60, left: 70};
              const width = containerWidth - margin.left - margin.right;
              const height = containerHeight - margin.top - margin.bottom;
              
              // Ensure minimum dimensions
              if (width < 200 || height < 150) {
                console.log('Dimensions too small, skipping render');
                return;
              }

              // Create SVG with viewBox for true responsiveness
              const svg = d3.select("#chart")
                .append("svg")
                .attr("width", "100%")
                .attr("height", "100%")
                .attr("viewBox", `0 0 ${containerWidth} ${containerHeight}`)
                .attr("preserveAspectRatio", "xMidYMid meet");

              const g = svg.append("g")
                .attr("transform", `translate(${margin.left}, ${margin.top})`);

              // Parse and prepare data
              const parseDate = d3.timeParse("%Y-%m-%d");
              const processedData = data.map(d => ({
                date: parseDate(d.date),
                value: +d.value
              }));

              // Scales
              const x = d3.scaleTime()
                .domain(d3.extent(processedData, d => d.date))
                .range([0, width]);

              const y = d3.scaleLinear()
                .domain(d3.extent(processedData, d => d.value))
                .nice()
                .range([height, 0]);

              // Axes
              g.append("g")
                .attr("transform", `translate(0, ${height})`)
                .call(d3.axisBottom(x).tickFormat(d3.timeFormat("%b %Y")));

              g.append("g")
                .call(d3.axisLeft(y));

              // Axis labels
              svg.append("text")
                .attr("x", containerWidth / 2)
                .attr("y", containerHeight - 10)
                .style("text-anchor", "middle")
                .style("font-size", "12px")
                .text("Date");

              svg.append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 20)
                .attr("x", -containerHeight / 2)
                .style("text-anchor", "middle")
                .style("font-size", "12px")
                .text("Retail & Recreation Change");

              // Line generator
              const line = d3.line()
                .x(d => x(d.date))
                .y(d => y(d.value))
                .curve(d3.curveMonotoneX);

              // Draw line
              g.append("path")
                .datum(processedData)
                .attr("fill", "none")
                .attr("stroke", "#3b82f6")
                .attr("stroke-width", 2)
                .attr("d", line);

              // Tooltip
              const tooltip = d3.select("body").append("div")
                .attr("class", "tooltip");

              // Points for interaction
              g.selectAll(".dot")
                .data(processedData.filter((d, i) => i % 7 === 0)) // Show every 7th point to avoid clutter
                .enter().append("circle")
                .attr("class", "dot")
                .attr("cx", d => x(d.date))
                .attr("cy", d => y(d.value))
                .attr("r", 4)
                .attr("fill", "#3b82f6")
                .attr("stroke", "white")
                .attr("stroke-width", 2)
                .style("cursor", "pointer")
                .on("mouseover", function(event, d) {
                  d3.select(this).attr("r", 6);
                  tooltip
                    .style("opacity", 1)
                    .html(`<strong>Date:</strong> ${d3.timeFormat("%Y-%m-%d")(d.date)}<br/><strong>Value:</strong> ${d.value.toFixed(1)}`)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 10) + "px");
                })
                .on("mouseout", function() {
                  d3.select(this).attr("r", 4);
                  tooltip.style("opacity", 0);
                });
            }

            // Debounced resize function
            let resizeTimer;
            function handleResize() {
              clearTimeout(resizeTimer);
              resizeTimer = setTimeout(() => {
                drawChart();
              }, 150); // Debounce resize events
            }

            // Initial render after DOM is ready
            document.addEventListener('DOMContentLoaded', drawChart);
            
            // Handle window resize
            window.addEventListener('resize', handleResize);

            // Fallback: render after short delay if DOM already loaded
            if (document.readyState === 'complete') {
              setTimeout(drawChart, 100);
            }
          </script>
        </body>
        </html>
    """)

    rendered = html_template.render(
        city_id=city_id,
        chart_data=chart_data
    )
    return common.html_to_obj(rendered)