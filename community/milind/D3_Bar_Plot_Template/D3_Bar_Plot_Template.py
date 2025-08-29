@fused.udf
def udf():
    import pandas as pd, requests, json
    from jinja2 import Template

    # Load common utilities for returning HTML objects
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    @fused.cache
    def fetch_starred_repos():
        url = "https://api.github.com/search/repositories?q=stars:>10000&sort=stars&order=desc&per_page=100"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        items = resp.json().get("items", [])
        # Keep only a few useful columns
        return pd.DataFrame([{
            "name": r["full_name"],
            "stars": r["stargazers_count"],
            "forks": r["forks_count"],
            "language": r["language"],
            "url": r["html_url"]
        } for r in items])

    # Get the data
    df = fetch_starred_repos()

    # Prepare data for the bar chart – top 20 repos by stars
    df_top = df.nlargest(20, "stars").sort_values("stars", ascending=False).reset_index(drop=True)
    data_json = df_top[["name", "stars"]].to_dict(orient="records")
    data_json_str = json.dumps(data_json)  # safe injection into the template

    # Jinja2 HTML template with a responsive D3 horizontal bar chart
    html_template = Template("""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Top GitHub Repos by Stars</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    html,body{height:100%;margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f5f7fa}
    .wrap{box-sizing:border-box;height:100%;display:flex;align-items:center;justify-content:center}
    .card{width:100%;max-width:1200px;height:100%;background:#fff;border-radius:12px;box-shadow:0 6px 25px rgba(0,0,0,0.08);overflow:hidden;display:flex;flex-direction:column}
    .title{padding:20px 20px 0 20px;margin:0;font-size:18px;font-weight:600;color:#333}
    #my_dataviz{flex:1;min-height:0}
    .tooltip{position:absolute;padding:8px;background:rgba(0,0,0,0.8);color:#fff;border-radius:4px;pointer-events:none;font-size:12px;opacity:0;transition:opacity 0.2s}
    .axis text{font-size:11px}
    .axis-title{font-size:12px;font-weight:500}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1 class="title">Top GitHub Repositories by Stars</h1>
      <div id="my_dataviz"></div>
    </div>
  </div>

  <script>
    const data = {{ data_json_str | safe }};

    function drawChart() {
      d3.select("#my_dataviz").selectAll("*").remove();

      const container = document.getElementById('my_dataviz');
      const rect = container.getBoundingClientRect();
      const margin = {top: 20, right: 30, bottom: 60, left: 200};
      const width = Math.max(rect.width - margin.left - margin.right, 300);
      const height = Math.max(rect.height - margin.top - margin.bottom, 300);
      if (width <= 0 || height <= 0) return;

      const svg = d3.select("#my_dataviz")
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      // Y scale (repo names) – horizontal bars
      const y = d3.scaleBand()
        .domain(data.map(d => d.name))
        .range([0, height])
        .padding(0.1);

      // X scale (stars)
      const x = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.stars)])
        .nice()
        .range([0, width]);

      // Y axis (repo names)
      svg.append("g")
        .attr("class", "axis")
        .call(d3.axisLeft(y));

      // X axis (stars)
      svg.append("g")
        .attr("class", "axis")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.axisBottom(x).ticks(5));

      // Tooltip
      const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip");

      // Bars (horizontal)
      svg.selectAll(".bar")
        .data(data)
        .enter()
        .append("rect")
        .attr("class", "bar")
        .attr("y", d => y(d.name))
        .attr("x", 0)
        .attr("height", y.bandwidth())
        .attr("width", d => x(d.stars))
        .attr("fill", "#69b3a2")
        .on("mouseover", function(event, d) {
          d3.select(this).attr("fill", "#40a3a2");
          tooltip.transition().duration(200).style("opacity", .9);
          tooltip.html("<strong>" + d.name + "</strong><br/>Stars: " + d.stars)
            .style("left", (event.pageX + 10) + "px")
            .style("top", (event.pageY - 28) + "px");
        })
        .on("mouseout", function() {
          d3.select(this).attr("fill", "#69b3a2");
          tooltip.transition().duration(500).style("opacity", 0);
        });

      // X axis label
      svg.append("text")
        .attr("class", "axis-title")
        .attr("x", width / 2)
        .attr("y", height + margin.bottom - 10)
        .style("text-anchor", "middle")
        .text("Stars");

      // Y axis label
      svg.append("text")
        .attr("class", "axis-title")
        .attr("transform", "rotate(-90)")
        .attr("y", -margin.left + 20)
        .attr("x", -height / 2)
        .style("text-anchor", "middle")
        .text("Repository");
    }

    // Initial render
    drawChart();

    // Redraw on resize (debounced)
    let resizeTimer;
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(drawChart, 150);
    });
  </script>
</body>
</html>""")

    rendered = html_template.render(data_json_str=data_json_str)
    return common.html_to_obj(rendered)