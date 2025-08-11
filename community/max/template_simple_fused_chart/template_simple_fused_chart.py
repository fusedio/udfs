@fused.udf
def udf(
    path: str = "s3://fused-sample/nyc_rolling_sales_manhattan_cleaned.parquet",
    sample_size: int = 500,   # rows sent to the browser – change if you need more
):
    """
    Load a small sample of the NYC rolling‑sales dataset and render a
    responsive Fused‑branded D3 scatter‑plot (Year Built vs. Sale Price).
    The chart will resize automatically to the container / screen size.
    Y-axis tick labels are rounded/abbreviated (k/M) and a chart title is added.
    """
    # -------------------------------------------------
    # 1️⃣ Load the HTML wrapper utility
    # -------------------------------------------------
    common = fused.load(
        "https://github.com/fusedio/udfs/tree/fbf5682/public/common/"
    )

    import pandas as pd
    import json

    # -------------------------------------------------
    # 2️⃣ Cached file read (runs once per path)
    # -------------------------------------------------
    @fused.cache
    def load_parquet(p):
        return pd.read_parquet(p)

    df = load_parquet(path)

    # -------------------------------------------------
    # 3️⃣ Prepare a tiny, clean dataset
    # -------------------------------------------------
    chart_df = (
        df[["YEAR_BUILT", "SALE_PRICE", "NEIGHBORHOOD", "ADDRESS"]]
        .dropna()
        .rename(columns={"YEAR_BUILT": "year", "SALE_PRICE": "price"})
    )

    # deterministic sample if the dataset is larger than `sample_size`
    if len(chart_df) > sample_size:
        chart_df = chart_df.sample(n=sample_size, random_state=42)

    # JSON string for safe embedding in JavaScript
    data_json = json.dumps(chart_df.to_dict(orient="records"))

    # -------------------------------------------------
    # 4️⃣ Responsive D3 scatter‑plot (Fused brand colors)
    # -------------------------------------------------
    # Note: all JS curly braces are doubled to be safe inside the Python f-string.
    html_content = f"""
<div id="chart" style="font-family:Arial; width:100%; height:50vh; min-height:240px; background:#333333; color:#E5FF44; padding:8px; box-sizing:border-box;"></div>

<script src="https://d3js.org/d3.v6.min.js"></script>
<script>
// ---- Data -------------------------------------------------
const data = {data_json};

// ---- Config -----------------------------------------------
const margin = {{top: 40, right: 30, bottom: 50, left: 70}}; // a bit larger left/top for label/title

// full-dollar formatter for tooltips
const priceFormat = d3.format("$,.0f");

// abbreviated formatter for y-axis (rounded: k / M)
function formatYAxisValue(d) {{
  const abs = Math.abs(d);
  if (abs >= 1e6) {{
    // show one decimal for millions, remove trailing .0
    let val = (d / 1e6).toFixed(1).replace(/\\.0$/, "");
    return "$" + val + "M";
  }} else if (abs >= 1e3) {{
    // round to nearest thousand (no decimals)
    let val = Math.round(d / 1e3);
    return "$" + val + "k";
  }} else {{
    return "$" + d3.format(",")(Math.round(d));
  }}
}}

// ---- Render function (redraws on resize) ------------------
function render() {{
  const container = document.getElementById("chart");
  const rect = container.getBoundingClientRect();

  // minimum sizes to keep axes readable
  const totalWidth = Math.max(300, Math.floor(rect.width));
  const totalHeight = Math.max(240, Math.floor(rect.height));

  const width = totalWidth - margin.left - margin.right;
  const height = totalHeight - margin.top - margin.bottom;

  // clear any previous svg (we keep other DOM nodes like tooltip, but remove svg/title)
  d3.select("#chart").selectAll("svg").remove();

  // create svg with responsive viewBox and 100% width
  const svgEl = d3.select("#chart")
    .append("svg")
      .attr("width", "100%")
      .attr("viewBox", "0 0 " + (totalWidth) + " " + (totalHeight))
      .style("background", "#333333");

  // add title (centered at the top)
  svgEl.append("text")
    .attr("x", totalWidth / 2)
    .attr("y", margin.top / 2)
    .attr("text-anchor", "middle")
    .attr("fill", "#E5FF44")
    .style("font-size", Math.max(12, Math.min(20, width / 18)) + "px")
    .style("font-weight", "600")
    .text("Manhattan Rolling Sales — Year Built vs Sale Price");

  // main group translated by margins
  const svg = svgEl.append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  // ---- Scales -------------------------------------------
  const x = d3.scaleLinear()
      .domain(d3.extent(data, d => d.year)).nice()
      .range([0, width]);

  const y = d3.scaleLinear()
      .domain(d3.extent(data, d => d.price)).nice()
      .range([height, 0]);

  // ---- Axes ---------------------------------------------
  const xAxis = d3.axisBottom(x).ticks(Math.max(4, Math.round(width/80))).tickFormat(d3.format("d"));
  const yAxis = d3.axisLeft(y).ticks(Math.max(4, Math.round(height/60))).tickFormat(formatYAxisValue);

  // X axis
  svg.append("g")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
    .call(g => g.selectAll("text").attr("fill", "#E5FF44"))
    .call(g => g.selectAll("path, line").attr("stroke", "#E5FF44"))
    .append("text")
      .attr("x", width/2)
      .attr("y", 40)
      .attr("fill", "#E5FF44")
      .attr("text-anchor", "middle")
      .text("Year Built");

  // Y axis
  svg.append("g")
      .call(yAxis)
    .call(g => g.selectAll("text").attr("fill", "#E5FF44"))
    .call(g => g.selectAll("path, line").attr("stroke", "#E5FF44"))
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("x", -height/2)
      .attr("y", -55)
      .attr("fill", "#E5FF44")
      .attr("text-anchor", "middle")
      .text("Sale Price (USD)");

  // ---- Points -------------------------------------------
  const points = svg.append("g")
      .attr("class", "points");

  points.selectAll("circle")
      .data(data)
      .enter()
      .append("circle")
        .attr("cx", d => x(d.year))
        .attr("cy", d => y(d.price))
        .attr("r", Math.max(2, Math.min(6, width / 200)))  // scale radius lightly with width
        .attr("fill", "#E5FF44")
        .attr("opacity", 0.9)
        .attr("stroke", "#333333");

  // ---- Tooltip (simple) -------------------------------
  d3.select("#chart").selectAll(".tooltip").remove();

  const tooltip = d3.select("#chart")
    .append("div")
      .attr("class", "tooltip")
      .style("position", "absolute")
      .style("pointer-events", "none")
      .style("background", "#222")
      .style("color", "#E5FF44")
      .style("padding", "6px 8px")
      .style("border-radius", "4px")
      .style("font-size", "12px")
      .style("display", "none");

  points.selectAll("circle")
    .on("mousemove", function(event, d) {{
      tooltip
        .style("left", (event.pageX + 10) + "px")
        .style("top", (event.pageY + 10) + "px")
        .style("display", "block")
        .html("<strong>" + (d.year || "") + "</strong><br/>" + (d.ADDRESS || d.ADDRESS === "" ? d.ADDRESS : "") + "<br/>" + priceFormat(d.price));
    }})
    .on("mouseout", function() {{
      tooltip.style("display", "none");
    }});
}}

// ---- Initial render + resize handling --------------------
render();

// Re-render when window resizes
window.addEventListener("resize", function() {{
  window.requestAnimationFrame(render);
}});

// Also use a ResizeObserver if available for container-specific changes
if (typeof ResizeObserver !== "undefined") {{
  try {{
    const ro = new ResizeObserver(entries => {{
      window.requestAnimationFrame(render);
    }});
    const el = document.getElementById("chart");
    if (el) ro.observe(el);
  }} catch (e) {{
    // ignore and rely on window resize
  }}
}}
</script>
"""

    # Return the HTML object that the Workbench can render
    return common.html_to_obj(html_content)