@fused.udf
def udf():
    # Load HTML wrapper utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    import pandas as pd
    import json

    # -------------------------------------------------
    # Load and cache the egg‑price data (cached once per URL)
    # -------------------------------------------------
    @fused.cache
    def load_egg_prices(_force_reload: int = 0):
        # FRED: Average Price: Eggs, Grade A, Large (APU0000708111)
        url = (
            "https://fred.stlouisfed.org/graph/fredgraph.csv?"
            "id=APU0000708111&cosd=1980-01-01&coed=2025-06-01"
        )
        df = pd.read_csv(
            url,
            header=0,
            usecols=[0, 1],
            names=["date", "price"],
            parse_dates=["date"],
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = (
            df.dropna(subset=["date", "price"])
            .sort_values("date")
            .reset_index(drop=True)
        )
        return df

    # -------------------------------------------------
    # Prepare the data
    # -------------------------------------------------
    df = load_egg_prices()
    df = df.sort_values("date").reset_index(drop=True)

    # 6‑month moving average for smoother trend
    df["price_ma"] = df["price"].rolling(window=6, min_periods=1).mean()

    # Serialize to JSON for D3 (use ISO date string for reliable parsing)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    data_json = json.dumps(df[["date", "price", "price_ma"]].to_dict(orient="records"))

    # -------------------------------------------------
    # Responsive, interactive D3 line chart
    # -------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>Egg Prices Chart</title>
<style>
:root {{
  --bg: #0b0f19;
  --panel: #111827;
  --grid: #1f2937;
  --axis: #9ca3af;
  --text: #e5e7eb;
  --accent: #7dd3fc;
  --accent-2: #34d399;
  --muted: #6b7280;
}}
body {{
  margin: 0;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system,
    Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans,
    Helvetica Neue, Arial, "Apple Color Emoji",
    "Segoe UI Emoji", "Segoe UI Symbol";
  color: var(--text);
  background: var(--bg);
}}
#chart-wrap {{
  width: 100%;
  max-width: 1100px;
  margin: 0 auto;
  padding: 16px 20px 8px 20px;
  box-sizing: border-box;
}}
.card {{
  background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 12px;
  padding: 12px;
}}
.header {{
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 8px;
  padding: 4px 6px 10px 6px;
}}
.title {{
  font-weight: 600;
  font-size: 16px;
  letter-spacing: 0.2px;
}}
.subtitle {{
  color: var(--muted);
  font-size: 12px;
}}
.controls {{
  display: flex;
  gap: 10px;
  align-items: center;
  color: var(--muted);
  font-size: 12px;
}}
.toggle {{
  display: inline-flex;
  gap: 6px;
  align-items: center;
  cursor: pointer;
  user-select: none;
}}
.toggle input {{
  accent-color: var(--accent);
}}
.chart {{
  width: 100%;
}}
.tooltip {{
  position: absolute;
  pointer-events: none;
  background: rgba(17,24,39,0.92);
  color: var(--text);
  border: 1px solid rgba(255,255,255,0.12);
  border-radius: 8px;
  padding: 8px 10px;
  font-size: 12px;
  line-height: 1.35;
  box-shadow: 0 10px 30px rgba(0,0,0,0.35);
}}
.legend {{
  display: flex;
  gap: 14px;
  align-items: center;
  padding: 6px 6px 0 6px;
  color: var(--muted);
  font-size: 12px;
}}
.legend .item {{
  display: inline-flex;
  align-items: center;
  gap: 6px;
  cursor: pointer;
  user-select: none;
}}
.legend .swatch {{
  width: 14px;
  height: 3px;
  border-radius: 2px;
  background: var(--muted);
}}
.legend .price .swatch {{ background: var(--accent); }}
.legend .ma .swatch {{ background: var(--accent-2); }}
.grid line {{ stroke: var(--grid); stroke-width: 1; }}
.axis path, .axis line {{ stroke: var(--axis); stroke-opacity: 0.6; }}
.axis text {{ fill: var(--axis); font-size: 11px; }}
.note {{ color: var(--muted); font-size: 11px; padding: 6px 6px 2px 6px; }}
</style>
</head>
<body>
<div id="chart-wrap">
  <div class="card">
    <div class="header">
      <div>
        <div class="title">Average Price of Eggs (Grade A, Large)</div>
        <div class="subtitle">United States, Jan 1980 &ndash; Jun 2025</div>
      </div>
      <div class="controls">
        <label class="toggle"><input id="toggle-ma" type="checkbox" checked /> 6&nbsp;mo moving average</label>
        <label class="toggle"><input id="toggle-pts" type="checkbox" /> Show markers</label>
      </div>
    </div>
    <div class="legend">
      <div class="item price" data-series="price"><span class="swatch"></span> Price</div>
      <div class="item ma" data-series="ma"><span class="swatch"></span> 6&nbsp;mo avg</div>
    </div>
    <div id="chart" class="chart"></div>
    <div class="note">Hover to see values. Drag to pan, scroll to zoom. Double‑click to reset.</div>
  </div>
</div>
<div id="tooltip" class="tooltip" style="display:none;"></div>

<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<script>
// Data injected from Python
const data = {data_json};

// Parse dates and coerce numbers
const parseDate = d3.utcParse("%Y-%m-%d");
data.forEach(d => {{
  d.date = parseDate(d.date);
  d.price = +d.price;
  d.price_ma = +d.price_ma;
}});

// Chart state
const state = {{
  showMA: true,
  showPts: false
}};

// DOM references
const wrap = document.getElementById("chart-wrap");
const container = document.getElementById("chart");
const tooltip = document.getElementById("tooltip");
const chkMA = document.getElementById("toggle-ma");
const chkPts = document.getElementById("toggle-pts");

// Formatters
const priceFmt = d3.format("$,.2f");
const dateFmt = d3.utcFormat("%b %Y");

// Event listeners
chkMA.addEventListener("change", () => {{
  state.showMA = chkMA.checked;
  render();
}});
chkPts.addEventListener("change", () => {{
  state.showPts = chkPts.checked;
  render();
}});

// Responsive rendering with ResizeObserver
const ro = new ResizeObserver(() => render());
ro.observe(container);

// Scales that can change with zoom
let currentXScale = null;

// Render function
let svg, xScale, yScale, xAxisG, yAxisG, gridBottomG, gridLeftG, pricePath, maPath;

function render() {{
  // Clear previous chart
  container.innerHTML = "";

  // Compute responsive size with fallbacks
  const fallbackW = wrap.getBoundingClientRect().width || 800;
  const width = container.clientWidth || fallbackW;
  const height = Math.max(280, Math.min(0.62 * width, 520));
  const margins = {{ top: 20, right: 28, bottom: 38, left: 52 }};
  const innerW = Math.max(10, width - margins.left - margins.right);
  const innerH = Math.max(10, height - margins.top - margins.bottom);

  // Scales
  xScale = d3.scaleUtc()
    .domain(d3.extent(data, d => d.date))
    .range([0, innerW]);

  const yMin = d3.min(data, d => Math.min(d.price, d.price_ma));
  const yMax = d3.max(data, d => Math.max(d.price, d.price_ma));
  yScale = d3.scaleLinear()
    .domain([Math.floor(yMin * 0.95 * 10) / 10, Math.ceil(yMax * 1.05 * 10) / 10])
    .nice()
    .range([innerH, 0]);

  currentXScale = xScale; // reset on re-render

  // SVG container
  svg = d3.select(container).append("svg")
    .attr("viewBox", `0 0 ${{width}} ${{height}}`)
    .attr("width", "100%")
    .attr("height", height)
    .style("display", "block");

  const g = svg.append("g")
    .attr("transform", `translate(${{margins.left}},${{margins.top}})`);

  // Grid
  gridBottomG = g.append("g").attr("class", "grid")
    .attr("transform", `translate(0,${{innerH}})`)
    .call(d3.axisBottom(xScale).ticks(width < 600 ? 6 : 10).tickSize(-innerH).tickFormat(""))
    .call(s => s.selectAll(".domain").remove());
  gridLeftG = g.append("g").attr("class", "grid")
    .call(d3.axisLeft(yScale).ticks(6).tickSize(-innerW).tickFormat(""))
    .call(s => s.selectAll(".domain").remove());

  // Axes
  xAxisG = g.append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${{innerH}})`)
    .call(d3.axisBottom(xScale).ticks(width < 600 ? 6 : 10));
  yAxisG = g.append("g")
    .attr("class", "axis")
    .call(d3.axisLeft(yScale).ticks(6).tickFormat(d3.format("$,.2f")));

  // Clip path
  const clipId = "clip-" + Math.random().toString(36).slice(2);
  g.append("clipPath")
    .attr("id", clipId)
    .append("rect")
    .attr("width", innerW)
    .attr("height", innerH);

  const plot = g.append("g")
    .attr("clip-path", `url(#${{clipId}})`);

  // Line generators
  const linePrice = d3.line()
    .x(d => xScale(d.date))
    .y(d => yScale(d.price))
    .curve(d3.curveMonotoneX);
  const lineMA = d3.line()
    .x(d => xScale(d.date))
    .y(d => yScale(d.price_ma))
    .curve(d3.curveMonotoneX);

  // Price line
  pricePath = plot.append("path")
    .datum(data)
    .attr("fill", "none")
    .attr("stroke", "var(--accent)")
    .attr("stroke-width", 2)
    .attr("d", linePrice);

  // Moving average line
  maPath = plot.append("path")
    .datum(data)
    .attr("fill", "none")
    .attr("stroke", "var(--accent-2)")
    .attr("stroke-width", 2)
    .attr("opacity", state.showMA ? 1 : 0)
    .attr("d", lineMA);

  // Optional point markers
  if (state.showPts) {{
    plot.selectAll(".pt")
      .data(data)
      .join("circle")
      .attr("class", "pt")
      .attr("r", 2.2)
      .attr("cx", d => xScale(d.date))
      .attr("cy", d => yScale(d.price))
      .attr("fill", "var(--accent)")
      .attr("opacity", 0.85);
  }}

  // Hover interaction
  const bisect = d3.bisector(d => d.date).center;
  const hover = plot.append("g");
  const crosshair = hover.append("line")
    .attr("stroke", "rgba(255,255,255,0.45)")
    .attr("stroke-width", 1)
    .attr("y1", 0)
    .attr("y2", innerH)
    .style("display", "none");
  const focus = hover.append("circle")
    .attr("r", 3.5)
    .attr("fill", "var(--accent)")
    .attr("stroke", "white")
    .attr("stroke-width", 1.2)
    .style("display", "none");

  const overlay = plot.append("rect")
    .attr("width", innerW)
    .attr("height", innerH)
    .attr("fill", "transparent")
    .style("cursor", "crosshair")
    .on("mousemove", onMove)
    .on("mouseleave", onLeave);

  function onMove(event) {{
    const [mx] = d3.pointer(event);
    const x0 = currentXScale.invert(mx);
    const i = Math.max(0, Math.min(data.length - 1, bisect(data, x0)));
    const d = data[i];

    crosshair
      .attr("x1", currentXScale(d.date))
      .attr("x2", currentXScale(d.date))
      .style("display", null);
    focus
      .attr("cx", currentXScale(d.date))
      .attr("cy", yScale(d.price))
      .style("display", null);

    tooltip.style.display = "block";
    tooltip.innerHTML = `
      <div><strong>${{dateFmt(d.date)}}</strong></div>
      <div>Price: <strong>${{priceFmt(d.price)}}</strong></div>
      ${{ state.showMA ? `<div>6&nbsp;mo avg: <strong>${{priceFmt(d.price_ma)}}</strong></div>` : "" }}
    `;

    const ttRect = tooltip.getBoundingClientRect();
    const wrapRect = wrap.getBoundingClientRect();
    const pageX = wrapRect.left + margins.left + currentXScale(d.date);
    const pageY = wrapRect.top + margins.top + yScale(d.price);

    let left = pageX + 12;
    let top = pageY - ttRect.height - 10;
    if (left + ttRect.width > window.innerWidth - 10) left = pageX - ttRect.width - 12;
    if (top < 10) top = pageY + 12;

    tooltip.style.left = `${{left}}px`;
    tooltip.style.top = `${{top}}px`;
  }}

  function onLeave() {{
    crosshair.style("display", "none");
    focus.style("display", "none");
    tooltip.style.display = "none";
  }}

  // Zoom & pan
  const zoom = d3.zoom()
    .scaleExtent([1, 20])
    .translateExtent([[0, 0], [innerW, innerH]])
    .extent([[0, 0], [innerW, innerH]])
    .on("zoom", (event) => {{
      const zx = event.transform.rescaleX(xScale);
      currentXScale = zx;

      // Update axes and grid
      xAxisG.call(d3.axisBottom(zx).ticks(width < 600 ? 6 : 10));
      gridBottomG.call(d3.axisBottom(zx).ticks(width < 600 ? 6 : 10).tickSize(-innerH).tickFormat(""))
        .call(s => s.selectAll(".domain").remove());

      // Update lines under zoom
      const linePriceZ = d3.line()
        .x(d => zx(d.date))
        .y(d => yScale(d.price))
        .curve(d3.curveMonotoneX);
      const lineMAZ = d3.line()
        .x(d => zx(d.date))
        .y(d => yScale(d.price_ma))
        .curve(d3.curveMonotoneX);

      pricePath.attr("d", linePriceZ(data));
      maPath.attr("d", lineMAZ(data));

      // Update markers
      plot.selectAll(".pt").attr("cx", d => zx(d.date));
    }});

  svg.call(zoom).on("dblclick.zoom", null);
  svg.on("dblclick", () => {{
    svg.transition().duration(250).call(zoom.transform, d3.zoomIdentity);
  }});

  // Legend toggles
  d3.selectAll(".legend .item").on("click", function() {{
    const series = this.getAttribute("data-series");
    if (series === "ma") {{
      state.showMA = !state.showMA;
      chkMA.checked = state.showMA;
      maPath.attr("opacity", state.showMA ? 1 : 0);
    }} else {{
      state.showPts = !state.showPts;
      chkPts.checked = state.showPts;
      render();
    }}
  }});
}}

render();
</script>
</body>
</html>
"""
    return common.html_to_obj(html_content)