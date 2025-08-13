@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Scatter Plot with Brushing Selection
    # TEMPLATE: Minimal, clean interactive scatter template for LLM-generated charts
    # DATA REQUIREMENTS: 2 numeric columns (X, Y), optional 1 categorical column for color
    # FEATURES: 2D brushing selection, hover tooltips, color-coded categories, responsive design
    # =============================================================================

    import pandas as pd
    from jinja2 import Template

    # Load common utilities - REQUIRED for HTML UDFs
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    # -------------------------------------------------------------------------
    # DATA LOADING SECTION
    # Replace this with your own data loading logic if needed
    # -------------------------------------------------------------------------
    @fused.cache
    def load_data():
        """
        Must return a pandas DataFrame with at least two numeric columns.
        Example replacements:
          - return pd.read_csv("your_file.csv")
          - return pd.read_parquet("s3://bucket/data.parquet")
        """
        return pd.read_csv(
            "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        ).dropna(subset=["bill_length_mm", "bill_depth_mm"])

    df = load_data()

    # -------------------------------------------------------------------------
    # DATA PREPARATION SECTION  - update column names to match your dataset
    # -------------------------------------------------------------------------
    chart_data = df[["bill_length_mm", "bill_depth_mm", "species"]]  # [x_col, y_col, category_col]
    data_json = chart_data.to_json(orient="records")

    # -------------------------------------------------------------------------
    # CONFIGURATION SECTION - update for your dataset and styling
    # -------------------------------------------------------------------------
    config = {
        "xField": "bill_length_mm",
        "yField": "bill_depth_mm",
        "categoryField": "species",
        "pointRadius": 4,
        "pointRadiusHover": 6,
        "xAxisLabel": "Bill Length (mm)",
        "yAxisLabel": "Bill Depth (mm)",
        "chartTitle": "Penguin Bill Measurements by Species",
        "colorPalette": ["#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"],
        "showGrid": True,
        "enableBrushing": True,
        "transitionDuration": 200,
        "margin": {"top": 70, "right": 140, "bottom": 60, "left": 70},  # increased right margin for legend
    }

    theme = {
        "bg_color": "#1a1a1a",
        "text_color": "#ffffff",
        "primary_color": "#E8FF59",
        "secondary_color": "#ff7f0e",
        "accent_color": "#2ca02c",
        "grid_color": "#333333",
        "grid_light": "#666666",
        "tooltip_bg": "rgba(0, 0, 0, 0.9)",
        "brush_stroke": "#ffffff",
        "brush_fill": "rgba(255, 255, 255, 0.1)",
        "font_family": "-apple-system, BlinkMacSystemFont, sans-serif",
    }

    # -------------------------------------------------------------------------
    # JINJA2 TEMPLATE
    # -------------------------------------------------------------------------
    html_template = Template("""
<!DOCTYPE html>
<html>
<head>
    <title>{{ config.chartTitle }}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        :root {
            --bg-color: {{ theme.bg_color }};
            --text-color: {{ theme.text_color }};
            --primary-color: {{ theme.primary_color }};
            --secondary-color: {{ theme.secondary_color }};
            --accent-color: {{ theme.accent_color }};
            --grid-color: {{ theme.grid_color }};
            --grid-light: {{ theme.grid_light }};
            --tooltip-bg: {{ theme.tooltip_bg }};
            --brush-stroke: {{ theme.brush_stroke }};
            --brush-fill: {{ theme.brush_fill }};
            --font-family: {{ theme.font_family }};
        }

        body {
            margin: 0;
            background: var(--bg-color);
            color: var(--text-color);
            font-family: var(--font-family);
            height: 100vh;
            overflow: hidden;
        }

        svg { width: 100%; height: 100%; }

        .dot {
            cursor: pointer;
            transition: all 0.2s ease;
            opacity: 0.7;
        }

        .dot.faded {
            opacity: 0.1 !important;
            stroke: none !important;
        }

        .dot.highlighted {
            opacity: 1 !important;
            stroke: var(--text-color) !important;
            stroke-width: 2px !important;
        }

        .axis { color: var(--text-color); }
        .axis text { fill: var(--text-color); font-size: 11px; }
        .axis path, .axis line { stroke: var(--grid-light); }

        .grid-line {
            stroke: var(--grid-color);
            stroke-dasharray: 2,2;
        }

        .axis-label {
            fill: var(--text-color);
            font-size: 13px;
            font-weight: 500;
            text-anchor: middle;
        }

        .title {
            fill: var(--text-color);
            font-size: 16px;
            font-weight: bold;
            text-anchor: middle;
        }

        .brush .extent {
            stroke: var(--brush-stroke);
            fill: var(--brush-fill);
            shape-rendering: crispEdges;
        }

        .tooltip {
            position: absolute;
            padding: 8px 12px;
            background: var(--tooltip-bg);
            color: var(--text-color);
            border-radius: 4px;
            pointer-events: none;
            font-size: 12px;
            border: 1px solid var(--grid-color);
            max-width: 200px;
            opacity: 0;
            transition: opacity 0.2s ease;
        }

        .selection-display {
            fill: var(--text-color);
            font-size: 12px;
            text-anchor: middle;
            opacity: 0;
            transition: opacity 0.2s ease;
        }

        .selection-display.visible {
            opacity: 1;
        }

        /* Legend styles */
        .legend {
            fill: var(--text-color);
            font-size: 12px;
        }
        .legend-item { cursor: pointer; }
        .legend-rect {
            stroke: rgba(255,255,255,0.08);
            stroke-width: 1px;
            rx: 2px;
        }
        .legend-text { fill: var(--text-color); font-size: 12px; }
    </style>
</head>
<body>
    <svg></svg>
    <div class="tooltip"></div>

    <script>
        const data = {{ data_json | safe }};
        const CONFIG = {{ config | tojson }};

        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");

        function draw() {
            const svgWidth = svg.node().clientWidth;
            const svgHeight = svg.node().clientHeight;
            const width = svgWidth - CONFIG.margin.left - CONFIG.margin.right;
            const height = svgHeight - CONFIG.margin.top - CONFIG.margin.bottom;

            svg.selectAll("*").remove();

            const g = svg.append("g")
                .attr("transform", `translate(${CONFIG.margin.left},${CONFIG.margin.top})`);

            svg.append("text")
                .attr("class", "title")
                .attr("x", svgWidth / 2)
                .attr("y", 25)
                .text(CONFIG.chartTitle);

            const selectionDisplay = svg.append("text")
                .attr("class", "selection-display")
                .attr("x", svgWidth / 2)
                .attr("y", 45);

            const xScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d[CONFIG.xField]))
                .range([0, width])
                .nice();

            const yScale = d3.scaleLinear()
                .domain(d3.extent(data, d => d[CONFIG.yField]))
                .range([height, 0])
                .nice();

            const colorScale = d3.scaleOrdinal()
                .domain([...new Set(data.map(d => d[CONFIG.categoryField]))])
                .range(CONFIG.colorPalette);

            const xAxis = g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${height})`)
                .call(d3.axisBottom(xScale).tickSize(CONFIG.showGrid ? -height : 0).tickSizeOuter(0));

            const yAxis = g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(yScale).tickSize(CONFIG.showGrid ? -width : 0).tickSizeOuter(0));

            if (CONFIG.showGrid) {
                g.selectAll(".axis .tick line").attr("class", "grid-line");
            }

            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 45)
                .text(CONFIG.xAxisLabel);

            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -50)
                .text(CONFIG.yAxisLabel);

            const dots = g.selectAll(".dot")
                .data(data)
                .enter()
                .append("circle")
                .attr("class", "dot")
                .attr("cx", d => xScale(d[CONFIG.xField]))
                .attr("cy", d => yScale(d[CONFIG.yField]))
                .attr("r", CONFIG.pointRadius)
                .attr("fill", d => colorScale(d[CONFIG.categoryField]))
                .attr("stroke", "none");

            dots
                .on("mouseover", function(event, d) {
                    tooltip.transition()
                        .duration(CONFIG.transitionDuration)
                        .style("opacity", 1);

                    tooltip.html(`
                        <strong>Species:</strong> ${d[CONFIG.categoryField]}<br/>
                        <strong>${CONFIG.xAxisLabel}:</strong> ${d[CONFIG.xField]} mm<br/>
                        <strong>${CONFIG.yAxisLabel}:</strong> ${d[CONFIG.yField]} mm
                    `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");

                    d3.select(this)
                        .transition()
                        .duration(100)
                        .attr("r", CONFIG.pointRadiusHover)
                        .attr("stroke", "#fff")
                        .attr("stroke-width", 1);
                })
                .on("mouseout", function() {
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);

                    if (!d3.select(this).classed("highlighted")) {
                        d3.select(this)
                            .transition()
                            .duration(100)
                            .attr("r", CONFIG.pointRadius)
                            .attr("stroke", "none");
                    }
                });

            // -------------------------
            // LEGEND IMPLEMENTATION
            // -------------------------
            const categories = colorScale.domain();
            const legendX = CONFIG.margin.left + width + 10; // position legend to the right of chart area
            const legendY = CONFIG.margin.top;

            // Maintain active categories set
            let activeCategories = new Set(categories);

            const legend = svg.append("g")
                .attr("class", "legend")
                .attr("transform", `translate(${legendX}, ${legendY})`);

            const legendItems = legend.selectAll(".legend-item")
                .data(categories)
                .enter()
                .append("g")
                .attr("class", "legend-item")
                .attr("transform", (d, i) => `translate(0, ${i * 22})`)
                .on("click", (event, cat) => {
                    // toggle category visibility
                    if (activeCategories.has(cat)) activeCategories.delete(cat);
                    else activeCategories.add(cat);
                    updateLegendAndDots();
                })
                .on("mouseover", (event, cat) => {
                    // highlight dots of this category
                    dots.each(function(d) {
                        const sel = d[CONFIG.categoryField] === cat;
                        d3.select(this).classed("highlighted", sel).classed("faded", !sel && activeCategories.has(d[CONFIG.categoryField]) );
                    });
                })
                .on("mouseout", () => {
                    // restore highlight state
                    dots.each(function(d) {
                        d3.select(this).classed("highlighted", false).classed("faded", false);
                    });
                });

            legendItems.append("rect")
                .attr("class", "legend-rect")
                .attr("x", 0)
                .attr("y", -10)
                .attr("width", 16)
                .attr("height", 16)
                .attr("rx", 3)
                .attr("fill", d => colorScale(d))
                .attr("opacity", 1);

            legendItems.append("text")
                .attr("class", "legend-text")
                .attr("x", 22)
                .attr("y", 0)
                .attr("dy", "0.35em")
                .text(d => d);

            function updateLegendAndDots() {
                // update dots visibility based on activeCategories
                dots.attr("display", d => activeCategories.has(d[CONFIG.categoryField]) ? null : "none");

                // update legend item style
                legendItems.select("rect")
                    .attr("opacity", d => activeCategories.has(d) ? 1 : 0.25);
                legendItems.select("text")
                    .attr("opacity", d => activeCategories.has(d) ? 1 : 0.4);
            }

            // initial legend/dot state
            updateLegendAndDots();

            // -------------------------
            // BRUSHING
            // -------------------------
            if (CONFIG.enableBrushing) {
                const brush = d3.brush()
                    .extent([[0, 0], [width, height]])
                    .on("start brush end", updateBrush);

                const brushGroup = g.append("g")
                    .attr("class", "brush")
                    .call(brush);

                function updateBrush(event) {
                    const selection = event.selection;

                    if (!selection) {
                        dots
                            .classed("highlighted", false)
                            .classed("faded", false)
                            .transition()
                            .duration(CONFIG.transitionDuration)
                            .attr("opacity", 0.7);

                        selectionDisplay.classed("visible", false);
                        return;
                    }

                    let selectedCount = 0;

                    dots.each(function(d) {
                        const x = xScale(d[CONFIG.xField]);
                        const y = yScale(d[CONFIG.yField]);

                        const isSelected = x >= selection[0][0] && x <= selection[1][0] &&
                                           y >= selection[0][1] && y <= selection[1][1];

                        // don't consider points that are currently hidden by legend selection
                        const visibleByLegend = activeCategories.has(d[CONFIG.categoryField]);

                        if (isSelected && visibleByLegend) selectedCount++;

                        d3.select(this)
                            .classed("highlighted", isSelected && visibleByLegend)
                            .classed("faded", !isSelected || !visibleByLegend);
                    });

                    selectionDisplay
                        .text(`${selectedCount} points selected`)
                        .classed("visible", true);
                }

                // Double click clears selection
                svg.on("dblclick", function() {
                    brushGroup.call(brush.clear);
                    selectionDisplay.classed("visible", false);
                    updateBrush({ selection: null });
                });
            }
        }

        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
    """)

    # -------------------------------------------------------------------------
    # RENDER TEMPLATE WITH DATA
    # -------------------------------------------------------------------------
    html_content = html_template.render(
        data_json=data_json,
        config=config,
        theme=theme
    )

    return common.html_to_obj(html_content)