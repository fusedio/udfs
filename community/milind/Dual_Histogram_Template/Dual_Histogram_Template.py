@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Double (Overlaid) Histogram
    # WHEN TO USE: Compare distributions between two groups/categories, show differences in shape/spread/central tendency
    # DATA REQUIREMENTS: 1 continuous numeric variable + 1 categorical variable with exactly 2 categories to compare
    # DOUBLE HISTOGRAM SPECIFICS: Same bins for both series, overlaid bars with transparency, comparative analysis focus
    # =============================================================================

    import pandas as pd
    from jinja2 import Template

    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    @fused.cache
    def load_data():
        """
        DOUBLE HISTOGRAM DATA REQUIREMENTS:
        - Must have continuous numeric variable for distribution analysis
        - Must have categorical variable with at least 2 distinct groups
        - Both groups should have sufficient data points (20+ each) for meaningful comparison
        - Remove missing values from both numeric and categorical columns
        """
        url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
        column_names = [
            "sepal_length",
            "sepal_width", 
            "petal_length",
            "petal_width",
            "species",
        ]
        return pd.read_csv(url, names=column_names)

    df = load_data()

    # DOUBLE HISTOGRAM REQUIREMENT: Select numeric + categorical columns
    chart_data = df[["sepal_length", "species"]]
    data_json = chart_data.to_json(orient="records")

    # DOUBLE HISTOGRAM CONFIGURATION
    config = {
        # CORE FIELDS
        "numericField": "sepal_length",           # Continuous variable to compare
        "categoryField": "species",               # Grouping variable
        
        # COMPARISON SETUP: Exactly 2 categories to overlay
        "series": ["Iris-setosa", "Iris-versicolor"],  # Two specific groups to compare
        "seriesLabels": ["Setosa", "Versicolor"],      # Display names for legend
        "seriesColors": ["#69b3a2", "#404080"],        # Colors for each series (need contrast)
        
        # HISTOGRAM BINNING: Same structure for fair comparison
        "numBins": 30,                           # Same bins applied to both series
        "domainMin": 4,                          # Fixed domain ensures same scale
        "domainMax": 8,                          # Fixed domain ensures same scale
        
        # LABELS
        "xAxisLabel": "Sepal Length (cm)",
        "yAxisLabel": "Count",                   # Raw count (not density) for easier comparison
        "chartTitle": "Iris Sepal Length – Overlaid Histograms",
    }

    theme = {
        "bg_color": "#FAFAFA",
        "contrast_color": "#0F172A",
        "mid_color": "#6B7280",
        "tooltip_bg": "rgba(15, 23, 42, 0.9)",
        "font_family": "-apple-system, BlinkMacSystemFont, sans-serif",
    }

    html_template = Template("""<!DOCTYPE html>
<html>
<head>
    <title>{{ config.chartTitle }}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        :root {
            --bg-color: {{ theme.bg_color }};
            --contrast-color: {{ theme.contrast_color }};
            --mid-color: {{ theme.mid_color }};
            --tooltip-bg: {{ theme.tooltip_bg }};
            --font-family: {{ theme.font_family }};
        }

        body {
            margin: 0;
            background: var(--bg-color);
            color: var(--contrast-color);
            font-family: var(--font-family);
            height: 100vh;
            overflow: hidden;
        }

        svg { width: 100%; height: 100%; display: block; }

        /* Double histogram bars: Semi-transparent for overlay visibility */
        .bar-series-0 { fill: {{ config.seriesColors[0] }}; opacity: 0.6; }
        .bar-series-1 { fill: {{ config.seriesColors[1] }}; opacity: 0.6; }

        .axis .domain { stroke: var(--mid-color); stroke-width: 1; }
        .axis line { stroke: var(--mid-color); }
        .axis text { fill: var(--contrast-color); font-size: 12px; }

        .axis-label {
            fill: var(--contrast-color);
            font-size: 14px;
            font-weight: bold;
            text-anchor: middle;
        }

        .title {
            fill: var(--contrast-color);
            font-size: 18px;
            font-weight: bold;
            text-anchor: middle;
        }

        .tooltip {
            position: absolute;
            padding: 10px;
            background: var(--tooltip-bg);
            color: var(--bg-color);
            border-radius: 5px;
            pointer-events: none;
            font-size: 14px;
            opacity: 0;
            transition: opacity 0.15s ease;
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
            z-index: 10;
        }

        .legend {
            font-size: 13px;
            fill: var(--contrast-color);
        }
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
        const margin = { top: 70, right: 30, bottom: 70, left: 70 };

        function draw() {
            const width = svg.node().clientWidth - margin.left - margin.right;
            const height = svg.node().clientHeight - margin.top - margin.bottom;

            svg.selectAll("*").remove();

            const g = svg.append("g")
                .attr("transform", `translate(${margin.left},${margin.top})`);

            // SHARED X SCALE: Same domain and bins for fair comparison
            const x = d3.scaleLinear()
                .domain([CONFIG.domainMin, CONFIG.domainMax])
                .range([0, width]);

            g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${height})`)
                .call(d3.axisBottom(x));

            // DOUBLE HISTOGRAM BINNING: Same bin structure applied to both series
            const histogram = d3.histogram()
                .value(d => +d[CONFIG.numericField])
                .domain(x.domain())                    // Same domain for both
                .thresholds(x.ticks(CONFIG.numBins));  // Same bin boundaries

            // SERIES DATA SEPARATION: Filter data by category
            const seriesKeys = CONFIG.series;
            const binsA = histogram(data.filter(d => d[CONFIG.categoryField] === seriesKeys[0]));
            const binsB = histogram(data.filter(d => d[CONFIG.categoryField] === seriesKeys[1]));

            // SHARED Y SCALE: Covers maximum count from either series
            const maxCount = d3.max([
                d3.max(binsA, d => d.length || 0),
                d3.max(binsB, d => d.length || 0)
            ]);

            const y = d3.scaleLinear()
                .domain([0, maxCount])
                .range([height, 0]);

            g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y).ticks(6));

            // Axis labels
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

            // Title
            svg.append("text")
                .attr("class", "title")
                .attr("x", svg.node().clientWidth / 2)
                .attr("y", 30)
                .text(CONFIG.chartTitle);

            // FIRST HISTOGRAM SERIES: Base layer
            g.selectAll(".barA")
                .data(binsA)
                .enter()
                .append("rect")
                .attr("class", "bar-series-0")
                .attr("x", d => x(d.x0))
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
                .attr("y", d => y(d.length))
                .attr("height", d => height - y(d.length))
                .on("mouseover", function(event, d) {
                    // COMPARATIVE TOOLTIP: Show series info + percentage within group
                    const range = `${d.x0.toFixed(1)} - ${d.x1.toFixed(1)} cm`;
                    const seriesTotal = data.filter(x => x[CONFIG.categoryField] === CONFIG.series[0]).length;
                    tooltip.style("opacity", 1)
                        .html(`<strong>${CONFIG.seriesLabels[0] || CONFIG.series[0]}</strong><br>
                               <strong>Range:</strong> ${range}<br>
                               <strong>Count:</strong> ${d.length}<br>
                               <strong>Percent of series:</strong> ${((d.length/seriesTotal)*100).toFixed(1)}%`)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", () => tooltip.style("opacity", 0));

            // SECOND HISTOGRAM SERIES: Overlay layer (rendered on top)
            g.selectAll(".barB")
                .data(binsB)
                .enter()
                .append("rect")
                .attr("class", "bar-series-1")
                .attr("x", d => x(d.x0))
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
                .attr("y", d => y(d.length))
                .attr("height", d => height - y(d.length))
                .on("mouseover", function(event, d) {
                    const range = `${d.x0.toFixed(1)} - ${d.x1.toFixed(1)} cm`;
                    const seriesTotal = data.filter(x => x[CONFIG.categoryField] === CONFIG.series[1]).length;
                    tooltip.style("opacity", 1)
                        .html(`<strong>${CONFIG.seriesLabels[1] || CONFIG.series[1]}</strong><br>
                               <strong>Range:</strong> ${range}<br>
                               <strong>Count:</strong> ${d.length}<br>
                               <strong>Percent of series:</strong> ${((d.length/seriesTotal)*100).toFixed(1)}%`)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", () => tooltip.style("opacity", 0));

            // LEGEND: Essential for distinguishing overlaid series
            const legend = svg.append("g")
                .attr("transform", `translate(${width - 10}, -40)`);

            const legendItems = legend.selectAll(".legend-item")
                .data([0, 1])
                .enter()
                .append("g")
                .attr("class", "legend-item")
                .attr("transform", (d, i) => `translate(${-160}, ${i * 28})`);

            legendItems.append("circle")
                .attr("cx", 0)
                .attr("cy", 0)
                .attr("r", 6)
                .attr("fill", d => CONFIG.seriesColors[d]);

            legendItems.append("text")
                .attr("x", 18)
                .attr("y", 0)
                .attr("class", "legend")
                .attr("alignment-baseline", "middle")
                .text(d => CONFIG.seriesLabels[d] || CONFIG.series[d]);
        }

        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>""")

    html_content = html_template.render(
        data_json=data_json,
        config=config,
        theme=theme,
    )
    return common.html_to_obj(html_content)