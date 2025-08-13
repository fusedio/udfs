@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Histogram with Tooltips
    # WHEN TO USE: Show distribution of ONE numeric variable, identify patterns like normal/skewed distributions, outliers, data clusters
    # DATA REQUIREMENTS: Exactly 1 numeric column (continuous data works best), optional 1 categorical column for grouping
    # HISTOGRAM SPECIFICS: Creates bins from continuous data, shows frequency in each range, good for 50+ data points
    # =============================================================================

    import pandas as pd
    import numpy as np
    import json
    from jinja2 import Template

    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    @fused.cache
    def load_data():
        """
        HISTOGRAM DATA REQUIREMENTS:
        - Must have numeric column with continuous values (not just categories)
        - Best with 50+ data points (fewer points = sparse histogram)
        - Remove/handle missing values in numeric column
        - Data should have reasonable spread (not all same value)
        """
        url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
        column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
        df = pd.read_csv(url, names=column_names)
        # HISTOGRAM REQUIREMENT: Clean numeric data
        df = df.dropna(subset=['sepal_length'])
        return df

    df = load_data()

    # HISTOGRAM REQUIREMENT: Select numeric column + optional category column
    try:
        chart_data = df[["sepal_length", "species"]]  # [numeric_col, optional_category_col]
    except KeyError as e:
        available_cols = list(df.columns)
        raise ValueError(f"Column not found. Available columns: {available_cols}. Error: {e}")

    data_json = chart_data.to_json(orient="records")

    config = {
        # HISTOGRAM CORE SETTINGS
        "numericField": "sepal_length",           # The continuous numeric column to analyze
        "categoryField": "species",               # Optional: for colored/grouped histograms

        # BIN CONFIGURATION (histogram-specific)
        "numBins": 15,                            # Number of bins: 10-20 for most data
        "domainMin": "auto",                      # "auto" uses data min
        "domainMax": "auto",                      # "auto" uses data max

        # LABELS
        "xAxisLabel": "Sepal Length (cm)",
        "yAxisLabel": "Count",
        "chartTitle": "Iris Sepal Length Distribution",

        # APPEARANCE
        "barColor": "#008080",                    # Updated to teal
        "barOpacity": 0.8,
        "showGrid": True,
        "margin": {"top": 80, "right": 30, "bottom": 80, "left": 70}
    }

    metadata = {
        "title": config["chartTitle"],
        "subtitle": f"Distribution of {config['xAxisLabel'].lower()} across dataset",
        "dataset_size": len(df),
        "numeric_column": config["numericField"]
    }

    theme = {
        "font_family": "Arial, sans-serif",
        "bg_color": "white",
        "text_color": "black",
        "subtitle_color": "grey",
        "axis_color": "#666",
        "grid_color": "#e0e0e0",
        "tooltip_bg": "white",
        "tooltip_border": "solid 2px black"
    }

    # -------------------------------------------------------------------------
    # JINJA2 TEMPLATE
    # -------------------------------------------------------------------------

    html_template = Template("""<!DOCTYPE html>
<html>
<head>
    <title>{{ metadata.title }}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {
            margin: 20px;
            background: {{ theme.bg_color }};
            color: {{ theme.text_color }};
            font-family: {{ theme.font_family }};
        }
        svg { width: 100%; height: calc(100vh - 120px); }

        .bar {
            cursor: pointer;
            transition: all 0.2s ease;
            opacity: {{ config.barOpacity }};
        }
        .bar:hover {
            stroke: {{ theme.text_color }};
            stroke-width: 2px;
            opacity: 1;
        }

        .axis text {
            fill: {{ theme.text_color }};
            font-size: 12px;
            font-family: {{ theme.font_family }};
        }
        .axis path, .axis line { stroke: {{ theme.axis_color }}; }
        .grid-line {
            stroke: {{ theme.grid_color }};
            stroke-width: 0.5;
            opacity: 0.7;
        }

        .axis-label {
            fill: {{ theme.text_color }};
            font-size: 14px;
            text-anchor: middle;
            font-family: {{ theme.font_family }};
        }

        .tooltip {
            position: absolute;
            text-align: center;
            padding: 8px;
            font-size: 12px;
            background: {{ theme.tooltip_bg }};
            border: {{ theme.tooltip_border }};
            border-radius: 5px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.2s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            font-family: {{ theme.font_family }};
        }

        .chart-title {
            font-size: 22px;
            font-weight: bold;
            margin-bottom: 5px;
            color: {{ theme.text_color }};
        }
        .chart-subtitle {
            font-size: 14px;
            color: {{ theme.subtitle_color }};
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="chart-title">{{ metadata.title }}</div>
    <div class="chart-subtitle">{{ metadata.subtitle }}</div>
    <svg></svg>
    <div class="tooltip"></div>

    <script>
        // DATA AND CONFIGURATION
        const data = {{ data_json | safe }};
        const CONFIG = {{ config | tojson }};

        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");

        function draw() {
            const svgWidth = svg.node().clientWidth;
            const svgHeight = svg.node().clientHeight;
            const width = svgWidth - CONFIG.margin.left - CONFIG.margin.right;
            const height = svgHeight - CONFIG.margin.top - CONFIG.margin.bottom;

            // Clear previous content
            svg.selectAll("*").remove();

            // Main group
            const g = svg.append("g")
                .attr("transform", "translate(" + CONFIG.margin.left + "," + CONFIG.margin.top + ")");

            // Determine domain
            const domainMin = CONFIG.domainMin === "auto" ? d3.min(data, d => d[CONFIG.numericField]) : CONFIG.domainMin;
            const domainMax = CONFIG.domainMax === "auto" ? d3.max(data, d => d[CONFIG.numericField]) : CONFIG.domainMax;

            // X scale
            const x = d3.scaleLinear()
                .domain([domainMin, domainMax])
                .range([0, width])
                .nice();

            // Bins
            const bins = d3.histogram()
                .value(d => d[CONFIG.numericField])
                .domain(x.domain())
                .thresholds(x.ticks(CONFIG.numBins))
                (data);

            // Y scale
            const y = d3.scaleLinear()
                .domain([0, d3.max(bins, d => d.length)])
                .range([height, 0])
                .nice();

            // Grid lines
            if (CONFIG.showGrid) {
                g.append("g")
                    .attr("class", "grid")
                    .attr("transform", "translate(0," + height + ")")
                    .call(d3.axisBottom(x).tickSize(-height).tickFormat(""))
                    .selectAll("line")
                    .attr("class", "grid-line");

                g.append("g")
                    .attr("class", "grid")
                    .call(d3.axisLeft(y).tickSize(-width).tickFormat(""))
                    .selectAll("line")
                    .attr("class", "grid-line");
            }

            // X axis
            g.append("g")
                .attr("class", "axis")
                .attr("transform", "translate(0," + height + ")")
                .call(d3.axisBottom(x));

            // Y axis
            g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y));

            // X axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 50)
                .text(CONFIG.xAxisLabel);

            // Y axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -45)
                .text(CONFIG.yAxisLabel);

            // Bars
            g.selectAll("rect")
                .data(bins)
                .enter()
                .append("rect")
                .attr("class", "bar")
                .attr("x", d => x(d.x0))
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
                .attr("y", d => y(d.length))
                .attr("height", d => height - y(d.length))
                .attr("fill", CONFIG.barColor)
                .on("mouseover", function(event, d) {
                    const rangeText = d.x0.toFixed(2) + " - " + d.x1.toFixed(2);
                    const percentage = (d.length / data.length * 100).toFixed(1);
                    tooltip.style("opacity", 1)
                        .html("<strong>Range:</strong> " + rangeText + "<br/>" +
                              "<strong>Count:</strong> " + d.length + " items<br/>" +
                              "<strong>Percentage:</strong> " + percentage + "%")
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", function() {
                    tooltip.style("opacity", 0);
                });

            console.log("Histogram created: " + bins.length + " bins, " + data.length + " data points");
        }

        // Initialize and handle resize
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
""")

    # RENDER TEMPLATE
    html_content = html_template.render(
        data_json=data_json,
        config=config,
        metadata=metadata,
        theme=theme
    )

    return common.html_to_obj(html_content)