@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Interactive Pie Chart with Tooltips  
    # WHEN TO USE: Show proportions/composition of a whole, compare parts to total, demographic breakdowns
    # DATA REQUIREMENTS: Categorical data with counts/values, works best with 3-10 categories
    # PIE CHART SPECIFICS: Each slice represents category's share of total, uses angular encoding, shows percentages
    # =============================================================================

    import pandas as pd
    import json
    from jinja2 import Template

    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    @fused.cache
    def load_acs_data():
        """
        PIE CHART DATA REQUIREMENTS:
        - Must have categorical data with numeric counts/values
        - Categories should be mutually exclusive (no overlap)
        - Values should sum to meaningful total
        - Works best with 3-10 categories (too many = hard to read)
        - Remove zero/negative values
        """
        # ACS Census table configuration
        table_id = 'B02001'  # Race and Ethnicity table
        year = 2022

        # Load ACS table from census.gov
        url = f'https://www2.census.gov/programs-surveys/acs/summary_file/{year}/table-based-SF/data/5YRData/acsdt5y{year}-{table_id.lower()}.dat'
        df = pd.read_csv(url, delimiter='|')

        print(f"Loaded ACS table {table_id} for {year}")
        print("Available columns:", [col for col in df.columns if col.startswith(f'{table_id}_E')][:10])

        return df, table_id, year

    df, table_id, year = load_acs_data()

    # PIE CHART DATA STRUCTURE: Map ACS columns to category names
    category_mapping = {
        'B02001_E002': 'White alone',
        'B02001_E003': 'Black or African American alone',
        'B02001_E004': 'American Indian and Alaska Native alone',
        'B02001_E005': 'Asian alone',
        'B02001_E006': 'Native Hawaiian and Other Pacific Islander alone',
        'B02001_E007': 'Some other race alone',
        'B02001_E008': 'Two or more races'
    }

    # PIE CHART REQUIREMENT: Extract categorical data with counts
    chart_data = []
    for col, category in category_mapping.items():
        if col in df.columns:
            value = df[col].iloc[0]  # Get first row (usually US total)
            if value > 0:  # Only include non-zero values
                chart_data.append([category, int(value)])  # [name, value] format required by D3 pie

    # Sort by value for better visual hierarchy (largest slice first)
    chart_data.sort(key=lambda x: x[1], reverse=True)

    # Convert to JSON for D3
    data_json = json.dumps(chart_data)

    print("Chart data prepared:")
    for name, value in chart_data:
        print(f"  {name}: {value:,}")

    # PIE CHART CONFIGURATION
    config = {
        # PIE/DONUT STRUCTURE
        "width": 640,
        "height": 400,
        "innerRadius": 0,  # 0 = pie chart, >0 = donut chart
        "stroke": "white",  # Color between slices
        "strokeWidth": 1,  # Width of slice borders
        "padAngle": 0,

        # PIE CHART APPEARANCE
        "format": ",",  # Number format for values
        "showLabels": False,
        "showLegend": True,
        "colorScheme": "schemeSpectral"
    }

    metadata = {
        "title": "US Population by Race/Ethnicity",
        "subtitle": f"American Community Survey {year}",
        "table_id": table_id,
        "year": year
    }

    theme = {
        "font_family": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
        "bg_color": "white",
        "text_color": "#333",
        "subtitle_color": "#666",
        "border_color": "#ccc"
    }

    html_template = Template("""
<!DOCTYPE html>
<html>
<head>
    <title>{{ metadata.title }}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {
            font-family: {{ theme.font_family }};
            margin: 0;
            padding: 20px;
            background: {{ theme.bg_color }};
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            text-align: center;
        }
        h1 {
            font-size: 24px;
            margin-bottom: 10px;
            color: {{ theme.text_color }};
        }
        .subtitle {
            font-size: 14px;
            color: {{ theme.subtitle_color }};
            margin-bottom: 30px;
        }
        svg {
            font: 10px sans-serif;
            display: block;
            margin: 0 auto;
        }
        .legend {
            margin-top: 30px;
            display: inline-block;
            text-align: left;
        }
        .legend-item {
            display: flex;
            align-items: center;
            margin: 4px 0;
            font-size: 13px;
        }
        .legend-color {
            width: 18px;
            height: 18px;
            margin-right: 8px;
            border: 1px solid {{ theme.border_color }};
        }
        /* Tooltip styling */
        .tooltip {
            position: absolute;
            pointer-events: none;
            display: none;
            background: rgba(255,255,255,0.95);
            border: 1px solid #ddd;
            padding: 8px 10px;
            border-radius: 4px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.12);
            font-size: 13px;
            color: {{ theme.text_color }};
            z-index: 1000;
            min-width: 120px;
            text-align: left;
        }
        @media (max-width: 480px) {
            h1 { font-size: 18px; }
            .subtitle { font-size: 12px; margin-bottom: 20px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>{{ metadata.title }}</h1>
        <p class="subtitle">{{ metadata.subtitle }}</p>
        <div id="chart"></div>
        <div id="legend" class="legend"></div>
    </div>
    <div id="tooltip" class="tooltip" aria-hidden="true"></div>

    <script>
        // =================================================================
        // DATA AND CONFIGURATION
        // =================================================================

        const data = {{ data_json | safe }};
        const CONFIG = {{ config | tojson }};

        // =================================================================
        // PIE CHART FUNCTION (chart-specific D3 logic)
        // - Adds a styled tooltip that follows the mouse
        // - SVG uses viewBox + max-width for responsiveness (scales vector)
        // =================================================================

        function PieChart(data, {
            name = ([x]) => x,
            value = ([, y]) => y,
            title,
            width = CONFIG.width,
            height = CONFIG.height,
            innerRadius = CONFIG.innerRadius,
            outerRadius = Math.min(width, height) / 2,
            labelRadius = (innerRadius * 0.2 + outerRadius * 0.8),
            format = CONFIG.format,
            names,
            colors,
            stroke = CONFIG.stroke,
            strokeWidth = CONFIG.strokeWidth,
            strokeLinejoin = CONFIG.strokeLinejoin,
            padAngle = CONFIG.padAngle
        } = {}) {

            // Process data for pie chart
            const N = d3.map(data, name);
            const V = d3.map(data, value);
            const I = d3.range(N.length).filter(i => !isNaN(V[i]));

            // Set up color scheme
            if (names === undefined) names = N;
            names = new d3.InternSet(names);

            if (colors === undefined) colors = d3[CONFIG.colorScheme][names.size];
            if (colors === undefined) colors = d3.quantize(t => d3.interpolateSpectral(t * 0.8 + 0.1), names.size);

            const color = d3.scaleOrdinal(names, colors);

            // Default title / tooltip text generator
            if (title === undefined) {
                const formatValue = d3.format(format);
                title = i => `${N[i]}\\n${formatValue(V[i])}`;
            } else {
                const O = d3.map(data, d => d);
                const T = title;
                title = i => T(O[i], i, data);
            }

            // PIE CHART GEOMETRY
            const arcs = d3.pie().padAngle(padAngle).sort(null).value(i => V[i])(I);
            const arc = d3.arc().innerRadius(innerRadius).outerRadius(outerRadius);
            const arcLabel = d3.arc().innerRadius(labelRadius).outerRadius(labelRadius);

            const svg = d3.create("svg")
                .attr("width", width)
                .attr("height", height)
                .attr("viewBox", [-width / 2, -height / 2, width, height])
                .attr("style", "max-width: 100%; height: auto; height: intrinsic;");

            // Tooltip element (uses the floating div #tooltip)
            const tooltip = d3.select("#tooltip");

            // Calculate total once for percentages
            const total = d3.sum(V);

            // CREATE PIE SLICES
            const slices = svg.append("g")
                .attr("stroke", stroke)
                .attr("stroke-width", strokeWidth)
                .attr("stroke-linejoin", strokeLinejoin)
                .selectAll("path")
                .data(arcs)
                .join("path")
                .attr("fill", d => color(N[d.data]))
                .attr("d", arc);

            // Add accessible title elements (still useful for screen readers)
            slices.append("title").text(d => title(d.data));

            // Add interactive tooltip behavior (mouseover, move, out)
            slices
                .on("mouseover", (event, d) => {
                    const idx = d.data;
                    const val = V[idx];
                    const percent = ((val / total) * 100).toFixed(1);
                    const nameText = N[idx];
                    tooltip
                        .style("display", "block")
                        .html(`<strong>${nameText}</strong><br/>${val.toLocaleString()} (${percent}%)`)
                        .attr("aria-hidden", "false");
                })
                .on("mousemove", (event) => {
                    // Position tooltip near the pointer, with small offset
                    const x = event.pageX + 12;
                    const y = event.pageY + 12;
                    tooltip.style("left", x + "px").style("top", y + "px");
                })
                .on("mouseout", () => {
                    tooltip.style("display", "none").attr("aria-hidden", "true");
                });

            // Add labels if enabled
            if (CONFIG.showLabels) {
                svg.append("g")
                    .attr("font-family", "sans-serif")
                    .attr("font-size", 10)
                    .attr("text-anchor", "middle")
                    .selectAll("text")
                    .data(arcs)
                    .join("text")
                    .attr("transform", d => `translate(${arcLabel.centroid(d)})`)
                    .selectAll("tspan")
                    .data(d => {
                        const lines = `${title(d.data)}`.split(/\\n/);
                        return (d.endAngle - d.startAngle) > 0.25 ? lines : lines.slice(0, 1);
                    })
                    .join("tspan")
                    .attr("x", 0)
                    .attr("y", (_, i) => `${i * 1.1}em`)
                    .attr("font-weight", (_, i) => i ? null : "bold")
                    .text(d => d);
            }

            return Object.assign(svg.node(), {scales: {color}});
        }

        // =================================================================
        // INITIALIZE PIE CHART
        // =================================================================

        // Create chart and append (SVG is responsive via viewBox + max-width)
        const chart = PieChart(data);
        document.getElementById("chart").appendChild(chart);

        // PIE CHART LEGEND
        if (CONFIG.showLegend) {
            createLegend(data, chart.scales.color);
        }

        // =================================================================
        // LEGEND FUNCTION
        // =================================================================

        function createLegend(data, colorScale) {
            const legend = document.getElementById("legend");
            legend.innerHTML = "";

            const total = data.reduce((sum, [, v]) => sum + v, 0);

            data.forEach(([name, value]) => {
                const item = document.createElement("div");
                item.className = "legend-item";

                const colorBox = document.createElement("div");
                colorBox.className = "legend-color";
                colorBox.style.backgroundColor = colorScale(name);

                const label = document.createElement("span");
                const percentage = ((value / total) * 100).toFixed(1);
                label.textContent = `${name} (${percentage}%)`;

                item.appendChild(colorBox);
                item.appendChild(label);
                legend.appendChild(item);
            });
        }

        console.log("Pie Chart created with data:", data);
    </script>
</body>
</html>
    """)

    html_content = html_template.render(
        data_json=data_json,
        config=config,
        metadata=metadata,
        theme=theme
    )

    return common.html_to_obj(html_content)