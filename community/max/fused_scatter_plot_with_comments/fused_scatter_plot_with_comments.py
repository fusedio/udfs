@fused.udf
def udf():
    # -----------------------------------------------------------------------------
    # Template UDF: Responsive scatter plot using D3 + embedded JSON data.
    #
    # Goal:
    # - Provide a fully working HTML UDF that draws a dark-themed scatter plot
    #   (Penguin bill length vs bill depth) with categorical color by species.
    # - This file is intended as a re-usable template. Extensive comments below
    #   explain how to adapt this to another dataset or to change styling/behavior.
    #
    # Important reuse notes (high-level):
    # - The client-side JS expects `data` to be an array of objects like:
    #   [{ "xField": 45.1, "yField": 14.2, "category": "A" }, ...]
    #   In this example the field names are bill_length_mm, bill_depth_mm, species.
    # - If you change column names in the dataframe, update both:
    #   1) the `data_json` construction in Python (the fields exported to JSON)
    #   2) the JS code that references those fields (x/y accessors, color domain)
    # - Keep the exported JSON "minimal": include only columns used by the chart
    #   and tooltips. This reduces payload size and improves performance.
    # -----------------------------------------------------------------------------

    # Use the recommended `common` helper - do NOT access `.utils` directly.
    # `common.html_to_obj()` is the recommended way to return an HTML UDF.
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    # -------------------------------------------------------------------------
    # Data loading: wrap heavy operations in a cached function to speed up
    # subsequent runs of the UDF. If you replace the source with a remote
    # dataset (S3, HTTP, database), put the loading logic inside a @fused.cache
    # function so re-runs are faster.
    # -------------------------------------------------------------------------
    @fused.cache
    def load_penguins():
        # Current example: load palmer penguins CSV and drop rows missing the
        # two measurement columns we plot.
        # If using a different dataset:
        # - Replace the URL or loading logic here (e.g., pd.read_parquet, pd.read_csv)
        # - Ensure the DataFrame contains numeric columns for the X and Y axes.
        return pd.read_csv(
            "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        ).dropna(subset=["bill_length_mm", "bill_depth_mm"])

    df = load_penguins()

    # -------------------------------------------------------------------------
    # Prepare minimal JSON payload for the client-side chart.
    # - Only include the fields the JS needs (x, y, category, and any tooltip fields).
    # - If you rename columns in your dataset, update the list below.
    #
    # Example: if your dataset has 'height_cm' and 'weight_kg' and a 'group' column,
    # change this to: df[['height_cm','weight_kg','group']].to_json(...)
    # -------------------------------------------------------------------------
    data_json = df[["bill_length_mm", "bill_depth_mm", "species"]].to_json(orient="records")

    # -------------------------------------------------------------------------
    # HTML + JS template:
    # - This is an f-string where only {data_json} is evaluated by Python.
    # - All JavaScript template braces and other braces inside the f-string are
    #   escaped (doubled) so the Python f-string remains valid.
    # - Extensive inline comments below explain how to adapt each section.
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Penguins</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        /* -----------------------------------------------------------
           Layout & overall theme - change colors/fonts here to fit
           another codebase or design system.
           ----------------------------------------------------------- */
        body{{margin:0;background:#1a1a1a;color:#fff;font-family:sans-serif;height:100vh}}
        svg{{width:100%;height:100%}}

        /* Point styling and hover states. If you want hollow points, add stroke and set fill to 'none'. */
        .dot{{fill:#E8FF59;stroke:none;cursor:pointer}}
        .dot:hover{{stroke:#fff;stroke-width:2px}}

        /* Axis and label styling */
        .axis text{{fill:#fff;font-size:12px}}
        .axis-label{{fill:#fff;font-size:14px;font-weight:bold;text-anchor:middle}}
        .title {{fill:#fff;font-size:18px;font-weight:bold;text-anchor:middle}}

        /* Tooltip styling - absolute positioned div, visually matches dark theme */
        .tooltip {{
            position: absolute;
            padding: 10px;
            background: rgba(0, 0, 0, 0.8);
            color: white;
            border-radius: 5px;
            pointer-events: none;
            font-size: 12px;
            box-shadow: 0 0 10px rgba(0,0,0,0.5);
        }}
    </style>
</head>
<body>
    <svg></svg>
    <div class="tooltip" style="opacity: 0;"></div>

    <script>
        // ---------------------------------------------------------------------
        // Data: injected from Python as JSON string -> parsed into JS objects.
        // If you change the fields included in data_json, make sure you update
        // the JS accessors below accordingly (e.g., d.myXField, d.myYField).
        // ---------------------------------------------------------------------
        const data = {data_json};

        // Selectors and layout constants
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        // Margin used on all sides of the plot area. Adjust to make room for labels.
        const margin = 70;

        // draw() handles responsive sizing, scales, axes, points and interactions.
        function draw() {{
            // ------------------------
            // Responsive sizing:
            // - width/height computed from svg client size so the chart scales.
            // - Subtract margins to get inner plotting dimensions.
            // ------------------------
            const w = svg.node().clientWidth - 2*margin;
            const h = svg.node().clientHeight - 2*margin;

            // Clear previous drawing to allow re-draw on resize.
            svg.selectAll("*").remove();

            // Group element shifted by margin, this is the main plotting group.
            const g = svg.append("g").attr("transform", `translate(${{margin}},${{margin}})`);

            // Title centered at the top of the whole SVG (not inside the margin group).
            svg.append("text")
                .attr("class", "title")
                .attr("x", svg.node().clientWidth / 2)
                .attr("y", 25)
                .text("Penguin Bill Measurements by Species");

            // ------------------------
            // Scales: linear scales using the extents of the data.
            // - If your data has outliers you want to clip, consider using
            //   d3.extent or setting a custom domain like [min, max] with padding.
            // - If x or y are dates, use d3.scaleTime instead.
            // ------------------------
            const x = d3.scaleLinear().domain(d3.extent(data, d => d.bill_length_mm)).range([0, w]);
            const y = d3.scaleLinear().domain(d3.extent(data, d => d.bill_depth_mm)).range([h, 0]);

            // ------------------------
            // Color scale: categorical mapping per species.
            // - If you change the categorical field name, update the accessor here.
            // - To use a custom palette: set .range([...yourColors...]).
            // - For continuous color mapping (e.g., a numeric variable), use
            //   d3.scaleSequential or d3.scaleLinear with an interpolator.
            // ------------------------
            const color = d3.scaleOrdinal()
                .domain([...new Set(data.map(d => d.species))]) // unique categories
                .range(["#ff7f0e","#2ca02c","#d62728"]);        // example palette

            // ------------------------
            // Axes:
            // - Axis tick formatting can be customized, e.g., .tickFormat(d3.format(".1f"))
            // - For date axes, use d3.axisBottom(x).ticks(d3.timeMonth.every(1)) etc.
            // ------------------------
            g.append("g")
                .attr("transform", `translate(0,${{h}})`)
                .call(d3.axisBottom(x));

            g.append("g")
                .call(d3.axisLeft(y));

            // Axis labels. Update units/label text if you change the data fields.
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", w / 2)
                .attr("y", h + 40)
                .text("Bill Length (mm)");

            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -h / 2)
                .attr("y", -40)
                .text("Bill Depth (mm)");

            // ------------------------
            // Draw points:
            // - We bind the data array to circle elements.
            // - Position is computed using the x/y scales.
            // - Radius and stroke can be customized; for many points reduce radius.
            // ------------------------
            g.selectAll("circle").data(data).enter().append("circle")
                .attr("class","dot")
                .attr("cx", d => x(d.bill_length_mm))   // <-- update this if using different X field
                .attr("cy", d => y(d.bill_depth_mm))    // <-- update this if using different Y field
                .attr("r", 5)
                .attr("fill", d => color(d.species))    // <-- update if using different categorical field
                // Tooltip interactions: show species and measurements on hover.
                .on("mouseover", function(event, d) {{
                    // Make tooltip visible and populate with HTML. If you add fields,
                    // append them here for richer tooltips.
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", .9);
                    tooltip.html(`<strong>Species:</strong> ${{d.species}}<br/>
                                 <strong>Bill Length:</strong> ${{d.bill_length_mm}} mm<br/>
                                 <strong>Bill Depth:</strong> ${{d.bill_depth_mm}} mm`)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                }})
                .on("mouseout", function(d) {{
                    // Hide tooltip smoothly.
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                }});
        }}

        // Initial render and responsive redraw on window resize.
        draw();
        window.addEventListener("resize", draw);

        // ----------------------------------------------
        // Tips for re-use and extension (client-side):
        // - To add a legend: compute unique categories (new Set(...)) and append
        //   a small group of rect+text elements, mapping color(category) to a swatch.
        // - To support brushing/selection: add an overlay rect and use d3.brush().
        // - To support zooming/panning: wrap scales with d3.zoom and re-render axes.
        // - For very large datasets (10k+ points): consider using WebGL (deck.gl)
        //   or reduce the number of DOM circles by binning or sampling.
        // - If you want to change the theme: adjust CSS above (background, axis colors).
        // ----------------------------------------------
    </script>
</body>
</html>
"""

    # Return the HTML via the common helper. This is the recommended pattern for HTML UDFs.
    return common.html_to_obj(html_content)