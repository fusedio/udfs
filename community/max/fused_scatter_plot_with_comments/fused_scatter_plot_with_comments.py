@fused.udf
def udf():
    # -------------------------------------------------------------------------
    # Template UDF: FUSED branded - Responsive scatter plot using D3 + embedded JSON
    # data. Points use a single FUSED accent color (yellow) — no categorical colors.
    #
    # IMPORTANT:
    # - This is a FUSED-BRANDED scatterplot. The yellow accent color below is
    #   required when re-using this template to remain on-brand.
    # - Brand color variables are ALL-CAPS JS constants (see FUSED_BRAND_PALETTE).
    # -------------------------------------------------------------------------

    # Use the recommended `common` helper and import pandas for data loading.
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    import pandas as pd

    # -------------------------------------------------------------------------
    # Data loading: cache heavy operations so UDF reruns are fast.
    # -------------------------------------------------------------------------
    @fused.cache
    def load_penguins():
        # Load Palmer penguins CSV and drop rows missing the two measurement columns.
        return pd.read_csv(
            "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv"
        ).dropna(subset=["bill_length_mm", "bill_depth_mm"])

    df = load_penguins()

    # Prepare minimal JSON payload for the client-side chart.
    data_json = df[["bill_length_mm", "bill_depth_mm", "species"]].to_json(orient="records")

    # -------------------------------------------------------------------------
    # HTML + JS template:
    # - This is an f-string where only {data_json} is evaluated by Python.
    # - All JS/template braces are escaped (doubled) so the Python f-string is valid.
    # -------------------------------------------------------------------------
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Penguins - FUSED Branded Scatterplot (Single-Color)</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        /* -----------------------------------------------------------
           Layout & overall theme - FUSED branded colors only:
           - ACCENT (yellow): #E5FF44
           - MID             : #333333
           - BG              : #141414
           - TEXT            : #FFFFFF
           ----------------------------------------------------------- */
        body{{margin:0;background:#141414;color:#FFFFFF;font-family:sans-serif;height:100vh}}
        svg{{width:100%;height:100%}}

        /* Points use JS-controlled fill; stroke/hover styling here. */
        .dot{{stroke:none;cursor:pointer}}
        .dot:hover{{stroke:#FFFFFF;stroke-width:2px}}

        /* Axis and label styling */
        .axis text{{fill:#FFFFFF;font-size:12px}}
        .axis-label{{fill:#FFFFFF;font-size:14px;font-weight:bold;text-anchor:middle}}
        .title {{fill:#FFFFFF;font-size:18px;font-weight:bold;text-anchor:middle}}

        /* Tooltip styling - matches dark theme */
        .tooltip {{
            position: absolute;
            padding: 10px;
            background: rgba(51, 51, 51, 0.95); /* #333333 with high opacity */
            color: #FFFFFF;
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
        // Data injected from Python
        // ---------------------------------------------------------------------
        const data = {data_json};

        // ---------------------------------------------------------------------
        // FUSED BRAND CONSTANTS (ALL CAPS) - IMPORTANT:
        // - Use these variables when re-using the template to remain on-brand.
        // ---------------------------------------------------------------------
        const FUSED_BRAND_PALETTE = {{ ACCENT: "#E5FF44", MID: "#333333", BG: "#141414", WHITE: "#FFFFFF" }};

        // Selectors and layout constants
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        const margin = 70;

        // draw() handles responsive sizing, scales, axes, points and interactions.
        function draw() {{
            // Responsive sizing
            const w = svg.node().clientWidth - 2*margin;
            const h = svg.node().clientHeight - 2*margin;

            // Clear previous drawing
            svg.selectAll("*").remove();

            // Main plotting group
            const g = svg.append("g").attr("transform", `translate(${{margin}},${{margin}})`);

            // Title
            svg.append("text")
                .attr("class", "title")
                .attr("x", svg.node().clientWidth / 2)
                .attr("y", 25)
                .text("Penguin Bill Measurements by Species (Single Color)");

            // Scales
            const x = d3.scaleLinear().domain(d3.extent(data, d => d.bill_length_mm)).range([0, w]);
            const y = d3.scaleLinear().domain(d3.extent(data, d => d.bill_depth_mm)).range([h, 0]);

            // Axes
            g.append("g")
                .attr("transform", `translate(0,${{h}})`)
                .call(d3.axisBottom(x));

            g.append("g")
                .call(d3.axisLeft(y));

            // Axis labels
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

            // Draw points: SINGLE COLOR for all points (FUSED accent)
            g.selectAll("circle").data(data).enter().append("circle")
                .attr("class","dot")
                .attr("cx", d => x(d.bill_length_mm))
                .attr("cy", d => y(d.bill_depth_mm))
                .attr("r", 5)
                .attr("fill", FUSED_BRAND_PALETTE.ACCENT) // All points use the FUSED yellow accent
                .on("mouseover", function(event, d) {{
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
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                }});
        }}

        // Initial render and responsive redraw on window resize.
        draw();
        window.addEventListener("resize", draw);

        // ---------------------------------------------------------------------
        // Notes for re-use:
        // - This template purposefully uses a single FUSED ACCENT color for all
        //   points. To remain on-brand, keep FUSED_BRAND_PALETTE.ACCENT when
        //   reusing this UDF. If you must change colors, update FUSED_BRAND_PALETTE.
        // - If you later want categorical coloring, add a color scale that references
        //   FUSED_BRAND_PALETTE values, but avoid introducing non-brand colors.
        // ---------------------------------------------------------------------
    </script>
</body>
</html>
"""

    # Return the HTML via the common helper.
    return common.html_to_obj(html_content)