@fused.udf
def udf():
    import pandas as pd
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_penguins():
        return pd.read_csv("https://raw.githubusercontent.com/allisonhorst/palmerpenguins/master/inst/extdata/penguins.csv").dropna(subset=["bill_length_mm", "bill_depth_mm"])
    
    df = load_penguins()
    
    return common.html_to_obj(f"""
<!DOCTYPE html>
<html>
<head>
    <title>Penguins</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body{{margin:0;background:#1a1a1a;color:#fff;font-family:sans-serif;height:100vh}}
        svg{{width:100%;height:100%}}
        .dot{{fill:#E8FF59;stroke:none;cursor:pointer}}
        .dot:hover{{stroke:#fff;stroke-width:2px}}
        .axis text{{fill:#fff;font-size:12px}}
        .axis-label{{fill:#fff;font-size:14px;font-weight:bold;text-anchor:middle}}
        .title {{fill:#fff;font-size:18px;font-weight:bold;text-anchor:middle}}
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
        const data = {df[["bill_length_mm","bill_depth_mm","species"]].to_json(orient="records")};
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        const margin = 70;
        
        function draw() {{
            const w = svg.node().clientWidth - 2*margin;
            const h = svg.node().clientHeight - 2*margin;
            svg.selectAll("*").remove();
            const g = svg.append("g").attr("transform", `translate(${{margin}},${{margin}})`);
            
            // Add title
            svg.append("text")
                .attr("class", "title")
                .attr("x", svg.node().clientWidth / 2)
                .attr("y", 25)
                .text("Penguin Bill Measurements by Species");
            
            const x = d3.scaleLinear().domain(d3.extent(data, d => d.bill_length_mm)).range([0, w]);
            const y = d3.scaleLinear().domain(d3.extent(data, d => d.bill_depth_mm)).range([h, 0]);
            const color = d3.scaleOrdinal().domain([...new Set(data.map(d => d.species))]).range(["#ff7f0e","#2ca02c","#d62728"]);
            
            // Add X axis
            g.append("g")
                .attr("transform", `translate(0,${{h}})`)
                .call(d3.axisBottom(x));
            
            // Add Y axis
            g.append("g")
                .call(d3.axisLeft(y));
            
            // Add X axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", w / 2)
                .attr("y", h + 40)
                .text("Bill Length (mm)");
            
            // Add Y axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -h / 2)
                .attr("y", -40)
                .text("Bill Depth (mm)");
            
            g.selectAll("circle").data(data).enter().append("circle")
                .attr("class","dot")
                .attr("cx", d => x(d.bill_length_mm))
                .attr("cy", d => y(d.bill_depth_mm))
                .attr("r", 5)
                .attr("fill", d => color(d.species))
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
        
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
""")