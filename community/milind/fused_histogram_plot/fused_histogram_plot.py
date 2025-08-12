@fused.udf
def udf():
    
    import pandas as pd
    import numpy as np
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_iris():
        # Load the classic Iris dataset
        url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
        column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
        return pd.read_csv(url, names=column_names)
    
    df = load_iris()
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Iris Histogram with Tooltips</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body{{margin:0;background:#1a1a1a;color:#fff;font-family:sans-serif;height:100vh}}
        svg{{width:100%;height:100%}}
        .bar{{fill:#E8FF59;stroke:none;opacity:1}}
        .bar:hover{{opacity:0.4}}
        .axis text{{fill:#fff;font-size:12px}}
        .axis-label{{fill:#fff;font-size:14px;font-weight:bold;text-anchor:middle}}
        .title {{fill:#fff;font-size:18px;font-weight:bold;text-anchor:middle}}
        .axis path,.axis line{{stroke:#fff}}
        
        /* Tooltip styles */
        .tooltip {{
            position: absolute;
            padding: 10px;
            background: rgba(0, 0, 0, 0.8);
            color: #fff;
            border-radius: 5px;
            pointer-events: none;
            font-size: 14px;
            opacity: 0;
            transition: opacity 0.2s;
        }}
    </style>
</head>
<body>
    <svg></svg>
    <div class="tooltip"></div>
    <script>
        const data = {df[["sepal_length","species"]].to_json(orient="records")};
        const svg = d3.select("svg");
        const tooltip = d3.select(".tooltip");
        const margin = {{top: 60, right: 30, bottom: 60, left: 70}};
        
        function draw() {{
            const width = svg.node().clientWidth - margin.left - margin.right;
            const height = svg.node().clientHeight - margin.top - margin.bottom;
            
            svg.selectAll("*").remove();
            
            const g = svg.append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);
            
            // Create histogram bins
            const x = d3.scaleLinear()
                .domain([4, 8])
                .range([0, width]);
                
            const bins = d3.histogram()
                .value(d => d.sepal_length)
                .domain(x.domain())
                .thresholds(x.ticks(15))
                (data);
            
            const y = d3.scaleLinear()
                .domain([0, d3.max(bins, d => d.length)])
                .range([height, 0]);
            
            // Add title
            svg.append("text")
                .attr("class", "title")
                .attr("x", svg.node().clientWidth / 2)
                .attr("y", 30)
                .text("Iris Sepal Length Distribution");
            
            // Add X axis
            g.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${{height}})`)
                .call(d3.axisBottom(x));
            
            // Add Y axis
            g.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y));
            
            // Add X axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("x", width / 2)
                .attr("y", height + 40)
                .text("Sepal Length (cm)");
            
            // Add Y axis label
            g.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -height / 2)
                .attr("y", -40)
                .text("Count");
            
            // Add bars with tooltips
            g.selectAll("rect")
                .data(bins)
                .enter()
                .append("rect")
                .attr("class", "bar")
                .attr("x", d => x(d.x0))
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
                .attr("y", d => y(d.length))
                .attr("height", d => height - y(d.length))
                .on("mouseover", function(event, d) {{
                    const rangeText = `${{d.x0.toFixed(1)}} - ${{d.x1.toFixed(1)}} cm`;
                    tooltip
                        .style("opacity", 1)
                        .html(`
                            <strong>Range:</strong> ${{rangeText}}<br>
                            <strong>Count:</strong> ${{d.length}} flowers
                        `)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                }})
                .on("mouseout", function() {{
                    tooltip.style("opacity", 0);
                }});
        }}
        
        draw();
        window.addEventListener("resize", draw);
    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)