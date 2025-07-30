@fused.udf
def udf(path: str = "s3://fused-sample/demo_data/nyc_taxi/yellow_tripdata_2025-05.parquet"):
    import duckdb
    import json
    
    common = fused.load("https://github.com/fusedio/udfs/tree/6e8abb9/public/common/")    
    con = common.duckdb_connect()
    
    df = con.execute(f"""
        SELECT 
            EXTRACT(hour FROM tpep_pickup_datetime) as pickup_hour,
            COUNT(*) as pickup_count
        FROM read_parquet('{path}') 
        GROUP BY EXTRACT(hour FROM tpep_pickup_datetime)
        ORDER BY pickup_hour
    """).df()
    
    data = df.to_dict('records')
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://d3js.org/d3.v7.min.js"></script>
        <style>
            .bar {{ fill: steelblue; }}
            .bar:hover {{ fill: orange; }}
            .axis {{ font: 12px sans-serif; }}
            .axis path, .axis line {{ fill: none; stroke: #000; shape-rendering: crispEdges; }}
            .title {{ font: 16px sans-serif; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div id="chart"></div>
        <script>
            const data = {json.dumps(data)};
            
            const margin = {{top: 40, right: 30, bottom: 70, left: 80}};
            const width = 800 - margin.left - margin.right;
            const height = 400 - margin.top - margin.bottom;
            
            const svg = d3.select("#chart")
                .append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);
            
            const x = d3.scaleBand()
                .range([0, width])
                .domain(data.map(d => d.pickup_hour))
                .padding(0.1);
            
            const y = d3.scaleLinear()
                .domain([0, d3.max(data, d => d.pickup_count)])
                .range([height, 0]);
            
            svg.selectAll(".bar")
                .data(data)
                .enter().append("rect")
                .attr("class", "bar")
                .attr("x", d => x(d.pickup_hour))
                .attr("width", x.bandwidth())
                .attr("y", d => y(d.pickup_count))
                .attr("height", d => height - y(d.pickup_count));
            
            svg.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${{height}})`)
                .call(d3.axisBottom(x));
            
            svg.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y).tickFormat(d3.format(".2s")));
            
            svg.append("text")
                .attr("class", "title")
                .attr("x", width / 2)
                .attr("y", -10)
                .attr("text-anchor", "middle")
                .text("NYC Taxi Pickups by Hour of Day");
            
            svg.append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 0 - margin.left)
                .attr("x", 0 - (height / 2))
                .attr("dy", "1em")
                .style("text-anchor", "middle")
                .text("Number of Pickups");
            
            svg.append("text")
                .attr("transform", `translate(${{width / 2}}, ${{height + margin.bottom - 30}})`)
                .style("text-anchor", "middle")
                .text("Hour of Day");
        </script>
    </body>
    </html>
    """
    
    return common.html_to_obj(html_content)