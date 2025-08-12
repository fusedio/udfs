@fused.udf
def udf(wine_type: str = "red", column: str = "alcohol"):
    """
    Create a Kernel Density Estimation (KDE) plot for wine quality data
    
    Parameters:
    wine_type: str - either "red" or "white" wine dataset
    column: str - which column to visualize (alcohol, pH, density, etc.)
    """
    import pandas as pd
    import numpy as np
    from scipy.stats import gaussian_kde
    
    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    
    @fused.cache
    def load_wine_data(wine_type):
        """Load wine quality dataset based on wine type"""
        if wine_type.lower() == "red":
            url = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
        elif wine_type.lower() == "white":
            url = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv"
        else:
            raise ValueError("wine_type must be either 'red' or 'white'")
        
        df = pd.read_csv(url, sep=';')
        return df
    
    # Load the wine data
    df = load_wine_data(wine_type)
    
    # Get the data for the selected column
    data = df[column].dropna()
    
    # Create KDE
    kde = gaussian_kde(data)
    x_range = np.linspace(data.min(), data.max(), 100)
    y_values = kde(x_range)
    
    # Prepare data for D3
    kde_data = [{"x": float(x), "y": float(y)} for x, y in zip(x_range, y_values)]
    hist_data = []
    
    # Create histogram data
    hist, bins = np.histogram(data, bins=30, density=True)
    for i in range(len(hist)):
        hist_data.append({
            "x0": float(bins[i]),
            "x1": float(bins[i+1]),
            "height": float(hist[i])
        })
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Wine Quality KDE - {column}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            max-width: 900px;
            margin: 0 auto;
        }}
        .axis {{
            font-size: 12px;
        }}
        .axis-label {{
            font-size: 14px;
            font-weight: bold;
        }}
        .bar {{
            fill: #69b3a2;
            opacity: 0.7;
        }}
        .line {{
            fill: none;
            stroke: #ff7f0e;
            stroke-width: 2.5;
        }}
        .title {{
            font-size: 18px;
            font-weight: bold;
            text-align: center;
            margin-bottom: 20px;
        }}
        .subtitle {{
            font-size: 14px;
            color: #666;
            text-align: center;
            margin-bottom: 30px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="title">Kernel Density Estimation</div>
        <div class="subtitle">{wine_type.title()} Wine - {column.replace('_', ' ').title()}</div>
        <div id="chart"></div>
    </div>

    <script>
        // Data
        const kdeData = {kde_data};
        const histData = {hist_data};
        const wineType = "{wine_type}";
        const columnName = "{column}";
        
        // Set dimensions
        const margin = {{top: 20, right: 30, bottom: 50, left: 60}};
        const width = 800 - margin.left - margin.right;
        const height = 400 - margin.top - margin.bottom;
        
        // Create SVG
        const svg = d3.select("#chart")
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${{margin.left}},${{margin.top}})`);
        
        // Set scales
        const x = d3.scaleLinear()
            .domain(d3.extent(kdeData, d => d.x))
            .range([0, width]);
            
        const y = d3.scaleLinear()
            .domain([0, d3.max([...kdeData.map(d => d.y), ...histData.map(d => d.height)]) * 1.1])
            .range([height, 0]);
        
        // Add axes
        svg.append("g")
            .attr("transform", `translate(0,${{height}})`)
            .call(d3.axisBottom(x))
            .append("text")
            .attr("x", width / 2)
            .attr("y", 40)
            .attr("fill", "black")
            .style("text-anchor", "middle")
            .style("font-size", "14px")
            .text(columnName.replace('_', ' ').toLowerCase());
        
        svg.append("g")
            .call(d3.axisLeft(y).tickFormat(d3.format(".2f")))
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", -40)
            .attr("x", -height / 2)
            .attr("fill", "black")
            .style("text-anchor", "middle")
            .style("font-size", "14px")
            .text("Density");
        
        // Add histogram bars
        svg.selectAll(".bar")
            .data(histData)
            .enter().append("rect")
            .attr("class", "bar")
            .attr("x", d => x(d.x0))
            .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
            .attr("y", d => y(d.height))
            .attr("height", d => y(0) - y(d.height));
        
        // Add KDE line
        const line = d3.line()
            .x(d => x(d.x))
            .y(d => y(d.y))
            .curve(d3.curveBasis);
            
        svg.append("path")
            .datum(kdeData)
            .attr("class", "line")
            .attr("d", line);
        
        // Add legend
        const legend = svg.append("g")
            .attr("transform", `translate(${{width - 150}}, 20)`);
            
        legend.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", 15)
            .attr("height", 15)
            .attr("fill", "#69b3a2")
            .attr("opacity", 0.7);
            
        legend.append("text")
            .attr("x", 20)
            .attr("y", 12)
            .text("Histogram")
            .style("font-size", "12px");
            
        legend.append("line")
            .attr("x1", 0)
            .attr("y1", 30)
            .attr("x2", 15)
            .attr("y2", 30)
            .attr("stroke", "#ff7f0e")
            .attr("stroke-width", 2.5);
            
        legend.append("text")
            .attr("x", 20)
            .attr("y", 35)
            .text("KDE")
            .style("font-size", "12px");
    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)