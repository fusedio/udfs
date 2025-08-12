@fused.udf
def udf():
    # =============================================================================
    # CHART TYPE: Simple KDE (Kernel Density Estimation) Plot (D3 Gallery Style)
    # WHEN TO USE: Show distribution shape of continuous numeric variables
    # DATA REQUIREMENTS: Dataset with numeric columns for density analysis
    # FEATURES: Histogram bars with KDE curve overlay, clean D3 aesthetic
    # =============================================================================

    # =============================================================================
    # AI CUSTOMIZATION GUIDE:
    # 1. DATA ADAPTATION: Update load_data() function with your dataset
    # 2. VARIABLE: Change numeric_column for your analysis variable
    # 3. FILTERING: Modify data_filter for subset analysis (optional)
    # 4. STYLING: Change colors and chart appearance
    # 5. TITLES: Update chart title and axis labels in HTML
    #
    # WINE DATASET VARIABLES YOU CAN ANALYZE:
    # - 'alcohol': Alcohol content distribution
    # - 'quality': Wine quality score distribution  
    # - 'pH': Acidity level distribution
    # - 'fixed acidity': Non-volatile acids distribution
    # - 'volatile acidity': Volatile acids distribution
    # - 'residual sugar': Sugar content distribution
    # - 'density': Wine density distribution
    # =============================================================================

    import pandas as pd
    import numpy as np
    from scipy.stats import gaussian_kde
    
    # Load common utilities
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    
    @fused.cache
    def load_data():
        """Load dataset for KDE analysis"""
        # UPDATE_DATA: Replace with your data source
        # Load red wine quality dataset
        url = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
        df = pd.read_csv(url, sep=';')
        
        print(f"Loaded {len(df)} wine samples")
        print("Available numeric columns:", df.select_dtypes(include=[np.number]).columns.tolist())
        return df
    
    # Load and prepare data
    df = load_data()
    
    # Configuration - UPDATE_DATA: Customize for your analysis
    numeric_column = 'alcohol'  # UPDATE_DATA: Your numeric column for KDE analysis
    data_filter = None          # UPDATE_DATA: Optional filter (e.g., df['quality'] > 6)
    
    # Apply filter if specified
    if data_filter is not None:
        df = df[data_filter]
        print(f"After filtering: {len(df)} samples")
    
    # Get the data for KDE analysis
    data = df[numeric_column].dropna()
    print(f"Analyzing {len(data)} values for {numeric_column}")
    print(f"Range: {data.min():.2f} to {data.max():.2f}")
    
    # Create KDE
    kde = gaussian_kde(data)
    x_range = np.linspace(data.min(), data.max(), 100)
    y_values = kde(x_range)
    
    # Prepare KDE data for D3
    kde_data = [{"x": float(x), "y": float(y)} for x, y in zip(x_range, y_values)]
    
    # Create histogram data
    hist, bins = np.histogram(data, bins=30, density=True)
    hist_data = []
    for i in range(len(hist)):
        hist_data.append({
            "x0": float(bins[i]),
            "x1": float(bins[i+1]),
            "height": float(hist[i])
        })
    
    # Convert to JSON
    kde_json = str(kde_data).replace("'", '"')
    hist_json = str(hist_data).replace("'", '"')
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>KDE Distribution Analysis</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f8f9fa;
            box-sizing: border-box;
        }}
        
        .container {{
            max-width: 900px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .chart-container {{
            width: 100%;
            height: 60vh;
            min-height: 400px;
            max-height: 600px;
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        
        svg {{
            max-width: 100%;
            max-height: 100%;
        }}
        
        .title {{
            text-align: center;
            margin-bottom: 30px;
        }}
        
        .title h1 {{
            margin: 0 0 10px 0;
            color: #333;
            font-size: clamp(18px, 4vw, 24px);
        }}
        
        .title p {{
            margin: 0;
            color: #666;
            font-size: clamp(14px, 2.5vw, 16px);
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
            transition: opacity 0.2s ease;
        }}
        
        .bar:hover {{
            opacity: 0.9;
        }}
        
        .kde-line {{
            fill: none;
            stroke: #ff7f0e;
            stroke-width: 2.5;
        }}
        
        .legend {{
            font-size: 12px;
        }}
        
        /* Responsive adjustments */
        @media (max-width: 768px) {{
            .container {{ 
                padding: 15px; 
                margin: 10px;
            }}
            .chart-container {{
                height: 50vh;
                min-height: 350px;
            }}
        }}
        
        @media (max-width: 480px) {{
            body {{ padding: 10px; }}
            .chart-container {{
                height: 45vh;
                min-height: 300px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="title">
            <!-- UPDATE_DATA: Customize title and subtitle -->
            <h1>Red Wine Alcohol Content Distribution</h1>
            <p>Kernel Density Estimation with histogram overlay</p>
        </div>
        
        <div class="chart-container">
            <div id="chart"></div>
        </div>
    </div>

    <script>
        // Responsive dimensions
        function calculateDimensions() {{
            const container = d3.select('.chart-container').node();
            const containerRect = container.getBoundingClientRect();
            
            const containerWidth = containerRect.width - 40;
            const containerHeight = containerRect.height - 40;
            
            const width = Math.max(400, Math.min(800, containerWidth));
            const height = Math.max(300, Math.min(500, containerHeight));
            
            return {{
                width: width,
                height: height,
                margin: {{
                    top: Math.max(20, height * 0.05),
                    right: Math.max(30, width * 0.08),
                    bottom: Math.max(50, height * 0.15),
                    left: Math.max(60, width * 0.1)
                }}
            }};
        }}
        
        function generateChart() {{
            const {{ width, height, margin }} = calculateDimensions();
            const chartWidth = width - margin.left - margin.right;
            const chartHeight = height - margin.top - margin.bottom;
            
            // Clear previous chart
            d3.select("#chart").selectAll("*").remove();
            
            // Data
            const kdeData = {kde_json};
            const histData = {hist_json};
            
            // Create SVG
            const svg = d3.select("#chart")
                .append("svg")
                .attr("width", width)
                .attr("height", height)
                .append("g")
                .attr("transform", `translate(${{margin.left}},${{margin.top}})`);
            
            // Set scales
            const x = d3.scaleLinear()
                .domain(d3.extent(kdeData, d => d.x))
                .range([0, chartWidth]);
                
            const y = d3.scaleLinear()
                .domain([0, d3.max([...kdeData.map(d => d.y), ...histData.map(d => d.height)]) * 1.1])
                .range([chartHeight, 0]);
            
            // Add X axis
            svg.append("g")
                .attr("class", "axis")
                .attr("transform", `translate(0,${{chartHeight}})`)
                .call(d3.axisBottom(x))
                .selectAll(".domain")
                .remove();
            
            // Add Y axis
            svg.append("g")
                .attr("class", "axis")
                .call(d3.axisLeft(y).tickFormat(d3.format(".2f")))
                .selectAll(".domain")
                .remove();
            
            // Add X axis label
            svg.append("text")
                .attr("class", "axis-label")
                .attr("x", chartWidth / 2)
                .attr("y", chartHeight + 35)
                .attr("text-anchor", "middle")
                .style("font-size", Math.max(12, Math.min(16, chartWidth / 40)) + "px")
                .text("Alcohol Content (%)");  // UPDATE_DATA: Change axis label
            
            // Add Y axis label
            svg.append("text")
                .attr("class", "axis-label")
                .attr("transform", "rotate(-90)")
                .attr("x", -chartHeight / 2)
                .attr("y", -35)
                .attr("text-anchor", "middle")
                .style("font-size", Math.max(12, Math.min(16, chartWidth / 40)) + "px")
                .text("Density");
            
            // Add histogram bars
            svg.selectAll(".bar")
                .data(histData)
                .enter()
                .append("rect")
                .attr("class", "bar")
                .attr("x", d => x(d.x0))
                .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 1))
                .attr("y", d => y(d.height))
                .attr("height", d => chartHeight - y(d.height));
            
            // Add KDE line
            const line = d3.line()
                .x(d => x(d.x))
                .y(d => y(d.y))
                .curve(d3.curveBasis);
                
            svg.append("path")
                .datum(kdeData)
                .attr("class", "kde-line")
                .attr("d", line);
            
            // Add legend
            const legendX = Math.max(chartWidth - 120, chartWidth * 0.7);
            const legend = svg.append("g")
                .attr("class", "legend")
                .attr("transform", `translate(${{legendX}}, 20)`);
                
            // Histogram legend
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
                
            // KDE legend
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
        }}
        
        // Generate initial chart
        generateChart();
        
        // Handle window resize
        let resizeTimeout;
        window.addEventListener('resize', function() {{
            clearTimeout(resizeTimeout);
            resizeTimeout = setTimeout(generateChart, 250);
        }});

    </script>
</body>
</html>
"""
    
    return common.html_to_obj(html_content)