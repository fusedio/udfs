@fused.udf
def udf(csv_url: str = "https://ers.usda.gov/sites/default/files/_laserfiche/DataFiles/48747/Poverty2023.csv?v=53561", 
        attribute_filter: str = None, 
        topo_url: str = None):
    """
    Creates an interactive US county-level poverty choropleth map.
    
    Parameters:
    - csv_url: URL to USDA poverty dataset (defaults to 2023 data)
    - attribute_filter: Specific poverty metric to map (auto-selects if None)
    - topo_url: US counties TopoJSON URL (uses default if None)
    
    Returns: HTML visualization object for display in Fused workbench
    """
    
    # Import required libraries for data processing and web requests
    import pandas as pd
    import io
    import requests
    from jinja2 import Template
    
    # Load Fused's common utilities for HTML rendering
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    # ================================================================
    # DATA LOADING SECTION
    # ================================================================
    
    @fused.cache  # Cache results to avoid re-downloading on multiple runs
    def load_data(url):
        """Downloads and parses CSV data from the provided URL."""
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()  # Raise error if download fails
        df = pd.read_csv(io.StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]  # Remove whitespace from column names
        return df
    
    # Load the poverty dataset and set default TopoJSON source
    df = load_data(csv_url)
    topo_src = topo_url or "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json"
    
    # ================================================================
    # DIAGNOSTIC OUTPUT (Required by Fused workbench)
    # ================================================================
    
    print("=== DataFrame dtypes ===")
    print(df.dtypes)
    print("=== First 5 rows ===")
    print(df.head(5).to_dict(orient="records"))
    
    # ================================================================
    # COLUMN IDENTIFICATION AND VALIDATION
    # ================================================================
    
    # Find FIPS column - different datasets use different column names
    fips_col = None
    if "FIPS_Code" in df.columns:
        fips_col = "FIPS_Code"
    elif "FIPS Code" in df.columns:
        fips_col = "FIPS Code"
    elif "FIPS" in df.columns:
        fips_col = "FIPS"
    elif "FIPStxt" in df.columns:
        fips_col = "FIPStxt"
    
    # Find value column - contains the numeric data to map
    value_col = None
    if "Value" in df.columns:
        value_col = "Value"
    elif "value" in df.columns:
        value_col = "value"
    
    # Validate that required columns were found
    if not fips_col or not value_col:
        raise ValueError(f"Missing required columns. FIPS: {fips_col}, Value: {value_col}")
    
    # ================================================================
    # DATA CLEANING AND STANDARDIZATION
    # ================================================================
    
    # Rename columns to standard names for consistent processing
    df = df.rename(columns={fips_col: "fips", value_col: "value"})
    
    # Clean and standardize data types
    df["fips"] = df["fips"].astype(str).str.zfill(5)  # Ensure 5-digit FIPS codes with leading zeros
    df["value"] = pd.to_numeric(df["value"], errors="coerce")  # Convert to numeric, NaN for invalid values
    
    # CRITICAL: Filter to county-level data only
    # State-level FIPS end with "000" (e.g., "01000" for Alabama)
    # National-level FIPS is "00000"
    # County FIPS end with non-zero digits (e.g., "01001" for Autauga County, AL)
    df = df[~df["fips"].str.endswith("000")]
    print(f"County-level rows: {len(df)}")
    
    # ================================================================
    # ATTRIBUTE SELECTION AND FILTERING
    # ================================================================
    
    # Auto-select attribute if user didn't specify one
    if not attribute_filter:
        # Prefer percentage attributes for better visualization (more intuitive scale)
        percent_attrs = df[df["Attribute"].str.contains("percent", case=False, na=False)]
        if not percent_attrs.empty:
            attribute_filter = percent_attrs["Attribute"].iloc[0]
        else:
            # Fall back to first available attribute
            attribute_filter = df["Attribute"].iloc[0]
    
    # Filter dataset to only the selected attribute
    df_filtered = df[df["Attribute"] == attribute_filter]
    if df_filtered.empty:
        raise ValueError(f"No data for attribute: {attribute_filter}")
    
    print(f"Selected attribute: {attribute_filter}")
    print(f"Counties with data: {len(df_filtered)}")
    
    # ================================================================
    # DOMAIN CALCULATION FOR COLOR SCALE
    # ================================================================
    
    # Extract non-null values for domain calculation
    values = df_filtered["value"].dropna()
    is_percentage = "percent" in attribute_filter.lower()
    
    # Set appropriate domain based on data type
    if is_percentage:
        # For percentages, use natural 0-100 range
        domain_min = 0
        domain_max = 100
    else:
        # For other metrics, use percentile-based range to avoid outliers
        # This prevents a few extreme values from making all other areas look the same
        domain_min = values.quantile(0.05)  # 5th percentile
        domain_max = values.quantile(0.95)  # 95th percentile
        
        # Ensure minimum range for color variation
        if domain_max - domain_min < 1:
            domain_max = domain_min + 1
    
    print(f"Domain: [{domain_min:.1f}, {domain_max:.1f}]")
    
    # ================================================================
    # DATA PREPARATION FOR VISUALIZATION
    # ================================================================
    
    # Convert filtered data to JSON format for JavaScript consumption
    # Only include FIPS and value columns - that's all the map needs
    data_json = df_filtered[["fips", "value"]].to_json(orient="records")
    
    # ================================================================
    # HTML TEMPLATE GENERATION
    # ================================================================
    
    # Create Jinja2 template for HTML/CSS/JavaScript
    # Template variables ({{ }}) will be replaced with actual values
    html_template = Template("""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<!-- Load D3.js for data visualization and TopoJSON for geographic data -->
<script src="https://d3js.org/d3.v7.min.js"></script>
<script src="https://unpkg.com/topojson@3"></script>
<style>
/* Main page styling */
body { margin: 0; font-family: Arial, sans-serif; background: #f5f7fa; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }
.title { font-size: 24px; font-weight: bold; margin-bottom: 10px; color: #2c3e50; }
.subtitle { font-size: 14px; color: #7f8c8d; margin-bottom: 20px; }

/* SVG map styling */
svg { width: 100%; height: auto; border: 1px solid #ddd; border-radius: 8px; background: white; }

/* Tooltip styling for county information on hover */
.tooltip { 
    position: absolute; padding: 10px; background: rgba(0,0,0,0.9); 
    color: white; border-radius: 4px; pointer-events: none; opacity: 0; 
    font-size: 13px; box-shadow: 0 2px 8px rgba(0,0,0,0.2); 
}

/* Legend styling */
.legend { 
    display: flex; align-items: center; margin-top: 15px; 
    font-size: 12px; gap: 5px; 
}
.legend-swatch { 
    width: 30px; height: 15px; border: 1px solid #ccc; border-radius: 2px; 
}
</style>
</head>
<body>
<div class="container">
    <div class="title">{{ title }}</div>
    <div class="subtitle">Interactive county-level poverty data visualization</div>
    <!-- SVG container for the map - viewBox makes it responsive -->
    <svg viewBox="0 0 975 610"></svg>
    <!-- Hidden tooltip that appears on county hover -->
    <div class="tooltip"></div>
    <!-- Color legend showing data range -->
    <div class="legend"></div>
</div>

<script>
// ================================================================
// JAVASCRIPT CONFIGURATION AND DATA SETUP
// ================================================================

// Data passed from Python (JSON format)
const data = {{ data_json | safe }};
const topoUrl = "{{ topo_url }}";

// Configuration object for map dimensions and styling
const config = {
    width: 975,
    height: 610,
    domainMin: {{ domain_min }},
    domainMax: {{ domain_max }},
    colors: d3.schemeBlues[9]  // 9-step blue color scheme
};

// Create lookup map: FIPS code -> value for fast county data retrieval
const dataMap = new Map(data.map(d => [String(d.fips), +d.value]));

// Create quantized color scale: divides domain into 9 equal buckets
const colorScale = d3.scaleQuantize()
    .domain([config.domainMin, config.domainMax])
    .range(config.colors);

// ================================================================
// D3.JS MAP SETUP
// ================================================================

// Select SVG element and set up geographic projection
const svg = d3.select("svg");
const projection = d3.geoAlbersUsa();  // Standard US map projection
const path = d3.geoPath(projection);   // Converts geographic data to SVG paths
const tooltip = d3.select(".tooltip");

// ================================================================
// MAP RENDERING
// ================================================================

// Load US counties TopoJSON data and render the map
d3.json(topoUrl).then(us => {
    // Extract county and state boundary features from TopoJSON
    const counties = topojson.feature(us, us.objects.counties);
    const states = topojson.mesh(us, us.objects.states, (a, b) => a !== b);
    
    // Fit projection to show entire US within SVG dimensions
    projection.fitSize([config.width, config.height], counties);
    
    // ================================================================
    // DRAW COUNTY POLYGONS
    // ================================================================
    
    svg.selectAll("path.county")
        .data(counties.features)
        .join("path")
        .attr("class", "county")
        .attr("d", path)  // Convert geographic coordinates to SVG path
        .attr("fill", d => {
            // Color each county based on its data value
            const value = dataMap.get(String(d.id));
            return value != null ? colorScale(value) : "#f0f0f0";  // Gray for no data
        })
        .attr("stroke", "#fff")      // White borders between counties
        .attr("stroke-width", 0.5)
        // ================================================================
        // INTERACTIVE HOVER EFFECTS
        // ================================================================
        .on("mouseover", (event, d) => {
            // Show tooltip with county name and value
            const value = dataMap.get(String(d.id));
            const name = d.properties?.name || "County " + d.id;
            const valueText = value != null ? value.toFixed(1) + "%" : "No data";
            
            tooltip.style("opacity", 1)
                .html("<strong>" + name + "</strong><br>" + 
                      "{{ attribute_name }}" + ": " + valueText)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 10) + "px");
        })
        .on("mouseout", () => {
            // Hide tooltip when mouse leaves county
            tooltip.style("opacity", 0);
        });
    
    // ================================================================
    // DRAW STATE BOUNDARIES
    // ================================================================
    
    // Add state borders on top of counties for clearer geographic context
    svg.append("path")
        .datum(states)
        .attr("fill", "none")
        .attr("stroke", "#666")      // Dark gray state borders
        .attr("stroke-width", 1)
        .attr("d", path);
    
    // ================================================================
    // CREATE COLOR LEGEND
    // ================================================================
    
    const legend = d3.select(".legend");
    
    // Add legend label
    legend.append("span").text("{{ attribute_name }}: ");
    
    // Add color swatches for each quantile bucket
    config.colors.forEach(color => {
        legend.append("div")
            .attr("class", "legend-swatch")
            .style("background", color);
    });
    
    // Add range labels showing min and max values
    legend.append("span")
        .text(config.domainMin.toFixed(1) + "% - " + config.domainMax.toFixed(1) + "%");
});
</script>
</body>
</html>""")
    
    # ================================================================
    # TEMPLATE RENDERING AND RETURN
    # ================================================================
    
    # Replace template variables with actual values
    rendered = html_template.render(
        title=f"US Poverty Data: {attribute_filter}",
        attribute_name=attribute_filter,  # Used in tooltip and legend
        data_json=data_json,              # County data in JSON format
        topo_url=topo_src,                # TopoJSON URL for map boundaries
        domain_min=domain_min,            # Minimum value for color scale
        domain_max=domain_max             # Maximum value for color scale
    )
    
    # Convert HTML string to Fused HTML object for display
    return common.html_to_obj(rendered)