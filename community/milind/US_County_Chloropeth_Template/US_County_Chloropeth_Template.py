@fused.udf
def udf(csv_url: str = "https://ers.usda.gov/sites/default/files/_laserfiche/DataFiles/48747/Poverty2023.csv?v=53561", attribute_filter: str = None, topo_url: str = None):
    import pandas as pd
    import io
    import requests
    from jinja2 import Template
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    
    @fused.cache
    def load_data(url):
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]
        return df
    
    df = load_data(csv_url)
    topo_src = topo_url or "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json"
    
    print("=== DataFrame dtypes ===")
    print(df.dtypes)
    print("=== First 5 rows ===")
    print(df.head(5).to_dict(orient="records"))
    
    # Find columns - updated to handle actual column names
    fips_col = None
    if "FIPS_Code" in df.columns:
        fips_col = "FIPS_Code"
    elif "FIPS Code" in df.columns:
        fips_col = "FIPS Code"
    elif "FIPS" in df.columns:
        fips_col = "FIPS"
    elif "FIPStxt" in df.columns:
        fips_col = "FIPStxt"
    
    value_col = None
    if "Value" in df.columns:
        value_col = "Value"
    elif "value" in df.columns:
        value_col = "value"
    
    if not fips_col or not value_col:
        raise ValueError(f"Missing columns. FIPS: {fips_col}, Value: {value_col}")
    
    # Rename columns
    df = df.rename(columns={fips_col: "fips", value_col: "value"})
    
    # Clean data
    df["fips"] = df["fips"].astype(str).str.zfill(5)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    
    # Filter county-level data only
    df = df[~df["fips"].str.endswith("000")]
    print(f"County-level rows: {len(df)}")
    
    # Select attribute
    if not attribute_filter:
        percent_attrs = df[df["Attribute"].str.contains("percent", case=False, na=False)]
        if not percent_attrs.empty:
            attribute_filter = percent_attrs["Attribute"].iloc[0]
        else:
            attribute_filter = df["Attribute"].iloc[0]
    
    df_filtered = df[df["Attribute"] == attribute_filter]
    if df_filtered.empty:
        raise ValueError(f"No data for attribute: {attribute_filter}")
    
    print(f"Selected attribute: {attribute_filter}")
    print(f"Counties with data: {len(df_filtered)}")
    
    # Set domain
    values = df_filtered["value"].dropna()
    is_percentage = "percent" in attribute_filter.lower()
    
    if is_percentage:
        domain_min = 0
        domain_max = 100
    else:
        domain_min = values.quantile(0.05)
        domain_max = values.quantile(0.95)
        if domain_max - domain_min < 1:
            domain_max = domain_min + 1
    
    print(f"Domain: [{domain_min:.1f}, {domain_max:.1f}]")
    
    data_json = df_filtered[["fips", "value"]].to_json(orient="records")
    
    html_template = Template("""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script src="https://unpkg.com/topojson@3"></script>
<style>
body { margin: 0; font-family: Arial, sans-serif; background: #f5f7fa; }
.container { max-width: 1200px; margin: 0 auto; padding: 20px; }
.title { font-size: 24px; font-weight: bold; margin-bottom: 10px; color: #2c3e50; }
.subtitle { font-size: 14px; color: #7f8c8d; margin-bottom: 20px; }
svg { width: 100%; height: auto; border: 1px solid #ddd; border-radius: 8px; background: white; }
.tooltip { position: absolute; padding: 10px; background: rgba(0,0,0,0.9); color: white; border-radius: 4px; pointer-events: none; opacity: 0; font-size: 13px; box-shadow: 0 2px 8px rgba(0,0,0,0.2); }
.legend { display: flex; align-items: center; margin-top: 15px; font-size: 12px; gap: 5px; }
.legend-swatch { width: 30px; height: 15px; border: 1px solid #ccc; border-radius: 2px; }
</style>
</head>
<body>
<div class="container">
<div class="title">{{ title }}</div>
<div class="subtitle">Interactive county-level poverty data visualization</div>
<svg viewBox="0 0 975 610"></svg>
<div class="tooltip"></div>
<div class="legend"></div>
</div>
<script>
const data = {{ data_json | safe }};
const topoUrl = "{{ topo_url }}";
const config = {
width: 975,
height: 610,
domainMin: {{ domain_min }},
domainMax: {{ domain_max }},
colors: d3.schemeBlues[9]
};
const dataMap = new Map(data.map(d => [String(d.fips), +d.value]));
const colorScale = d3.scaleQuantize().domain([config.domainMin, config.domainMax]).range(config.colors);
const svg = d3.select("svg");
const projection = d3.geoAlbersUsa();
const path = d3.geoPath(projection);
const tooltip = d3.select(".tooltip");

d3.json(topoUrl).then(us => {
    const counties = topojson.feature(us, us.objects.counties);
    const states = topojson.mesh(us, us.objects.states, (a, b) => a !== b);
    projection.fitSize([config.width, config.height], counties);
    
    svg.selectAll("path.county")
        .data(counties.features)
        .join("path")
        .attr("class", "county")
        .attr("d", path)
        .attr("fill", d => {
            const value = dataMap.get(String(d.id));
            return value != null ? colorScale(value) : "#f0f0f0";
        })
        .attr("stroke", "#fff")
        .attr("stroke-width", 0.5)
        .on("mouseover", (event, d) => {
            const value = dataMap.get(String(d.id));
            const name = d.properties?.name || "County " + d.id;
            const valueText = value != null ? value.toFixed(1) + "%" : "No data";
            tooltip.style("opacity", 1)
                .html("<strong>" + name + "</strong><br>" + "{{ attribute_name }}" + ": " + valueText)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 10) + "px");
        })
        .on("mouseout", () => tooltip.style("opacity", 0));
    
    svg.append("path")
        .datum(states)
        .attr("fill", "none")
        .attr("stroke", "#666")
        .attr("stroke-width", 1)
        .attr("d", path);
    
    const legend = d3.select(".legend");
    legend.append("span").text("{{ attribute_name }}: ");
    config.colors.forEach(color => {
        legend.append("div")
            .attr("class", "legend-swatch")
            .style("background", color);
    });
    legend.append("span")
        .text(config.domainMin.toFixed(1) + "% - " + config.domainMax.toFixed(1) + "%");
});
</script>
</body>
</html>""")
    
    rendered = html_template.render(
        title=f"US Poverty Data: {attribute_filter}",
        attribute_name=attribute_filter,
        data_json=data_json,
        topo_url=topo_src,
        domain_min=domain_min,
        domain_max=domain_max
    )
    
    return common.html_to_obj(rendered)