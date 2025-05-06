common = fused.load("https://github.com/fusedio/udfs/tree/1ed3d54/public/common/").utils
pdf_to_html = fused.load("https://github.com/fusedlabs/fusedudfs/tree/7cc3899/html_to_pdf/").pdf_to_html

def pdf_to_obj(pdf):
    return common.html_to_obj(pdf_to_html(pdf))

def udf_to_url(udf_tag):
    """Convert udf:// tag to URL"""
    if not udf_tag.startswith("udf://"):
        return udf_tag
        
    # Strip format specifier if present
    if "::" in udf_tag:
        udf_tag = udf_tag.split("::")[0]
    
    # Parse tag components
    tag_content = udf_tag[6:]
    parts = tag_content.split('/')
    identifier = parts[0].split('?')[0]
    
    # Extract parameters
    params = ""
    if '?' in tag_content:
        params = tag_content.split('?', 1)[1]
    
    # Construct base URL
    is_token = identifier.startswith("fsh_")
    base_url = f"https://www.fused.io/server/v1/realtime-shared/{'' if is_token else 'UDF_'}{identifier}/run"
    
    # Add path and parameters
    if len(parts) >= 4:
        url = f"{base_url}/tiles/{parts[1]}/{parts[2]}/{parts[3]}"
    else:
        url = f"{base_url}/file"
        
    return f"{url}{'?' + params if params else '?'}"

def replace_udf_in_html(content):
    """Replace udf:// references in HTML with formatted content"""
    import re
    import json
    import requests
    import pandas as pd
    import io
    
    # Registry of format handlers
    FORMAT_HANDLERS = {
        "IMAGE": process_image,
        "HTML": process_html,
        "TABLE": process_table,
        "BARCHART": process_barchart
    }
    
    # Updated pattern to catch format tags with JSON-style parameters
    # udf://identifier?params::FORMAT{json_params}
    udf_pattern = r'udf://[^\s<>"\']+(?:::(?:IMAGE|HTML|TABLE|BARCHART)(?:\{[^}]*\})?)?'
    udf_tags = re.findall(udf_pattern, content)
    
    for udf_tag in udf_tags:
        try:
            # Parse format specifier and parameters if present
            format_type = None
            format_params = {}
            base_tag = udf_tag
            
            if "::" in udf_tag:
                # Split the tag at the format specifier
                parts = udf_tag.split("::", 1)
                base_tag = parts[0]
                format_part = parts[1]
                
                # Parse the format type and parameters
                if "{" in format_part:
                    format_type = format_part.split("{", 1)[0]
                    # Extract JSON-style parameters
                    params_str = "{" + format_part.split("{", 1)[1]
                    # Convert to proper JSON by adding quotes to keys
                    params_str = re.sub(r'([{,])\s*([a-zA-Z0-9_]+):', r'\1"\2":', params_str)
                    try:
                        format_params = json.loads(params_str)
                    except json.JSONDecodeError:
                        # Fallback: parse manually if JSON parsing fails
                        params_str = params_str.strip("{}").split(",")
                        for param in params_str:
                            if ":" in param:
                                key, value = param.split(":", 1)
                                key = key.strip().strip('"')
                                value = value.strip().strip('"')
                                format_params[key] = value
                else:
                    format_type = format_part
            
            # Generate base URL
            base_url = udf_to_url(base_tag)
            
            # Process the tag based on format type
            if format_type in FORMAT_HANDLERS:
                replacement = FORMAT_HANDLERS[format_type](base_url, format_params)
            else:
                # Default auto-detection for unknown formats
                replacement = process_default(base_url)
            
        except Exception as e:
            replacement = f"<div style='color:#d32f2f;background:#ffebee;padding:10px'>Error: {str(e)}</div>"

        content = content.replace(udf_tag, replacement)
    
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {font-family:Arial;margin:20px;background:#f5f5f5;line-height:1.6}
            .container {background:white;padding:20px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);max-width:1200px;margin:0 auto}
            h1 {color:#2b5797;margin-top:0} h2 {color:#3b6ea7}
            button {padding:5px 10px;background:#f0f0f0;border:1px solid #ddd;border-radius:4px;cursor:pointer;margin-left:5px}
            button:hover {background:#e0e0e0}
        </style>
    </head>
    <body>
        <div class="container">""" + content + """</div>
    </body>
    </html>
    """
    
    return html_template

def create_iframe(url, height=500):
    """Create responsive iframe with resize controls"""
    import hashlib
    iframe_id = f"udf-frame-{hashlib.md5(url.encode()).hexdigest()[:8]}"
    
    return f"""
    <div style="width:100%;height:{height}px;margin:20px 0">
        <iframe id="{iframe_id}" src="{url}" style="width:100%;height:100%;border:1px solid #ddd;border-radius:4px" 
                sandbox="allow-scripts allow-same-origin" frameborder="0"></iframe>
    </div>
    """


def create_bar_chart(df, x_col=None, y_col=None, title=None, width=800, height=500):
    """
    Create a bar chart from DataFrame data
    
    Args:
        df: Pandas DataFrame with the data
        x_col: Column to use for x-axis categories (if None, uses first column)
        y_col: Column to use for y-axis values (if None, uses second column or first numeric column)
        title: Chart title
        width: Chart width in pixels
        height: Chart height in pixels
        
    Returns:
        HTML string with the embedded chart
    """
    import matplotlib.pyplot as plt
    import io
    import base64
    import numpy as np
    
    # Auto-select columns if not specified
    if x_col is None:
        x_col = df.columns[0]
    
    if y_col is None:
        # Try to find first numeric column that's not the x column
        for col in df.columns:
            if col != x_col and np.issubdtype(df[col].dtype, np.number):
                y_col = col
                break
        # If no numeric column found, use second column
        if y_col is None and len(df.columns) > 1:
            y_col = df.columns[1]
        # Fallback to same as x if only one column
        if y_col is None:
            y_col = x_col
    
    # Auto-generate title if not provided
    if title is None:
        title = f"{y_col} by {x_col}"
    
    # Create the figure and axes
    plt.figure(figsize=(width/100, height/100), dpi=100)
    
    # Generate the bar chart
    ax = df.plot.bar(x=x_col, y=y_col, legend=False)
    plt.title(title)
    plt.tight_layout()
    
    # Rotate x labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    
    # Convert to base64 for embedding in HTML
    img_str = base64.b64encode(buf.read()).decode('utf-8')
    plt.close()
    
    # Create responsive container with the chart
    html = f"""
    <div style="margin: 20px 0;">
        <div style="font-family: Arial; color: #2b5797; font-size: 18px; margin-bottom: 10px;">{title}</div>
        <div style="width: 100%; overflow-x: auto;">
            <img src="data:image/png;base64,{img_str}" alt="{title}" style="max-width: 100%;">
        </div>
    </div>
    """
    
    return html

    


# Format handler functions
def process_image(base_url, params=None):
    """Process IMAGE format"""
    img_url = f"{base_url}&dtype_out_raster=png"
    return f'<img src="{img_url}" alt="UDF Generated Image" style="max-width:100%">'

def process_html(base_url, params=None):
    """Process HTML format"""
    html_url = f"{base_url}&dtype_out_vector=html"
    height = params.get("height", 500) if params else 500
    return create_iframe(html_url, height=height)

def process_table(base_url, params=None):
    """Process TABLE format"""
    import requests
    import pandas as pd
    import io
    
    csv_url = f"{base_url}&dtype_out_vector=csv"
    response = requests.get(csv_url, timeout=30)
    
    if response.status_code == 200:
        df = pd.read_csv(io.StringIO(response.text))
        
        # Extract parameters
        title = None
        max_rows = None
        if params:
            title = params.get("title")
            max_rows = params.get("maxRows")
            if max_rows and isinstance(max_rows, str):
                max_rows = int(max_rows)
        
        # Default title if not provided
        if not title:
            # Extract identifier from base_url
            parts = base_url.split('/')
            identifier = parts[-2] if len(parts) > 2 else "Data"
            title = f"Data from {identifier}"
        
        return format_table_html(df, title=title, max_rows=max_rows)
    else:
        return f"<div style='color:#d32f2f;background:#ffebee;padding:10px;border-radius:4px'>Error: HTTP {response.status_code}</div>"

def process_barchart(base_url, params=None):
    """Process BARCHART format"""
    import requests
    import pandas as pd
    import io
    
    csv_url = f"{base_url}&dtype_out_vector=csv"
    response = requests.get(csv_url, timeout=30)
    
    if response.status_code == 200:
        try:
            # Parse CSV data
            df = pd.read_csv(io.StringIO(response.text))
            
            # Get chart parameters
            x_col = params.get("x") if params else None
            y_col = params.get("y") if params else None
            title = params.get("title") if params else None
            
            # Additional parameters
            width = int(params.get("width", 800)) if params else 800
            height = int(params.get("height", 500)) if params else 500
            
            # Create bar chart
            return create_bar_chart(df, x_col, y_col, title, width, height)
        except Exception as e:
            return f"<div style='color:#d32f2f;background:#ffebee;padding:10px;border-radius:4px'>Error creating chart: {str(e)}</div>"
    else:
        return f"<div style='color:#d32f2f;background:#ffebee;padding:10px;border-radius:4px'>Error: HTTP {response.status_code}</div>"



def format_table_html(df, width="100%", title=None, max_rows=None):
    """Create HTML table from DataFrame with compact styling"""
    import html
    import pandas as pd
    
    # Apply row limit if specified
    df = df.head(max_rows) if max_rows else df
    
    # Compact CSS - using string concatenation instead of f-string for CSS to avoid semicolon issues
    css = """
    <style>
        .table-container {max-width:100%;overflow-x:auto;margin:20px 0}
        .data-table {border-collapse:collapse;width:""" + width + """;font-family:Arial;box-shadow:0 2px 8px rgba(0,0,0,0.1)}
        .data-table th {background:#2b5797;color:white;padding:12px 15px;text-align:left;font-weight:bold;position:sticky;top:0}
        .data-table td {padding:10px 15px;border-bottom:1px solid #ddd;word-break:break-word}
        .data-table tr:nth-child(even) {background:#f2f2f2}
        .data-table tr:hover {background:#e6f7ff}
        .table-title {color:#2b5797;font-size:18px;margin-bottom:10px}
        .geometry-cell {max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .numeric-cell {text-align:right}
    </style>
    """
    
    # Build HTML
    html_parts = [css, '<div class="table-container">']
    
    if title:
        html_parts.append(f'<div class="table-title">{html.escape(title)}</div>')
    
    html_parts.append('<table class="data-table"><thead><tr>')
    
    # Headers
    for col in df.columns:
        html_parts.append(f'<th>{html.escape(str(col))}</th>')
    
    html_parts.append('</tr></thead><tbody>')
    
    # Rows
    for _, row in df.iterrows():
        html_parts.append('<tr>')
        for col, val in row.items():
            val_str = str(val)
            
            # Handle special cell types
            if col == 'geometry' or 'POLYGON' in val_str:
                display_val = val_str[:47] + "..." if len(val_str) > 50 else val_str
                html_parts.append(f'<td class="geometry-cell" title="{html.escape(val_str)}">{html.escape(display_val)}</td>')
            elif pd.api.types.is_numeric_dtype(type(val)):
                html_parts.append(f'<td class="numeric-cell">{html.escape(val_str)}</td>')
            else:
                html_parts.append(f'<td>{html.escape(val_str)}</td>')
        
        html_parts.append('</tr>')
    
    html_parts.append('</tbody></table></div>')
    return ''.join(html_parts)

def process_default(base_url):
    """Default processing for unknown formats - HTML only"""
    html_url = f"{base_url}&dtype_out_vector=html"
    return create_iframe(html_url)