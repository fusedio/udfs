"""
Fused Chart Factory - A comprehensive chart creation library
Supports both Altair and ECharts chart libraries with various chart types

Usage:
    chart_factory = fused.load("https://github.com/your-username/your-repo/chart_factory.py")
    result = chart_factory.create_chart(chart_type, library, data, params)
"""

import json
import numpy as np
import pandas as pd


def create_chart(chart_type, library, data, params):
    """
    Main factory function to create charts
    
    Args:
        chart_type: Type of chart ('line', 'scatter', 'histogram', 'box', 'pie', 'heatmap', 'card', 'kepler')
        library: Chart library ('echarts' or 'altair')
        data: pandas DataFrame with the data
        params: Dictionary of chart parameters
        
    Returns:
        Chart object or HTML string depending on chart type and library
    """
    if library == "echarts":
        chart_creators = {
            "line": create_echarts_line_chart,
            "scatter": create_echarts_scatter_chart,
            "histogram": create_echarts_histogram_chart,
            "box": create_echarts_box_chart,
            "pie": create_echarts_pie_chart,
            "heatmap": create_echarts_heatmap_chart,
            "card": create_echarts_card_component,
            "kepler": create_kepler_map_component,
        }
        
        if chart_type in chart_creators:
            return chart_creators[chart_type](data, params)
        else:
            raise ValueError(f"Unsupported chart_type for ECharts: {chart_type}")
            
    elif library == "altair":
        import altair as alt
        chart_creators = {
            "line": create_line_chart,
            "scatter": create_scatter_chart,
            "histogram": create_histogram_chart,
            "box": create_box_chart,
            "pie": create_pie_chart,
            "heatmap": create_heatmap_chart,
            "card": create_card_component,
            "kepler": create_kepler_map_component,
        }
        
        if chart_type in chart_creators:
            return chart_creators[chart_type](data, params)
        else:
            raise ValueError(f"Unsupported chart_type for Altair: {chart_type}")
    else:
        raise ValueError(f"Unsupported library: {library}")


def create_chart_html(chart_type, library, data, params):
    """
    Create complete HTML for a chart
    
    Args:
        chart_type: Type of chart
        library: Chart library ('echarts' or 'altair')  
        data: pandas DataFrame
        params: Chart parameters
        
    Returns:
        Complete HTML string ready to display
    """
    title = params.get("title", "Chart")
    height = params.get("height", 400)
    
    if library == "echarts":
        option = create_chart(chart_type, library, data, params)
        
        # Special handling for components that return HTML directly
        if chart_type in ["card", "kepler"]:
            return option
            
        # Convert any timestamps to strings for JSON serialization
        option_json = json.dumps(option, default=str)
            
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>{title}</title>
            <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
            <style>
                #chart-container {{
                    width: 100%;
                    height: {height}px;
                }}
            </style>
        </head>
        <body>
            <div id="chart-container"></div>
            <script>
                // Initialize ECharts instance
                var chartDom = document.getElementById('chart-container');
                var myChart = echarts.init(chartDom);
                
                // Set chart options and render
                myChart.setOption({option_json});

                // Handle window resize
                window.addEventListener('resize', function() {{
                    myChart.resize();
                }});
            </script>
        </body>
        </html>
        """
        return html
        
    elif library == "altair":
        chart = create_chart(chart_type, library, data, params)
        
        # Special case for card component which returns HTML directly
        if chart_type in ["card", "kepler"]:
            return chart
            
        # Apply common properties and return HTML
        html_str = chart.properties(title=title, height=height).interactive().to_html()
        return html_str


# =============================================================================
# ALTAIR CHART CREATORS
# =============================================================================

def create_line_chart(chart_data, params):
    """Create a line chart using Altair."""
    import altair as alt
    x_column = params.get("x_column", "date")
    y_column = params.get("y_column", "value")
    group_column = params.get("group_column", None)

    # Determine x-axis type based on data
    if pd.api.types.is_datetime64_any_dtype(chart_data[x_column]):
        x_type = 'T'
    elif pd.api.types.is_numeric_dtype(chart_data[x_column]):
        x_type = 'Q'
    else:
        x_type = 'N'

    # Base chart configuration
    base = alt.Chart(chart_data).encode(
        tooltip=[
            alt.Tooltip(x_column, title=x_column.capitalize()),
            alt.Tooltip(y_column, title=y_column.capitalize())
        ]
    )

    # Add group column to tooltip if present
    if group_column and group_column in chart_data.columns:
        base = base.encode(
            tooltip=base.tooltip + [alt.Tooltip(group_column, title=group_column.capitalize())]
        )

    # Create the line chart
    line = base.mark_line(
        strokeWidth=2,
        interpolate='monotone'
    ).encode(
        x=alt.X(f"{x_column}:{x_type}", 
                title=x_column.capitalize(),
                axis=alt.Axis(
                    labelAngle=30,
                    labelPadding=12,
                    labelFontSize=11,
                    labelColor="#374151"
                )),
        y=alt.Y(f"{y_column}:Q", 
                title=y_column.capitalize(),
                axis=alt.Axis(
                    labelFontSize=11,
                    labelColor="#374151"
                ))
    )

    # Add points to the line
    points = base.mark_point(
        filled=True,
        size=60,
        strokeWidth=2
    ).encode(
        x=alt.X(f"{x_column}:{x_type}"),
        y=alt.Y(f"{y_column}:Q")
    )

    # Add color encoding if group column is present
    if group_column and group_column in chart_data.columns:
        line = line.encode(
            color=alt.Color(f"{group_column}:N", 
                          title=group_column.capitalize(),
                          legend=alt.Legend(
                              orient='top',
                              titleFontSize=12,
                              labelFontSize=11,
                              labelColor="#374151"
                          ))
        )
        points = points.encode(
            color=alt.Color(f"{group_column}:N")
        )

    # Combine line and points
    chart = (line + points).interactive().properties(
        title=alt.TitleParams(
            text=params.get("title", "Line Chart"),
            fontSize=16,
            color="#111827"
        ),
        padding={"left": 30, "right": 40, "top": 50, "bottom": 30}
    )

    # Add zoom and pan capabilities
    chart = chart.add_selection(
        alt.selection_interval(bind='scales', encodings=['x'])
    )

    return chart


def create_scatter_chart(chart_data, params):
    """Create a scatter plot using Altair."""
    import altair as alt
    x_column = params.get("x_column", "x")
    y_column = params.get("y_column", "y")
    color_column = params.get("color_column", None)
    size_column = params.get("size_column", None)
    opacity = params.get("opacity", 0.7)

    encoding = {
        "x": alt.X(f"{x_column}:Q", title=x_column.capitalize()),
        "y": alt.Y(f"{y_column}:Q", title=y_column.capitalize()),
        "tooltip": [x_column, y_column],
    }

    if color_column and color_column in chart_data.columns:
        encoding["color"] = alt.Color(f"{color_column}:N", title=color_column.capitalize())
        encoding["tooltip"].append(color_column)

    if size_column and size_column in chart_data.columns:
        encoding["size"] = alt.Size(f"{size_column}:Q", title=size_column.capitalize())
        encoding["tooltip"].append(size_column)

    return alt.Chart(chart_data).mark_circle(opacity=opacity).encode(**encoding)


def create_histogram_chart(chart_data, params):
    """Create a histogram using Altair."""
    import altair as alt
    x_column = params.get("x_column", "value")
    bin_count = params.get("bin_count", 20)

    return (
        alt.Chart(chart_data)
        .mark_bar()
        .encode(
            alt.X(f"{x_column}:Q", bin=alt.Bin(maxbins=bin_count), title=x_column.capitalize()),
            y=alt.Y("count()", title="Count"),
            tooltip=[x_column, "count()"],
        )
    )


def create_box_chart(chart_data, params):
    """Create a box plot using Altair."""
    import altair as alt
    category_column = params.get("category_column", "category")
    value_column = params.get("value_column", "value")

    return (
        alt.Chart(chart_data)
        .mark_boxplot()
        .encode(
            x=alt.X(f"{category_column}:N", title=category_column.capitalize()),
            y=alt.Y(f"{value_column}:Q", title=value_column.capitalize()),
            tooltip=[category_column, value_column],
        )
    )


def create_pie_chart(chart_data, params):
    """Create a pie chart using Altair."""
    import altair as alt
    category_column = params.get("category_column", "category")
    value_column = params.get("value_column", "value")

    return (
        alt.Chart(chart_data)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta(f"{value_column}:Q", aggregate="sum"),
            color=alt.Color(f"{category_column}:N", legend=alt.Legend(title=category_column.capitalize())),
            tooltip=[category_column, value_column],
        )
    )


def create_heatmap_chart(chart_data, params):
    """Create a heatmap using Altair."""
    import altair as alt
    x_column = params.get("x_column", "x")
    y_column = params.get("y_column", "y")
    value_column = params.get("value_column", "value")

    return (
        alt.Chart(chart_data)
        .mark_rect()
        .encode(
            x=alt.X(f"{x_column}:N", title=x_column.capitalize()),
            y=alt.Y(f"{y_column}:N", title=y_column.capitalize()),
            color=alt.Color(f"{value_column}:Q", title=value_column.capitalize()),
            tooltip=[x_column, y_column, value_column],
        )
    )


def create_card_component(df, params):
    """Create card components using Altair."""
    # Cards don't use Altair, so we'll create HTML directly
    return create_echarts_card_component(df, params)


# =============================================================================
# ECHARTS CHART CREATORS
# =============================================================================

def create_echarts_line_chart(df, params):
    """Create a line chart using ECharts."""
    x_column = params.get("x_column", "date")
    y_column = params.get("y_column", "value")
    group_column = params.get("group_column", None)

    option = {
        "title": {
            "text": params.get("title", "Line Chart"),
            "left": "center"
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "cross"
            }
        },
        "toolbox": {
            "feature": {
                "dataZoom": {},
                "restore": {},
                "saveAsImage": {}
            }
        },
        "dataZoom": [
            {
                "type": "inside",
                "start": 0,
                "end": 100
            }
        ],
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "top": "15%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": df[x_column].astype(str).tolist(),
            "axisLabel": {
                "interval": 0,
                "rotate": 30,
                "margin": 12,
                "fontSize": 11,
                "color": "#374151"
            },
            "axisTick": {
                "alignWithLabel": True
            }
        },
        "yAxis": {
            "type": "value",
            "axisLabel": {
                "fontSize": 11,
                "color": "#374151"
            }
        }
    }

    if group_column and group_column in df.columns:
        series = []
        for group in df[group_column].unique():
            group_data = df[df[group_column] == group]
            series.append({
                "name": str(group),
                "type": "line",
                "data": group_data[y_column].tolist(),
                "smooth": True,
                "symbol": "circle",
                "symbolSize": 6,
                "lineStyle": {
                    "width": 2
                },
                "itemStyle": {
                    "borderWidth": 2
                }
            })
        option["series"] = series
        option["legend"] = {
            "data": [str(g) for g in df[group_column].unique()],
            "top": 30
        }
    else:
        option["series"] = [{
            "type": "line",
            "data": df[y_column].tolist(),
            "smooth": True,
            "symbol": "circle",
            "symbolSize": 6,
            "lineStyle": {
                "width": 2
            },
            "itemStyle": {
                "borderWidth": 2
            }
        }]

    return option


def create_echarts_scatter_chart(df, params):
    """Create a scatter plot using ECharts."""
    x_column = params.get("x_column", "x")
    y_column = params.get("y_column", "y")
    color_column = params.get("color_column", None)
    size_column = params.get("size_column", None)
    opacity = params.get("opacity", 0.7)

    option = {
        "title": {
            "text": params.get("title", "Scatter Plot"),
            "left": "center"
        },
        "tooltip": {
            "trigger": "item",
            "formatter": "function(params) { return params.data[0] + ', ' + params.data[1]; }"
        },
        "xAxis": {
            "type": "value",
            "name": x_column,
            "nameLocation": "middle",
            "nameGap": 30,
            "axisLabel": {
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "yAxis": {
            "type": "value",
            "name": y_column,
            "nameLocation": "middle",
            "nameGap": 30,
            "axisLabel": {
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "series": [{
            "type": "scatter",
            "data": df[[x_column, y_column]].values.tolist(),
            "symbolSize": 8,
            "itemStyle": {
                "opacity": opacity
            }
        }]
    }

    if color_column and color_column in df.columns:
        option["series"][0]["data"] = df[[x_column, y_column, color_column]].values.tolist()
        option["visualMap"] = {
            "dimension": 2,
            "min": df[color_column].min(),
            "max": df[color_column].max(),
            "calculable": True,
            "orient": "horizontal",
            "left": "center",
            "bottom": 0,
            "textStyle": {
                "color": "#374151"
            }
        }

    if size_column and size_column in df.columns:
        option["series"][0]["data"] = df[[x_column, y_column, color_column, size_column]].values.tolist()
        option["series"][0]["symbolSize"] = "function(data) { return Math.sqrt(data[3]) * 2; }"

    return option


def create_echarts_histogram_chart(df, params):
    """Create a histogram using ECharts."""
    x_column = params.get("x_column", "value")
    bin_count = params.get("bin_count", 20)

    # Calculate histogram bins
    values = df[x_column].values
    min_val = values.min()
    max_val = values.max()
    bin_size = (max_val - min_val) / bin_count
    bins = [min_val + i * bin_size for i in range(bin_count)]
    counts = np.histogram(values, bins=bin_count)[0]

    option = {
        "title": {
            "text": params.get("title", "Histogram"),
            "left": "center"
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "shadow"
            }
        },
        "xAxis": {
            "type": "category",
            "data": [f"{bins[i]:.1f}-{bins[i+1]:.1f}" for i in range(len(bins)-1)],
            "axisLabel": {
                "interval": 0,
                "rotate": 30,
                "margin": 12,
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Count",
            "axisLabel": {
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "series": [{
            "type": "bar",
            "data": counts.tolist(),
            "itemStyle": {
                "color": "#2563EB"
            },
            "barWidth": "80%"
        }]
    }

    return option


def create_echarts_box_chart(df, params):
    """Create a box plot using ECharts."""
    category_column = params.get("category_column", "category")
    value_column = params.get("value_column", "value")

    # Convert value column to numeric if it's not already
    df[value_column] = pd.to_numeric(df[value_column], errors='coerce')
    
    # Remove any NaN values
    df = df.dropna(subset=[value_column])

    categories = df[category_column].unique()
    box_data = []

    for cat in categories:
        values = df[df[category_column] == cat][value_column].values
        if len(values) > 0:  # Only process if we have values
            box_data.append([
                float(np.min(values)),
                float(np.percentile(values, 25)),
                float(np.percentile(values, 50)),
                float(np.percentile(values, 75)),
                float(np.max(values))
            ])

    option = {
        "title": {
            "text": params.get("title", "Box Plot"),
            "left": "center"
        },
        "tooltip": {
            "trigger": "item",
            "axisPointer": {
                "type": "shadow"
            }
        },
        "xAxis": {
            "type": "category",
            "data": [str(cat) for cat in categories],
            "axisLabel": {
                "interval": 0,
                "rotate": 30,
                "margin": 12,
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "yAxis": {
            "type": "value",
            "name": value_column,
            "axisLabel": {
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "series": [{
            "type": "boxplot",
            "data": box_data,
            "itemStyle": {
                "color": "#2563EB",
                "borderColor": "#1E40AF"
            },
            "emphasis": {
                "itemStyle": {
                    "color": "#1D4ED8",
                    "borderColor": "#1E3A8A"
                }
            }
        }]
    }

    return option


def create_echarts_pie_chart(df, params):
    """Create a pie chart using ECharts."""
    category_column = params.get("category_column", "category")
    value_column = params.get("value_column", "value")

    pie_data = df.groupby(category_column)[value_column].sum().reset_index()
    pie_data = pie_data.rename(columns={category_column: "name", value_column: "value"})

    option = {
        "title": {
            "text": params.get("title", "Pie Chart"),
            "left": "center"
        },
        "tooltip": {
            "trigger": "item",
            "formatter": "{b}: {c} ({d}%)"
        },
        "series": [{
            "type": "pie",
            "radius": ["40%", "70%"],
            "data": pie_data.to_dict("records"),
            "label": {
                "show": True,
                "formatter": "{b}: {c} ({d}%)",
                "fontSize": 11,
                "color": "#374151"
            },
            "emphasis": {
                "itemStyle": {
                    "shadowBlur": 10,
                    "shadowOffsetX": 0,
                    "shadowColor": "rgba(0, 0, 0, 0.5)"
                }
            }
        }]
    }

    return option


def create_echarts_heatmap_chart(df, params):
    """Create a heatmap using ECharts."""
    x_column = params.get("x_column", "x")
    y_column = params.get("y_column", "y")
    value_column = params.get("value_column", "value")

    x_categories = df[x_column].unique()
    y_categories = df[y_column].unique()
    heatmap_data = []

    for x_cat in x_categories:
        for y_cat in y_categories:
            value = df[(df[x_column] == x_cat) & (df[y_column] == y_cat)][value_column].values
            if len(value) > 0:
                heatmap_data.append([x_cat, y_cat, value[0]])

    option = {
        "title": {
            "text": params.get("title", "Heatmap"),
            "left": "center"
        },
        "tooltip": {
            "position": "top",
            "formatter": "function(params) { return params.data[0] + ', ' + params.data[1] + ': ' + params.data[2]; }"
        },
        "xAxis": {
            "type": "category",
            "data": [str(x) for x in x_categories],
            "splitArea": {
                "show": True
            },
            "axisLabel": {
                "interval": 0,
                "rotate": 30,
                "margin": 12,
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "yAxis": {
            "type": "category",
            "data": [str(y) for y in y_categories],
            "splitArea": {
                "show": True
            },
            "axisLabel": {
                "fontSize": 11,
                "color": "#374151"
            }
        },
        "visualMap": {
            "min": min([d[2] for d in heatmap_data]),
            "max": max([d[2] for d in heatmap_data]),
            "calculable": True,
            "orient": "horizontal",
            "left": "center",
            "bottom": 0,
            "textStyle": {
                "color": "#374151"
            }
        },
        "series": [{
            "type": "heatmap",
            "data": heatmap_data,
            "label": {
                "show": False
            },
            "emphasis": {
                "itemStyle": {
                    "shadowBlur": 10,
                    "shadowColor": "rgba(0, 0, 0, 0.5)"
                }
            }
        }]
    }

    return option


def create_echarts_card_component(df, params):
    """Create card components."""
    import html
    
    # Default parameters
    color = params.get("color", "minimal")  # Changed default to minimal
    title = params.get("title", "Statistics")
    layout = params.get("layout", "horizontal")  # horizontal or vertical
    
    # Validate color (allow blue, green, or minimal/white)
    if color.lower() not in ["blue", "green", "minimal", "white"]:
        color = "minimal"  # Default to minimal for invalid colors
    
    # Map "white" to "minimal" for user convenience (both terms work)
    if color.lower() == "white":
        color = "minimal"
    
    # Validate layout
    if layout.lower() not in ["horizontal", "vertical"]:
        layout = "horizontal"  # Default to horizontal for invalid layouts
    
    # Define color schemes
    color_schemes = {
        "blue": {
            "bg": "#e8f0fe",
            "border": "#2b5797",
            "title": "#2b5797",
            "text": "#333333"
        },
        "green": {
            "bg": "#e6f4ea",
            "border": "#34a853",
            "title": "#34a853",
            "text": "#333333"
        },
        "minimal": {
            "bg": "#ffffff",
            "border": "#e0e0e0",
            "title": "#333333",
            "text": "#666666"
        }
    }
    
    # Get selected color scheme
    scheme = color_schemes[color.lower()]
    
    # Set flex direction based on layout
    flex_direction = "row" if layout.lower() == "horizontal" else "column"
    card_width = "150px" if layout.lower() == "horizontal" else "100%"
    
    # Start building HTML for cards
    html_parts = []
    
    # Add container for cards with flex layout
    html_parts.append(f"""
    <div style="margin: 20px 0;">
        <div style="font-family: Arial, sans-serif; color: {scheme['title']}; font-size: 18px; margin-bottom: 10px; font-weight: 500;">{html.escape(title)}</div>
        <div style="display: flex; flex-direction: {flex_direction}; flex-wrap: wrap; gap: 15px;">
    """)
    
    # Handle different DataFrame formats
    if df.shape[1] == 2:
        # Format 1: key-value pairs
        for _, row in df.iterrows():
            key = row.iloc[0] if pd.notna(row.iloc[0]) else "N/A"
            value = row.iloc[1] if pd.notna(row.iloc[1]) else "N/A"
            
            # Format numbers nicely
            if isinstance(value, (int, float)):
                if value >= 1000000:
                    formatted_value = f"{value/1000000:.2f}M"
                elif value >= 1000:
                    formatted_value = f"{value/1000:.2f}K"
                else:
                    formatted_value = f"{value:,.2f}"
            else:
                formatted_value = str(value)
            
            # Create card
            html_parts.append(f"""
            <div style="flex: {1 if layout.lower() == 'horizontal' else 0}; min-width: {card_width}; background: {scheme['bg']}; border: 1px solid {scheme['border']}; 
                       border-radius: 8px; padding: 15px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
                <div style="font-size: 14px; color: {scheme['text']}; font-family: Arial, sans-serif;">{html.escape(str(key))}</div>
                <div style="font-size: 24px; font-weight: 600; color: {scheme['title']}; margin-top: 5px; font-family: Arial, sans-serif;">
                    {html.escape(formatted_value)}
                </div>
            </div>
            """)
    else:
        # Format 2: Each row is a separate card, columns are stats
        for _, row in df.iterrows():
            # Create card for each row with multiple data points
            html_parts.append(f"""
            <div style="flex: {1 if layout.lower() == 'horizontal' else 0}; min-width: {card_width}; background: {scheme['bg']}; border: 1px solid {scheme['border']}; 
                       border-radius: 8px; padding: 15px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
            """)
            
            # Add each column as a stat
            for col, val in row.items():
                # Skip any columns that appear to be index columns
                if col == '' or col == 'index' or col == 'Unnamed: 0' or col == df.index.name:
                    continue
                    
                if pd.notna(val):
                    # Format numbers
                    if isinstance(val, (int, float)):
                        if val >= 1000000:
                            formatted_val = f"{val/1000000:.2f}M"
                        elif val >= 1000:
                            formatted_val = f"{val/1000:.2f}K"
                        else:
                            formatted_val = f"{val:,.2f}"
                    else:
                        formatted_val = str(val)
                    
                    html_parts.append(f"""
                    <div style="margin-bottom: 10px;">
                        <div style="font-size: 14px; color: {scheme['text']}; font-family: Arial, sans-serif;">{html.escape(str(col))}</div>
                        <div style="font-size: 20px; font-weight: 600; color: {scheme['title']}; margin-top: 2px; font-family: Arial, sans-serif;">
                            {html.escape(formatted_val)}
                        </div>
                    </div>
                    """)
            
            html_parts.append("</div>")
    
    # Close container
    html_parts.append("</div></div>")
    
    return ''.join(html_parts)


def create_kepler_map_component(df, params):
    """Create a simple Kepler map using iframe to load Fused UDF directly."""
    import json
    import urllib.parse
    
    # Default parameters
    latlng = params.get('latlng', [40.73, -74.00]) 
    readOnly = params.get('readOnly', False)
    title = params.get('title', 'Kepler Map')
    height = params.get('height', 400)
    buffer_distance = params.get('buffer_distance', 1000)
    
    # Use the token provided by the user or default
    kepler_token = params.get('kepler_token', 'fsh_6t3VDs74eL0rQ0ocFzPH7H')
    
    # Build the Fused URL with parameters
    base_url = f"https://staging.fused.io/server/v1/realtime-shared/{kepler_token}/run/file"
    
    # Create URL parameters
    url_params = {
        'latlng': json.dumps(latlng),
        'readOnly': json.dumps(readOnly),
        'buffer_distance': buffer_distance
    }
    
    # Encode parameters
    query_string = urllib.parse.urlencode(url_params)
    full_url = f"{base_url}?{query_string}"
    
    # Create simple iframe HTML
    html = f'''
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8"/>
        <title>{title}</title>
        <style>
          body {{
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
            background: #f5f5f5;
          }}
          .header {{
            padding: 10px 20px;
            background: white;
            border-bottom: 1px solid #ddd;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
          }}
          .header h3 {{
            margin: 0;
            color: #333;
            font-size: 18px;
          }}
          .iframe-container {{
            width: 100%;
            height: {height - 60}px;
            background: white;
            position: relative;
          }}
          .iframe-container iframe {{
            width: 100%;
            height: 100%;
            border: none;
          }}
          .loading {{
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #f9f9f9;
            color: #666;
            font-size: 16px;
            z-index: 10;
          }}
          .loading.hidden {{
            display: none;
          }}
          .error {{
            color: #c33;
            text-align: center;
            padding: 20px;
          }}
          .fallback {{
            margin-top: 10px;
            font-size: 14px;
          }}
          .fallback a {{
            color: #2563eb;
            text-decoration: none;
          }}
          .fallback a:hover {{
            text-decoration: underline;
          }}
        </style>
      </head>
      <body>
        <div class="header">
          <h3>🗺️ {title}</h3>
        </div>
        <div class="iframe-container">
          <div id="loading" class="loading">
            <div>
              <div style="margin-bottom: 10px;">🗺️ Loading Kepler Map...</div>
              <div style="font-size: 12px; color: #888;">Token: {kepler_token}</div>
            </div>
          </div>
          <iframe 
            id="kepler-iframe" 
            src="{full_url}"
            title="Kepler Map"
            onload="hideLoading()"
            onerror="showError()"
          ></iframe>
        </div>
        
        <script>
          function hideLoading() {{
            const loading = document.getElementById('loading');
            if (loading) {{
              loading.classList.add('hidden');
            }}
          }}
          
          function showError() {{
            const loading = document.getElementById('loading');
            if (loading) {{
              loading.innerHTML = `
                <div class="error">
                  ❌ Failed to load Kepler map
                  <div class="fallback">
                    <a href="{full_url}" target="_blank">🔗 Open in new tab</a>
                  </div>
                </div>
              `;
            }}
          }}
          
          // Fallback timeout
          setTimeout(function() {{
            const loading = document.getElementById('loading');
            const iframe = document.getElementById('kepler-iframe');
            if (loading && !loading.classList.contains('hidden')) {{
              loading.innerHTML = `
                <div class="error">
                  ⏱️ Map loading timeout
                  <div class="fallback">
                    <a href="{full_url}" target="_blank">🔗 Open in new tab</a>
                  </div>
                </div>
              `;
            }}
          }}, 15000); // 15 second timeout
          
          // Handle iframe communication if needed
          window.addEventListener('message', function(event) {{
            if (event.data && event.data.type === 'kepler-loaded') {{
              hideLoading();
            }}
          }});
        </script>
      </body>
    </html>
    '''
    
    return html


def get_kepler_html_template():
    """Get a basic Kepler.gl HTML template - simplified version"""
    return '''
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8"/>
        <title>Kepler.gl Map</title>
        <link rel="stylesheet" href="https://d1a3f4spazzrp4.cloudfront.net/kepler.gl/uber-fonts/4.0.0/superfine.css">
        <link href="https://unpkg.com/maplibre-gl@^3/dist/maplibre-gl.css" rel="stylesheet">
        <script src="https://unpkg.com/react@18.3.1/umd/react.production.min.js" crossorigin></script>
        <script src="https://unpkg.com/react-dom@18.3.1/umd/react-dom.production.min.js" crossorigin></script>
        <script src="https://unpkg.com/redux@4.2.1/dist/redux.js" crossorigin></script>
        <script src="https://unpkg.com/react-redux@8.1.2/dist/react-redux.min.js" crossorigin></script>
        <script src="https://unpkg.com/styled-components@4.4.1/dist/styled-components.min.js" crossorigin></script>
        <script src="https://unpkg.com/kepler.gl@3.1.0-alpha.3/umd/keplergl.min.js" crossorigin></script>
        <style type="text/css">
          body {{margin: 0; padding: 0; overflow: hidden;}}
        </style>
        <script>
          const MAPBOX_TOKEN = 'pk.eyJ1IjoidWNmLW1hcGJveCIsImEiOiJjbDBiYzlveHgwdnF0M2NtZzUzZWZuNWZ4In0.l9J8ptz3MKwaU9I4PtCcig';
        </script>
      </head>
      <body>
        <div id="app"></div>
        <script>
          // Basic Kepler implementation - placeholder for template
          console.log("Kepler template loaded");
        </script>
      </body>
    </html>
    '''


# =============================================================================
# CHART PARAMETER DEFINITIONS
# =============================================================================

def get_chart_params():
    """Get chart parameters configuration."""
    return {
        "line": {
            "x_column": "Date/time column (str, default: 'date')",
            "y_column": "Value column (str, default: 'value')",
            "group_column": "Optional column for grouping/coloring (str, default: None)",
        },
        "scatter": {
            "x_column": "X-axis column (str, default: 'x')",
            "y_column": "Y-axis column (str, default: 'y')",
            "color_column": "Optional column for coloring points (str, default: None)",
            "size_column": "Optional column for point sizes (str, default: None)",
            "opacity": "Point opacity (float, default: 0.7)",
        },
        "histogram": {
            "x_column": "Numeric column to bin (str, default: 'value')",
            "bin_count": "Number of bins (int, default: 20)",
        },
        "box": {
            "category_column": "Category for x-axis (str, default: 'category')",
            "value_column": "Numeric values for y-axis (str, default: 'value')",
        },
        "pie": {
            "category_column": "Categories for pie segments (str, default: 'category')",
            "value_column": "Values determining segment size (str, default: 'value')",
        },
        "heatmap": {
            "x_column": "X-axis categories (str, default: 'x')",
            "y_column": "Y-axis categories (str, default: 'y')",
            "value_column": "Values for color intensity (str, default: 'value')",
        },
        "card": {
            "title": "Title for the card component (str, default: 'Statistics')",
            "color": "Color scheme for cards (str, default: 'minimal', options: 'minimal', 'white', 'blue', 'green')",
            "layout": "Layout direction for cards (str, default: 'horizontal', options: 'horizontal', 'vertical')",
        },
        "kepler": {
            "latlng": "Center latitude/longitude as [lat, lng] (list, default: [40.73, -74.00])",
            "buffer_distance": "Buffer distance in meters (float, default: 500)",
            "readOnly": "Read-only mode for map (bool, default: False)",
            "latitude_col": "Column name for latitude values (str, optional)",
            "longitude_col": "Column name for longitude values (str, optional)",
        },
    }


def get_common_params():
    """Get common chart parameters."""
    return {
        "title": "Chart title (str, default: 'Chart')",
        "height": "Chart height in pixels (int, default: 400)",
        "library": "Chart library to use (str, default: 'echarts', options: 'echarts', 'altair')",
    } 
