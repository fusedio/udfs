import fused
import pandas as pd
import numpy as np

@fused.udf
def udf(
    bounds: fused.types.Bounds = [-74.014, 40.700, -74.000, 40.717],
    chart_type: str = "line",
    library: str = "echarts"
):
    """
    Simple chart creation UDF using the chart factory
    
    Args:
        bounds: Geographic bounds (not used in this example)
        chart_type: Type of chart to create ('line', 'pie', 'scatter', etc.)
        library: Chart library to use ('echarts' or 'altair')
    """
    import utils
    
    # Create some sample data
    sample_data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=10, freq='D'),
        'sales': np.random.randint(100, 1000, 10),
        'category': np.random.choice(['A', 'B', 'C'], 10),
        'profit': np.random.uniform(10.0, 100.0, 10),
    })
    
    # Set up chart parameters
    if chart_type == "line":
        params = {
            'title': 'Sales Over Time',
            'x_column': 'date',
            'y_column': 'sales',
            'height': 400
        }
    elif chart_type == "pie":
        params = {
            'title': 'Sales by Category', 
            'category_column': 'category',
            'value_column': 'sales',
            'height': 400
        }
    elif chart_type == "scatter":
        params = {
            'title': 'Sales vs Profit',
            'x_column': 'sales',
            'y_column': 'profit',
            'height': 400
        }
    elif chart_type == "histogram":
        params = {
            'title': 'Sales Distribution',
            'x_column': 'sales',
            'bin_count': 20,
            'height': 400
        }
    elif chart_type == "card":
        params = {
            'title': 'Sales Summary',
            'color': 'blue',
            'layout': 'horizontal',
            'height': 400
        }
    else:
        # Default parameters for other chart types
        params = {
            'title': f'{chart_type.capitalize()} Chart',
            'x_column': 'sales',
            'y_column': 'profit',
            'height': 400
        }
    
    # Create the chart using the factory functions with utils namespace
    if library == "echarts":
        # Create complete HTML using the chart factory
        html_result = utils.create_chart_html(chart_type, library, sample_data, params)
        
        # Load common utils for HTML object conversion
        common = fused.load("https://github.com/fusedio/udfs/tree/1ed3d54/public/common/").utils
        return common.html_to_obj(html_result)
        
    else:
        html_result = utils.create_chart_html(chart_type, library, sample_data, params)

        common = fused.load("https://github.com/fusedio/udfs/tree/1ed3d54/public/common/").utils
        return common.html_to_obj(html_result) 