@fused.udf
def udf(bounds: fused.types.Tile=None):
    import duckdb
    import pandas as pd
    from utils import add_rgb_cmap, CMAP, get_data, run_query

    
    # This is the line that caculates the resolution based on zoom. You can overide the resolution parameter by hard coding it.
    resolution = max(min(int(6 + (bounds.z[0] - 10) * (5/9)), 11), 0)
    print(f"resolution: {resolution}")   
    print(f"zoom: {bounds.z[0]}")

    # We use the bounds to only query what's in view
    bounds = bounds.bounds.values[0] 
   
    # Load Arrow table (this takes about 15 - 30 seconds the first time)
    pplant_table = get_data()
    
    # Run the run_query function
    df = run_query(
        bounds=bounds, 
        pplant_table=pplant_table, 
        resolution=resolution, 
    )
    
    # Apply color mapping directly to the DataFrame
    df = add_rgb_cmap(gdf=df, key_field="primary_fuel", cmap_dict=CMAP)
    
    # Print just these three columns
    columns_to_print = ['cell_id', 'primary_fuel', 'cnt']
    print(df[columns_to_print])
    
    return df
