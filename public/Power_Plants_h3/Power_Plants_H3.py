@fused.udf
def udf(bounds: fused.types.Bounds=[-49.108,-35.500,72.922,67.364]):
    import duckdb
    import pandas as pd
    from utils import add_rgb_cmap, CMAP, get_data, run_query

    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = utils.get_tiles(bounds, clip=True)
    zoom = tile.iloc[0].z
    
    # This is the line that caculates the resolution based on zoom. You can overide the resolution parameter by hard coding it.
    resolution = max(min(int(6 + (zoom - 10) * (5/9)), 11), 0)
    print(f"resolution: {resolution}")   
    print(f"zoom: {zoom}")

    # We use the bounds to only query what's in view
    tile = tile.bounds.values[0]
   
    # Load Arrow table (this takes about 15 - 30 seconds the first time)
    pplant_table = get_data()
    
    # Run the run_query function
    df = run_query(
        bounds=tile,
        pplant_table=pplant_table, 
        resolution=resolution, 
    )
    
    # Apply color mapping directly to the DataFrame
    df = add_rgb_cmap(gdf=df, key_field="primary_fuel", cmap_dict=CMAP)
    
    # Print just these three columns
    columns_to_print = ['cell_id', 'primary_fuel', 'cnt']
    print(df[columns_to_print])
    
    return df
