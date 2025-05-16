@fused.udf
def udf(bounds: fused.types.Bounds=[-77.083,38.804,-76.969,38.983], time_of_interest="2021-12-01/2021-12-30", chip_len:int=256, scale:float=0.1):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np
    from utils import tile_to_df, df_to_hex

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)

    
    # find the tiles with intersecting geom
    gdf = gpd.read_file('s3://fused-asset/data/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    gdf_clipped = gdf.dissolve().to_crs(4326).clip(tile)
    gdf_w_bounds = pd.concat([gdf_clipped,tile])
    if len(gdf_w_bounds)<=1:
        print('No bounds is intersecting with the given geometry.')
        return 
        
    # read sentinel data
    udf_sentinel = fused.load('https://github.com/fusedio/udfs/tree/a1c01c6/public/DC_AOI_Example/')
    arr = udf_sentinel.utils.get_arr(tile, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len), max_items=100)
    arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]
    
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # create a geom mask
    geom_mask = utils.gdf_to_mask_arr(gdf_w_bounds, arr.shape[-2:], first_n=1)    
    arr = np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
    
    # convert arr to xyz dataframe
    df = tile_to_df(tile, arr)
    zoom = tile.z[0]
    h3_size = min(int(3+zoom/1.5),15)
    print(h3_size) 
    data_cols = [f'band{i+1}' for i in range(len(arr))]
    df = df_to_hex(df, data_cols=data_cols, h3_size=h3_size, hex_col='hex', return_avg_lalng=True)

    # calculate stats: mean pixel value for each hex
    mask=1
    for col in data_cols:
        df[f'agg_{col}']=df[f'agg_{col}'].map(lambda x:x.mean())
        mask=mask*df[f'agg_{col}']>0
    df = df[mask]

    # convert the h3_int to h3_hex
    df['hex'] = df['hex'].map(lambda x:hex(x)[2:])
    
    return df




    