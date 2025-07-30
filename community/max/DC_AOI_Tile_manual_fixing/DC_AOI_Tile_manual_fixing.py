@fused.udf
def udf(bounds: fused.types.Bounds=[-77.083,38.849,-76.969,38.938], time_of_interest="2021-09-01/2021-12-30", chip_len:int=512, scale:float=0.1):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/6e8abb9/public/common/")
    tile = common.get_tiles(bounds, target_num_tiles = 4, clip=True)

    # find the tiles with intersecting geom
    gdf = gpd.read_file('s3://fused-asset/data/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    gdf_clipped = gdf.dissolve().to_crs(4326).clip(tile)
    gdf_w_bounds = pd.concat([gdf_clipped,tile])
    if len(gdf_w_bounds)<=1:
        print('No bounds is interesecting with the given geometry.')
        return 
        
    # read sentinel data
    udf_sentinel = fused.load('https://github.com/fusedio/udfs/tree/7b98f99/public/DC_AOI_Example/')
    arr = udf_sentinel.common.get_arr(tile[:30], time_of_interest=time_of_interest, output_shape=(chip_len, chip_len))
    arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]

    # create a geom mask
    geom_mask = common.gdf_to_mask_arr(gdf_w_bounds, arr.shape[-2:], first_n=1)    
    return np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
