@fused.udf
def udf(bounds: fused.types.Bounds=None, time_of_interest="2021-09-01/2021-12-30", chip_len:int=512, scale:float=0.1):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)

    # find the tiles with intersecting geom
    gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    gdf_clipped = gdf.dissolve().to_crs(4326).clip(tile)
    gdf_w_bounds = pd.concat([gdf_clipped,tile])
    if len(gdf_w_bounds)<=1:
        print('No bounds is interesecting with the given geometry.')
        return 
        
    # read sentinel data
    udf_sentinel = fused.load('https://github.com/fusedio/udfs/tree/7b98f99/public/DC_AOI_Example/')
    arr = udf_sentinel.utils.get_arr(tile, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len))
    arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils

    # create a geom mask
    geom_mask = utils.gdf_to_mask_arr(gdf_w_bounds, arr.shape[-2:], first_n=1)    
    return np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
