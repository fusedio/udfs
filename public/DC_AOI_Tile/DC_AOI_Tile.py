@fused.udf
def udf(bbox: fused.types.TileGDF=None, time_of_interest="2021-09-01/2021-12-30", chip_len:int=512, scale:float=0.1):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np

    # find the tiles with intersecting geom
    gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    gdf_clipped = gdf.dissolve().to_crs(4326).clip(bbox)
    gdf_w_bbox = pd.concat([gdf_clipped,bbox])
    if len(gdf_w_bbox)<=1:
        print('No bbox is interesecting with the given geometry.')
        return 
        
    # read sentinel data
    udf_sentinel = fused.load('https://github.com/fusedio/udfs/tree/7b98f99/public/DC_AOI_Example/')
    arr = udf_sentinel.utils.get_arr(bbox, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len))
    arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils

    # create a geom mask
    geom_mask = utils.gdf_to_mask_arr(gdf_w_bbox, arr.shape[-2:], first_n=1)    
    return np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
