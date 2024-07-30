@fused.udf
def udf(time_of_interest= "2021-09-01/2021-12-30",chip_len:int=None, scale:float=0.1):
    import numpy as np   
    import geopandas as gpd 
    from utils import get_arr
    gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    # gdf = gdf.iloc[:1]
    xmin,ymin,xmax,ymax = fused.utils.common.geo_convert(gdf, crs='UTM').total_bounds
    if not chip_len:
        chip_len = int(max(xmax-xmin,ymax-ymin)/10) #considering pixel size of 10m
    if chip_len>3000:
        raise ValueError(f'THe image os too big ({chip_len=}>3000). Consider reducing your area of interest.')  
    print(chip_len)
    arr = get_arr(gdf, time_of_interest, chip_len, nth_item=None)
    arr = np.clip(arr * scale, 0, 255).astype("uint8")
    geom_mask = fused.utils.common.gdf_to_mask_arr(gdf, arr.shape[-2:])
    arr[:, geom_mask]=0
    return arr, gdf.total_bounds