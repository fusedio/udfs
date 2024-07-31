@fused.udf
def udf(time_of_interest= "2021-09-01/2021-12-30",chip_len:int=None, scale:float=0.1,show_ndvi=True):
    import numpy as np   
    import geopandas as gpd 
    from utils import get_arr, rgbi_to_ndvi
    gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    # gdf = gdf.iloc[:1]
    xmin,ymin,xmax,ymax = fused.utils.common.geo_convert(gdf, crs='UTM').total_bounds
    if not chip_len:
        chip_len = int(max(xmax-xmin,ymax-ymin)/10) #considering pixel size of 10m
    if chip_len>3000:
        raise ValueError(f'THe image os too big ({chip_len=}>3000). Consider reducing your area of interest.')  
    print(f'{chip_len=}')
    arr_rgbi = get_arr(gdf, time_of_interest, chip_len, nth_item=None)
    if show_ndvi:
        ndvi = rgbi_to_ndvi(arr_rgbi)
        if isinstance(ndvi, np.ma.MaskedArray):
            arr = fused.utils.common.arr_to_plasma(ndvi.data, colormap='RdYlGn', reverse=False)
            arr = np.ma.masked_array(arr, [ndvi.mask]*3)
        else:
            arr = fused.utils.common.arr_to_plasma(ndvi, colormap='RdYlGn', reverse=False)
    else:
        arr = np.clip(arr_rgbi[:3] * scale, 0, 255).astype("uint8")
    geom_mask = fused.utils.common.gdf_to_mask_arr(gdf, arr.shape[-2:])
    if len(arr.shape)==2:
        arr = np.ma.masked_array(arr, [geom_mask])
    else:
        arr = np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
    return arr, gdf.total_bounds