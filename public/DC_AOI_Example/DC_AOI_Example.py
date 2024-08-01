@fused.udf
def udf(time_of_interest= "2021-09-01/2021-12-30",chip_len:int=None, scale:float=0.1,show_ndvi=True):
    import numpy as np   
    import geopandas as gpd 
    from utils import get_arr, rgbi_to_ndvi

    # load the geometry
    gdf = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    if not chip_len:
        xmin,ymin,xmax,ymax = fused.utils.common.geo_convert(gdf, crs='UTM').total_bounds
        chip_len = int(max(xmax-xmin,ymax-ymin)/10) #considering pixel size of 10m
    
    # check to make sure the image is not too big
    print(f'{chip_len=}')
    if chip_len>3000:
        raise ValueError(f'THe image is too big ({chip_len=}>3000). Consider reducing your area of interest.')  

    # get the data
    arr_rgbi = get_arr(gdf, time_of_interest, output_shape=(chip_len, chip_len), nth_item=None)

    # Scale the values for visualization purpose
    if show_ndvi:
        arr = rgbi_to_ndvi(arr_rgbi, colormap='RdYlGn', reverse=False)
    else:
        arr = np.clip(arr_rgbi[:3] * scale, 0, 255).astype("uint8")
    

    # create clip using geom and convert to masked array
    geom_mask = fused.utils.common.gdf_to_mask_arr(gdf, arr.shape[-2:])
    if len(arr.shape)==2:
        arr = np.ma.masked_array(arr, [geom_mask])
    else:
        arr = np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
    return arr, gdf.total_bounds