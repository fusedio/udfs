@fused.udf
def udf(bounds: fused.types.Bounds, year: str = "2016", month: str = "07", period: str ="b", chip_len: int = 1024):
    xy_cols = ['lon', 'lat']
    from utils import get_masked_array, get_da, get_da_bounds, clip_arr
    import pandas as pd

    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    tile = utils.get_tiles(bounds, zoom=zoom)
    
    # Dynamically construct the path based on the year and month
    path = f's3://soldatanasasifglobalifoco2modis1863/Global_SIF_OCO2_MODIS_1863/data/sif_ann_{year}{month}{period}.nc'
    
    # Get the data array using the constructed path
    da = get_da(path, coarsen_factor=1, variable_index=0, xy_cols=xy_cols)
    
    # Clip the array based on the bounding box
    arr_aoi = clip_arr(da.values, 
                       bounds_aoi=bounds,
                       bounds_total=get_da_bounds(da, xy_cols=xy_cols))
    
    # Extract raw SIF values
    sif_values = arr_aoi[arr_aoi != 0]  # Filter out zero values (no data areas)
    
    # Calculate statistics for raw SIF values
    avg_sif = sif_values.mean()
    sum_sif = sif_values.sum()
    count_sif = len(sif_values)

    # Store in DataFrame
    df_sif = pd.DataFrame([{'sum_sif': sum_sif, 'count_sif': count_sif, 'sif': avg_sif}])
    print(df_sif)

    # Convert the array to an image with the specified colormap
    img = (arr_aoi * 255).astype('uint8')
    img = utils.arr_to_plasma(arr_aoi, min_max=(0, 1), colormap="rainbow", include_opacity=False, reverse=False)

    return img




    