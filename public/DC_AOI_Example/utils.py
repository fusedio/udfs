@fused.cache
def get_arr(bbox, time_of_interest, chip_len, nth_item=None, max_items=30):
    import fused
    import numpy as np
    utils = fused.load('https://github.com/fusedio/udfs/tree/bca4c9d/public/Satellite_Greenest_Pixel').utils
    stac_items = utils.search_pc_catalog(bbox=bbox, time_of_interest=time_of_interest, query={"eo:cloud_cover": {"lt": 5}}, collection="sentinel-2-l2a")
    if not stac_items:return
    df_tiff_catalog = utils.create_tiffs_catalog(stac_items, ["B02", "B03", "B04", "B08"])
    if len(df_tiff_catalog)>max_items:
        raise ValueError(f'{len(df_tiff_catalog)} > max number of images ({max_items})')  
    if nth_item:
        if nth_item>len(df_tiff_catalog):
            raise ValueError(f'{nth_item} > total number of images ({len(df_tiff_catalog)})') 
        df_tiff_catalog = df_tiff_catalog[nth_item:nth_item+1]
        arrs_out = utils.run_pool_tiffs(bbox, df_tiff_catalog, chip_len)
        arr=arrs_out.squeeze()
    else:
        arrs_out = utils.run_pool_tiffs(bbox, df_tiff_catalog, chip_len)
        arr = utils.get_greenest_pixel(arrs_out, how='median', fillna=True)
    return arr
    
def rgbi_to_ndvi(arr_rgbi, colormap='RdYlGn', reverse=False):
    import numpy as np
    ndvi = (arr_rgbi[-1] * 1.0 - arr_rgbi[-2] * 1.0) / (
        arr_rgbi[-1] * 1.0 + arr_rgbi[-2] * 1.0
    )
    ndvi = (np.clip(ndvi,0,1)*255).astype('uint8')
    if isinstance(ndvi, np.ma.MaskedArray):
        arr = fused.utils.common.arr_to_plasma(ndvi.data, colormap=colormap, reverse=reverse)
        return np.ma.masked_array(arr, [ndvi.mask]*3)
    else:
        return fused.utils.common.arr_to_plasma(ndvi, colormap=colormap, reverse=reverse)