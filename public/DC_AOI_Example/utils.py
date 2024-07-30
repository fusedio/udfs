@fused.cache
def get_arr(bbox, time_of_interest, chip_len, nth_item=None, max_items=30):
    import fused
    import numpy as np
    utils = fused.load('https://github.com/fusedio/udfs/tree/7b97acb/public/Satellite_Greenest_Pixel').utils
    stac_items = utils.search_pc_catalog(bbox=bbox, time_of_interest=time_of_interest, query={"eo:cloud_cover": {"lt": 5}}, collection="sentinel-2-l2a")
    if not stac_items:return
    df_tiff_catalog = utils.create_tiffs_catalog(stac_items, ["B02", "B03", "B04", "B08"])
    if len(df_tiff_catalog)>max_items:
        raise ValueError(f'{len(df_tiff_catalog)} > max number of images ({max_items})')  
    arrs_out = utils.run_pool_tiffs(bbox, df_tiff_catalog, chip_len)
    if nth_item:
        if nth_item>len(df_tiff_catalog):
            raise ValueError(f'{nth_item} > total number of images ({len(df_tiff_catalog)})') 
        df_tiff_catalog = df_tiff_catalog[nth_item:nth_item+1]
        arr=arrs_out[:3].squeeze()
    else:
        arr = utils.get_greenest_pixel(arrs_out, how='median', fillna=True)
    return arr