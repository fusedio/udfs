import fused
import numpy as np
import palettable

utils = fused.load(
    "https://github.com/fusedio/udfs/tree/2b25cb3/public/common/"
).utils

@fused.cache
def get_arr(
    bbox,
    time_of_interest,
    output_shape,
    nth_item=None,
    max_items=30
):

    greenest_example_utils = fused.load('https://github.com/fusedio/udfs/tree/9bfb5d0/public/Satellite_Greenest_Pixel').utils

    stac_items = greenest_example_utils.search_pc_catalog(
        bbox=bbox,
        time_of_interest=time_of_interest,
        query={"eo:cloud_cover": {"lt": 5}},
        collection="sentinel-2-l2a"
    )
    if not stac_items: return
    df_tiff_catalog = greenest_example_utils.create_tiffs_catalog(stac_items, ["B02", "B03", "B04", "B08"])
    if len(df_tiff_catalog) > max_items:
        raise ValueError(f'{len(df_tiff_catalog)} > max number of images ({max_items})')  
    if nth_item:
        if nth_item > len(df_tiff_catalog):
            raise ValueError(f'{nth_item} > total number of images ({len(df_tiff_catalog)})') 
        df_tiff_catalog = df_tiff_catalog[nth_item:nth_item + 1]
        arrs_out = greenest_example_utils.run_pool_tiffs(bbox, df_tiff_catalog, output_shape)
        arr = arrs_out.squeeze()
    else:
        arrs_out = greenest_example_utils.run_pool_tiffs(bbox, df_tiff_catalog, output_shape)
        arr = greenest_example_utils.get_greenest_pixel(arrs_out, how='median', fillna=True)
    return arr
    
def rgbi_to_ndvi(arr_rgbi):    
    ndvi = (arr_rgbi[-1] * 1.0 - arr_rgbi[-2] * 1.0) / (
        arr_rgbi[-1] * 1.0 + arr_rgbi[-2] * 1.0
    )
    rgb_image = utils.visualize(
        data=ndvi,
        min=0,
        max=1,
        colormap=['black', 'green']
    )
    return rgb_image

        