# need to fix the gap issue >> why read_tiff is not working?
#First get NOW then should I calculate childerens and then geo up to parents?
#then use DSM_tile_wip since that one need tiff reader
@fused.udf  
def udf(bbox=None, collections = ['cop-dem-glo-30'], 
        date_range = '2021', n_mosaic=1, overview_level=0):
    from utils import mosaic_tiff, arr_to_plasma, stac_search_bbox
    import numpy as np
    df = stac_search_bbox(bbox, 
                  stac_url="https://earth-search.aws.element84.com/v1", 
                  datetime=date_range,
                  collections=collections)
    arr = mosaic_tiff(bbox, 
                  tiff_list = df['tiff_url'].iloc[:n_mosaic], 
                  reduce_function = lambda x:np.max(x, axis=0),
                  overview_level = overview_level)
    return arr_to_plasma(arr)
    # return df[['geometry','tiff_url','datetime','proj:epsg']]