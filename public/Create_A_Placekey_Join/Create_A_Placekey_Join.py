@fused.udf
def udf(bbox: fused.types.Tile,
        dataset1: str = 'dataset name here',
        dataset2: str = 'dataset name here',
        preview: bool=False):
    #import necessary things
    from utils import placekey_merge, get_placekeyd_dataset
    import pandas as pd
    import geopandas as gpd
    import shapely

    #get two placekey'd datasets; choose these based on what's available on the module tab
    df1 = get_placekeyd_dataset(bbox, dataset1)
    df2 = get_placekeyd_dataset(bbox, dataset2)

    #join the datasets on placekey
    return placekey_merge(df1, df2)