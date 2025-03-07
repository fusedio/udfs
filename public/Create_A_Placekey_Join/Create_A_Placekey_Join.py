@fused.udf
def udf(bounds: fused.types.TileGDF,
        dataset1: str = 'home-health-agency-medicare-enrollments',
        dataset2: str = 'national-provider-identifier',
        preview: bool=False):
    #import necessary things
    from utils import placekey_merge, get_placekeyd_dataset
    import pandas as pd
    import geopandas as gpd
    import shapely

    #get two placekey'd datasets; choose these based on what's available on the module tab
    df1 = get_placekeyd_dataset(bounds, dataset1)
    df2 = get_placekeyd_dataset(bounds, dataset2)

    #join the datasets on placekey
    return placekey_merge(df1, df2)