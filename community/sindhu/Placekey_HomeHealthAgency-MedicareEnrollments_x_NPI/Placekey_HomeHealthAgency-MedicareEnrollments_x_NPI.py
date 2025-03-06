@fused.udf
def udf(bbox: fused.types.TileGDF, preview: bool=False):
    #import necessary things
    from utils import placekey_merge, get_placekeyd_dataset
    import pandas as pd
    import geopandas as gpd
    import shapely

    #get two placekey'd datasets; choose these based on what's available on the module tab
    df1 = get_placekeyd_dataset(bbox, "national-provider-identifier")
    df2 = get_placekeyd_dataset(bbox, "home-health-agency-medicare-enrollments")

    #join the datasets on placekey
    return placekey_merge(df1, df2)