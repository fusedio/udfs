@fused.udf
def udf(bounds: fused.types.Bounds = [-122.438,37.774,-122.434,37.777],
        dataset1: str = 'home-health-agency-medicare-enrollments',
        dataset2: str = 'national-provider-identifier',
        preview: bool=False):
    #import necessary things
    from utils import placekey_merge, get_placekeyd_dataset
    import pandas as pd
    import geopandas as gpd
    import shapely

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds, clip=True)

    #get two placekey'd datasets; choose these based on what's available on the module tab
    df1 = get_placekeyd_dataset(tile, dataset1)
    df2 = get_placekeyd_dataset(tile, dataset2)

    #join the datasets on placekey
    return placekey_merge(df1, df2)