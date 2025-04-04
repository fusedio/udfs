@fused.udf
def udf(bounds: fused.types.Bounds = None, path:str='s3://fused-asset/infra/hydro_wbdhu12_us'):
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/d0e8eb0/public/common/").utils
    df = utils.table_to_tile(bounds, path, min_zoom=9, use_columns=['huc12', 'name', 'hutype'])
    return df 