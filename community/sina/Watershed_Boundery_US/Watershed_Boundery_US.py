@fused.udf
def udf(bounds: fused.types.Bounds =[-74.26923870251201,40.56603101495095,-73.59327214867544,40.89565895351765], path:str='s3://fused-asset/infra/hydro_wbdhu12_us'):
    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/3ac8eaf/public/common/")
    df = common.table_to_tile(bounds, path, min_zoom=6, use_columns=['huc12', 'name', 'hutype'], clip = True)
    return df 