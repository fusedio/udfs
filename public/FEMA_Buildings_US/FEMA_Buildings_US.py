@fused.udf
def udf(bounds: fused.types.Bounds = [-74.008,40.684,-73.971,40.713]):
    path='s3://fused-asset/infra/building_oak/'

    utils = fused.load("https://github.com/fusedio/udfs/tree/d0e8eb0/public/common/").utils
    df = utils.table_to_tile(bounds, table=path, use_columns=None, min_zoom=13)
    return df# test