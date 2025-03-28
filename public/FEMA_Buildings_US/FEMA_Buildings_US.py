@fused.udf
def udf(bounds: fused.types.Bounds):
    path='s3://fused-asset/infra/building_oak/'

    utils = fused.load("https://github.com/fusedio/udfs/tree/91845c4/public/common/").utils
    df = utils.table_to_tile(bounds, table=path, use_columns=None, min_zoom=13)
    return df