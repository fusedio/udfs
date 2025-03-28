@fused.udf
def udf(bounds: fused.types.Bounds, table_path="s3://fused-asset/infra/building_msft_us"):
    utils = fused.load("https://github.com/fusedio/udfs/tree/91845c4/public/common/").utils
    df = utils.table_to_tile(bounds, table=table_path)
    return df
