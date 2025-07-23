@fused.udf
def udf(bounds: fused.types.Bounds = [-74.014,40.700,-74.000,40.717], table_path="s3://fused-asset/infra/building_msft_us"):
    utils = fused.load("https://github.com/fusedio/udfs/tree/d0e8eb0/public/common/").utils
    df = utils.table_to_tile(bounds, table=table_path)
    return df
