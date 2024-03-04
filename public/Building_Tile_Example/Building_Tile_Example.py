@fused.udf
def udf(bbox, table_path="s3://fused-asset/infra/building_msft_us"):
    table_to_tile = fused.core.load_udf_from_github(
        "https://github.com/fusedio/udfs/tree/ccbab4334b0cfa989c0af7d2393fb3d607a04eef/public/common/"
    ).utils.table_to_tile
    df = table_to_tile(bbox, table=table_path)
    return df
