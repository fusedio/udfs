@fused.udf
def udf(bbox):
    table_to_tile = fused.core.import_from_github(
        "https://github.com/fusedio/udfs/tree/a63664f4a4451d07efd003e318a1413c51a54889/public/common"
    ).utils.table_to_tile
    table_path="s3://fused-asset/infra/building_msft_us/"
    df=table_to_tile(bbox, table=table_path)
    return df
