@fused.udf
def udf(bounds: fused.types.Bounds = [-74.008,40.684,-73.971,40.713]):
    path='s3://fused-asset/infra/building_oak/'

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    df = common.table_to_tile(bounds, table=path, use_columns=None, min_zoom=10)
    bounds_gdf = common.to_gdf(bounds)
    df = df.clip(bounds_gdf.geometry[0]).explode()
    return df 