@fused.udf
def udf(bounds: fused.types.Bounds):
    path='s3://fused-asset/infra/building_oak/'

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    df = utils.table_to_tile(tile, table=path, use_columns=None, min_zoom=13)
    return df