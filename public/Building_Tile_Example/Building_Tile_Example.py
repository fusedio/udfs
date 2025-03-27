@fused.udf
def udf(bounds: fused.types.Bounds, table_path="s3://fused-asset/infra/building_msft_us"):
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    tile = utils.get_tiles(bounds, zoom=zoom)
    df = utils.table_to_tile(tile, table=table_path)
    return df
