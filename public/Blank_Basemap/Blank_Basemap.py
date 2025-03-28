@fused.udf
def udf(bounds: fused.types.Bounds = None):
    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    tile = utils.get_tiles(bounds, zoom=zoom)

    return tile
