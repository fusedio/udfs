@fused.udf
def udf(bounds: fused.types.Bounds = None):
    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = utils.get_tiles(bounds)

    return tile
