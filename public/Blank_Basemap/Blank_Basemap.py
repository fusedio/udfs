@fused.udf
def udf(bounds: fused.types.Bounds = [-58.483,-34.702,-58.376,-34.560]):
    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = utils.get_tiles(bounds)

    return tile
