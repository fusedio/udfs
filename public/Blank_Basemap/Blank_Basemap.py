@fused.udf
def udf(bounds: fused.types.Bounds = [-58.483,-34.702,-58.376,-34.560]):
    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = utils.get_tiles(bounds, clip=True)

    return tile
