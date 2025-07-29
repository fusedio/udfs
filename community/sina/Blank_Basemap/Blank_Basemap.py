@fused.udf
def udf(bounds: fused.types.Bounds = [-58.483,-34.702,-58.376,-34.560]):
    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    return tile
