@fused.udf
def udf(bounds: fused.types.Bounds):
    from utils import get_tiles
    bbox = get_tiles(bounds)
    return bbox