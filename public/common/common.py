@fused.udf
def udf(bounds: fused.types.Bounds):
    import utils
    return utils.to_gdf(bounds)



