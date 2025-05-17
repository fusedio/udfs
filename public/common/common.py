@fused.udf
def udf(bounds: fused.types.Bounds = [-74.014,40.700,-74.000,40.717]):
    import utils
    return utils.to_gdf(bounds)