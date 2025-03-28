@fused.udf
def udf(
    bounds: fused.types.Bounds,
    min_max=(0, 255),
    z_levels=[4, 6, 9, 11],
    verbose=False
):
    """Display a digital surface model from JAXA
    
    ref: https://data.earth.jaxa.jp/app/viewer/v1/?collection=https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/collection.json&band=DSM&date=2021-02-01T00:00:00.000Z
    """
    import palettable
    import utils
    
    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    arr = utils.dsm_to_tile(tile, z_levels=z_levels, verbose=verbose)
    return utils.common_utils.visualize(
        data=arr,
        min=0,
        max=3000,
        colormap=palettable.matplotlib.Viridis_20,
    )