@fused.udf
def udf(
    bounds: fused.types.Bounds, min_zoom=15, table="s3://fused-asset/infra/building_msft_us/", chip_len=256
):
    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/91845c4/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)


    if zoom >= min_zoom:
        gdf = utils.table_to_tile(bounds, table, min_zoom)
        arr = utils.dsm_to_tile(bounds, z_levels=[4, 6, 9, 11], verbose=False)
        gdf_zonal = utils.geom_stats(gdf, arr, output_shape=(chip_len, chip_len))
        return gdf_zonal
    else:
        print("Please zoom more... (US Only)")
