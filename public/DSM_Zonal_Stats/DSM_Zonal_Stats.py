@fused.udf
def udf(
    bounds: fused.types.Bounds= [-122.416,37.773,-122.383,37.792], min_zoom=15, table="s3://fused-asset/infra/building_msft_us/", chip_len=256
):
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/d0e8eb0/public/common/").utils
    dsm_utils = fused.load("https://github.com/fusedio/udfs/blob/5bc22d7/public/DSM_JAXA_Example/").utils

    zoom = utils.estimate_zoom(bounds)

    if zoom >= min_zoom:
        gdf = utils.table_to_tile(bounds, table, min_zoom)
        arr = dsm_utils.dsm_to_tile(bounds, z_levels=[4, 6, 9, 11], verbose=False)
        gdf_zonal = utils.geom_stats(gdf, arr, output_shape=(chip_len, chip_len))
        return gdf_zonal
    else:
        print("Please zoom more... (US Only)")
