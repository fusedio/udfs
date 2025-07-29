@fused.udf
def udf(
    bounds: fused.types.Bounds= [-122.416,37.773,-122.383,37.792], min_zoom=15, table="s3://fused-asset/infra/building_msft_us/", chip_len=256
):
    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    dsm_jaxa = fused.load("https://github.com/fusedio/udfs/tree/c682e4e/community/sina/DSM_JAXA_Example/")

    zoom = common.estimate_zoom(bounds)

    if zoom >= min_zoom:
        gdf = common.table_to_tile(bounds, table, min_zoom)
        arr = dsm_jaxa.dsm_to_tile(bounds, z_levels=[4, 6, 9, 11], verbose=False)
        gdf_zonal = common.geom_stats(gdf, arr, output_shape=(chip_len, chip_len))
        return gdf_zonal
    else:
        print("Please zoom more... (US Only)")
