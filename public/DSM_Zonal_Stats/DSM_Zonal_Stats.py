@fused.udf
def udf(
    bbox, min_zoom=15, table="s3://fused-asset/infra/building_msft_us/", chip_len=256
):
    import utils

    if bbox.z[0] >= min_zoom:
        gdf = utils.table_to_tile(bbox, table, min_zoom)
        arr = utils.dsm_to_tile(bbox, z_levels=[4, 6, 9, 11], verbose=False)
        gdf_zonal = utils.geom_stats(gdf, arr, chip_len=chip_len)
        return gdf_zonal
    else:
        print("Please zoom more... (US Only)")
