@fused.udf
def udf(
    bbox, min_zoom=15, table="s3://fused-asset/infra/building_msft_us/", chip_len=256
):
    if bbox.z[0] >= min_zoom:
        table_to_tile = fused.core.load_udf_from_github(
            "https://github.com/fusedio/udfs/tree/f928ee1bd5cbf72573b587c63a7cbfa4a24b8dfe/public/common/"
        ).utils.table_to_tile
        geom_stats = fused.core.load_udf_from_github(
            "https://github.com/fusedio/udfs/tree/f928ee1bd5cbf72573b587c63a7cbfa4a24b8dfe/public/common"
        ).utils.geom_stats
        dsm_to_tile = fused.core.load_udf_from_github(
            "https://github.com/fusedio/udfs/tree/f928ee1bd5cbf72573b587c63a7cbfa4a24b8dfe/public/DSM_JAXA_Example"
        ).utils.dsm_to_tile
        gdf = table_to_tile(bbox, table, min_zoom)
        arr = dsm_to_tile(bbox, z_levels=[4, 6, 9, 11], verbose=False)
        gdf_zonal = geom_stats(gdf, arr, chip_len=chip_len)
        return gdf_zonal
    else:
        print("Please zoom more... (US Only)")
