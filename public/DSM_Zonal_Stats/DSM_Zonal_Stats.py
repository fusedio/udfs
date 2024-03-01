@fused.udf
def udf(bbox, min_zoom=15, table="s3://fused-asset/infra/building_msft_us/", chip_len=256):
    if bbox.z[0]>=min_zoom:
        table_to_tile = fused.core.import_from_github('https://github.com/fusedio/udfs/tree/6891a7a189b4143bf412242bc62e99a02f66277f/public/common/').utils.table_to_tile
        geom_stats = fused.core.import_from_github('https://github.com/fusedio/udfs/tree/6891a7a189b4143bf412242bc62e99a02f66277f/public/common').utils.geom_stats
        dsm_to_tile = fused.core.import_from_github('https://github.com/fusedio/udfs/tree/8b5bae2f522eda16d9dbc134112321209430fc35/public/DSM_JAXA_Example').utils.dsm_to_tile
        gdf = table_to_tile(bbox, table, min_zoom)
        arr = dsm_to_tile(bbox, z_levels=[4, 6, 9, 11], verbose=False)
        gdf_zonal = geom_stats(gdf, arr, chip_len=chip_len)
        return gdf_zonal
    else:
        print('Please zoom more... (US Only)')
