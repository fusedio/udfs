@fused.udf
def udf(bbox: fused.types.TileGDF = None):
    import geopandas as gpd
    from shapely import box

    tile_bbox_gdf = (
        bbox
        if bbox is not None
        else gpd.GeoDataFrame(
            {"geometry": [box(-122.549, 37.681, -122.341, 37.818)]}, crs="EPSG:4326"
        )
    )

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
    ).utils

    input_tiff_path = (
        f"s3://fused-asset/solar_irradiance/DNI_LTAy_Avg_Daily_Totals_DNI.tif"
    )
    data = utils.read_tiff(
        bbox=tile_bbox_gdf, input_tiff_path=input_tiff_path, output_shape=(256, 256)
    )

    filled_data = data.filled(fill_value=0)

    return utils.arr_to_plasma(filled_data, min_max=(0, 6), reverse=False)
