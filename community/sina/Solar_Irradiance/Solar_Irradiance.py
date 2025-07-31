
@fused.udf
def udf(
    bounds: fused.types.Bounds =[-143.184,7.090,-39.292,61.808]
):
    import palettable
    import geopandas as gpd
    from shapely import box

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    input_tiff_path = (
        f"s3://fused-asset/solar_irradiance/DNI_LTAy_Avg_Daily_Totals_DNI.tif"
    )
    data = common.read_tiff(
        bounds=tile,
        input_tiff_path=input_tiff_path,
        output_shape=(256, 256)
    )

    data = data.filled(fill_value=0)
    
    rgb_image = common.visualize(
        data=data,
        min=0,
        max=8,
        colormap=palettable.scientific.sequential.LaJolla_20_r,
    )

    return rgb_image, bounds
