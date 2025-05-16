import geopandas as gpd
from shapely import box

@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.549, 37.681, -122.341, 37.818]
):
    import palettable

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/39d93ca/public/common/").utils
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

    return rgb_image
