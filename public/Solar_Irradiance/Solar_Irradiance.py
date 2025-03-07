import geopandas as gpd
from shapely import box

@fused.udf
def udf(
    bounds: fused.types.Tile = gpd.GeoDataFrame(
            {"geometry": [box(-122.549, 37.681, -122.341, 37.818)]},
            crs="EPSG:4326")
):
    import palettable    
    import utils

    input_tiff_path = (
        f"s3://fused-asset/solar_irradiance/DNI_LTAy_Avg_Daily_Totals_DNI.tif"
    )
    data = utils.read_tiff(
        bounds=bounds,
        input_tiff_path=input_tiff_path,
        output_shape=(256, 256)
    )

    data = data.filled(fill_value=0)
    
    rgb_image = utils.visualize(
        data=data,
        min=0,
        max=8,
        colormap=palettable.scientific.sequential.LaJolla_20_r,
    )

    return rgb_image
