@fused.udf
def udf(
    bounds: fused.types.Bounds=None, 
    path: str = "https://maxar-opendata.s3.amazonaws.com/events/Emilia-Romagna-Italy-flooding-may23/ard/33/031111210233/2023-05-23/1050010033C95B00-visual.tif", 
    chip_len=256,
    display_extent: bool = False
):
    import rasterio
    import numpy as np
    import geopandas as gpd
    from shapely.geometry import box
    from rasterio.session import AWSSession

    # Getting just bounds of image so we can zoom to layer
    if display_extent:
        print("Returning extent")
        with rasterio.Env(session=AWSSession()):
            with rasterio.open(path) as src:
                bbox_gdf = gpd.GeoDataFrame(geometry=[box(*src.bounds)],crs=src.crs)
        bbox_gdf.to_crs(4326, inplace=True)
    
        return bbox_gdf

    # Otherwise reading the GeoTiff
    else:
        print("Returning image")
        common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")  
        tiles = common.get_tiles(bounds)
    
        arr = common.read_tiff(tiles, path, output_shape=(chip_len, chip_len))
        print(f"{arr.shape=}")
        return arr