@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    tif_path: str = "https://huggingface.co/datasets/giswqs/geospatial/resolve/main/cars_7cm.tif",
    text_queries: list = ["a car from above"],
    threshold: float = 0.003,
    chip_size: int = 512,
    min_box_size: int = 5,  
    max_box_size: int = 50  
):
    import geopandas as gpd
    import rasterio
    from rasterio.session import AWSSession
    from utils import load_owlvit_model, process_image_chip, convert_detections_to_geometries

    common = fused.load("https://github.com/fusedio/udfs/tree/5432edc/public/common/").utils
    zoom_level = common.estimate_zoom(bounds)
    print(f"Zoom level: {zoom_level}")

    if zoom_level < 13:
        print("Zoom level too low, returning empty GeoDataFrame")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    processor, model = load_owlvit_model()
    detections, tiles, image_data = process_image_chip(bounds, tif_path, processor, model, text_queries, threshold, chip_size, min_box_size, max_box_size)

    if not detections:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    with rasterio.Env(session=AWSSession()):
        tile_bounds = tiles.geometry.iloc[0].bounds
        image_shape = image_data.shape[1:] if len(image_data.shape) > 2 else image_data.shape
        geometries, properties = convert_detections_to_geometries(detections, tile_bounds, image_shape)

    return gpd.GeoDataFrame(properties, geometry=geometries)
