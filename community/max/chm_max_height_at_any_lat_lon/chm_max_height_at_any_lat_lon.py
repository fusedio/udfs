@fused.udf
def udf(
    lat: float = 49.2426,  # Default to San Francisco
    lon: float = -123.1119,
):
    """Calculate max tree height within 100m buffer of given lat/lon using proper geospatial buffering"""
    import numpy as np
    import geopandas as gpd
    from shapely.geometry import Point
    import pandas as pd

    buffer_meters = 250,  # 100m buffer around the point
    chip_len=256
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    
    # Create a GeoDataFrame with the point
    point_geom = Point(lon, lat)
    gdf = gpd.GeoDataFrame(geometry=[point_geom], crs="EPSG:4326")
    
    gdf_projected = gdf.to_crs(gdf.estimate_utm_crs())
    gdf_buffered = gdf_projected.buffer(buffer_meters)
    gdf_buffered_geo = gdf_buffered.to_crs("EPSG:4326")
    
    minx, miny, maxx, maxy = gdf_buffered_geo.total_bounds
    bounds = [minx, miny, maxx, maxy]
    
    # Load the CHM tiles metadata
    meta_chm_tiles_geojson_udf = fused.load('https://github.com/fusedio/udfs/tree/0858846/public/Meta_CHM_tiles_geojson/')
    image_id = fused.run(meta_chm_tiles_geojson_udf, bounds=bounds, use_centroid_method=False)
    
    if len(image_id) == 0:
        return pd.DataFrame({"error": ["No CHM data available for this location"]})
    
    path_of_chm = f"s3://dataforgood-fb-data/forests/v1/alsgedi_global_v6_float/chm/{image_id['tile'].iloc[0]}.tif"
    print(f"Using {path_of_chm=}")
    
    tile = common.get_tiles(bounds, target_num_tiles=4, clip=True)
    
    arr = common.read_tiff(tile, path_of_chm, output_shape=(chip_len, chip_len))

    # return arr * 100, bounds
    
    max_height = float(arr.max())
    
    result = pd.DataFrame({
        'lat': [lat],
        'lon': [lon],
        'buffer_meters': [buffer_meters],
        'max_tree_height_m': [max_height],
        'tile_id': [image_id['tile'].iloc[0]]
    })
    
    print(f"Max tree height within {buffer_meters}m buffer: {max_height}m")
    print(f"{result=}")
    
    return result