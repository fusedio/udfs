import json

aoi = json.dumps({"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"coordinates":[[[-73.9945443706057,40.751205998161026],[-73.9945443706057,40.749882643934455],[-73.99222917395902,40.749882643934455],[-73.99222917395902,40.751205998161026],[-73.9945443706057,40.751205998161026]]],"type":"Polygon"}}]})

@fused.udf
def udf(bbox: fused.types.Tile=aoi):
    import geopandas as gpd

    # 1. Convert bbox to GeoDataFrame
    if isinstance(bbox, str):
        bbox = gpd.GeoDataFrame.from_features(json.loads(bbox))

    # 2. Load Overture Buildings that intersect the given bbox centroid
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/") # Load pinned versions of utility functions.
    bounds_tuple = bbox.total_bounds 
    gdf = overture_maps.get_overture(bounds=bounds_tuple, overture_type='building', min_zoom=10)
    # How many Overture buildings fall within the bbox centroid?
    print("Buildings in centroid: ", len(gdf))
    
    # 3. Rule to set only one GERS on the input polygon
    bbox['id'] = gdf.id.values[0]
    print(bbox['id'])
    
    return bbox

   

















    