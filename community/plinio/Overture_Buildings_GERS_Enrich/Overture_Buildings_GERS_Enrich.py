import json

aoi = json.dumps({"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"coordinates":[[[-73.9945443706057,40.751205998161026],[-73.9945443706057,40.749882643934455],[-73.99222917395902,40.749882643934455],[-73.99222917395902,40.751205998161026],[-73.9945443706057,40.751205998161026]]],"type":"Polygon"}}]})

@fused.udf
def udf(bounds: fused.types.Bounds=aoi):
    import geopandas as gpd
    from shapely.geometry import box

    # 2. Load Overture Buildings that intersect the given bbox centroid
    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/") # Load pinned versions of utility functions.
    gdf = overture_maps.get_overture(bounds=bounds, overture_type='building', min_zoom=10)
    # How many Overture buildings fall within the bbox centroid?
    print("Buildings in centroid: ", len(gdf))
    
    # 3. Rule to set only one GERS on the input polygon
    # Convert bounds to GeoDataFrame and add the id
    if len(gdf) > 0:
        # Create a polygon from the bounds
        bounds_geom = box(bounds[0], bounds[1], bounds[2], bounds[3])
        result_gdf = gpd.GeoDataFrame({
            'geometry': [bounds_geom],
            'id': [gdf.id.values[0]]
        })
        print(result_gdf.iloc[0]['id'])
    else:
        # Handle case where no buildings found
        bounds_geom = box(bounds[0], bounds[1], bounds[2], bounds[3])
        result_gdf = gpd.GeoDataFrame({
            'geometry': [bounds_geom],
            'id': [None]
        })
        print("No buildings found")
    
    return result_gdf