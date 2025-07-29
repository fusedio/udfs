import json

j = json.dumps({"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-122.4017963492825,37.628491164736616],[-122.38690611551642,37.606792625414464],[-122.38037214148257,37.60276024740176],[-122.37133040325998,37.61257919769027],[-122.35791086649414,37.60768132596657],[-122.35272358459858,37.61542869072087],[-122.36649356499498,37.62170750731398],[-122.36213655637812,37.628763217050896],[-122.37562464496435,37.63359108358579],[-122.39539369398146,37.63236203450454],[-122.4017963492825,37.628491164736616]]]}}]})

@fused.udf
def udf(target_gdf = j, zoom: int=17):
    import geopandas as gpd
    import pandas as pd
    import json
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    # Convert to GeoDataFrame, if needed
    if isinstance(target_gdf, str):
        target_gdf = gpd.GeoDataFrame.from_features(json.loads(target_gdf))

    # Tile the AOI
    bounds = target_gdf.total_bounds
    gdf_tiles = common.get_tiles(bounds, zoom=zoom)

    # Structure array of tile GeoDataFrames 
    list_of_tile_gdfs = [gpd.GeoDataFrame(row.to_frame().T) for _, row in gdf_tiles.iterrows()]
    print('Total tiles: ', len(list_of_tile_gdfs))
    if len(list_of_tile_gdfs)>400:
        print('Too many ',len(list_of_tile_gdfs), 'tiles, too large a bounds, shrink it until there are less than 400 tiles.')
        return gpd.GeoDataFrame({'a':[1]})

    # Run the UDF concurrently for each tile
    gdf_out = fused.submit(
        "UDF_DL4EO_Airplane_Detection", 
        [{"bounds": list(bounds.total_bounds)} for bounds in list_of_tile_gdfs],
    )
    gdf_out = gdf_out.reset_index()
    print('Total airplanes: ', len(gdf_out))
    return gdf_out