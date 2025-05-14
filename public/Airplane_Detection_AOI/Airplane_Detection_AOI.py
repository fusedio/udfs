import geopandas as gpd
import json

j = json.dumps({"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-122.4017963492825,37.628491164736616],[-122.38690611551642,37.606792625414464],[-122.38037214148257,37.60276024740176],[-122.37133040325998,37.61257919769027],[-122.35791086649414,37.60768132596657],[-122.35272358459858,37.61542869072087],[-122.36649356499498,37.62170750731398],[-122.36213655637812,37.628763217050896],[-122.37562464496435,37.63359108358579],[-122.39539369398146,37.63236203450454],[-122.4017963492825,37.628491164736616]]]}}]})

@fused.udf
def udf(target_gdf: gpd.GeoDataFrame = j, zoom: int=17):
    import geopandas as gpd
    import pandas as pd
    import json
    from utils import make_tiles_gdf

    # Convert to GeoDataFrame, if needed
    if isinstance(target_gdf, str):
        target_gdf = gpd.GeoDataFrame.from_features(json.loads(target_gdf))

    # Tile the AOI
    bounds = target_gdf.total_bounds
    gdf_tiles = make_tiles_gdf(bounds, zoom = zoom)

    # Structure array of tile GeoDataFrames 
    list_of_tile_gdfs = [gpd.GeoDataFrame(row.to_frame().T) for _, row in gdf_tiles.iterrows()]
    print('Total tiles: ', len(list_of_tile_gdfs))
    if len(list_of_tile_gdfs)>400:
        print('Too many ',len(list_of_tile_gdfs), 'tiles, too large a bounds, shrink it until there are less than 400 tiles.')
        return gpd.GeoDataFrame({'a':[1]})
    
    # Function to run UDF 
    @fused.cache
    def run_udf(bounds):
        out = fused.run("UDF_DL4EO_Airplane_Detection", bounds=bounds)
        if len(out)>0:
            return out

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # Run the UDF concurrently for each tile
    gdfs = utils.run_pool(run_udf, list_of_tile_gdfs)
    gdf_out = pd.concat(gdfs)
    print('Total airplanes: ', len(gdf_out))
    return gdf_out
