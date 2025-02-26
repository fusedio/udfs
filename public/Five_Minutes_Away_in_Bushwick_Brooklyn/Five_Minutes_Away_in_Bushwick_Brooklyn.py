@fused.udf
def udf(bbox: fused.types.Tile = None, 
        resolution: int  = 11,
        poi_category: str = "Coffee Shop",
        use_columns = ["subtype"], 
        costing = "pedestrian", 
        time_steps = [5] # Single time step
       ):
    
    import shapely
    import geopandas as gpd
    import pandas as pd
    from utils import get_fsq_isochrones_gdf, fsq_isochrones_to_h3, bushwick_boundary
    
    # This pulls 5 minute walking isochrones around FSQ coffee shops
    gdf_fsq_isochrones = get_fsq_isochrones_gdf(costing, time_steps, poi_category)
    
    # Converts geometries to WKT for DuckDB 
    gdf_fsq_isochrones['geometry'] = gdf_fsq_isochrones['geometry'].apply(shapely.wkt.dumps)
    
    # Better to use Pandas with DuckDB
    df_fsq_isochrones = pd.DataFrame(gdf_fsq_isochrones)
    if df_fsq_isochrones is None or df_fsq_isochrones.empty:
        return  
    
    # Convert the isochrone Polygons to H3
    gdf_h3_isochrones = fsq_isochrones_to_h3(df_fsq_isochrones, resolution)

    # Overlay Bushwick Boundary
    gdf_h3_isochrones = bushwick_boundary(gdf_h3_isochrones)
    
    # Get Overture Buildings
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    gdf_overture = overture_utils.get_overture(bbox=bbox, use_columns=use_columns, min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(gdf_h3_isochrones, how="inner", predicate="intersects")
    gdf_joined = gdf_joined.drop(columns='index_right')
    return gdf_joined