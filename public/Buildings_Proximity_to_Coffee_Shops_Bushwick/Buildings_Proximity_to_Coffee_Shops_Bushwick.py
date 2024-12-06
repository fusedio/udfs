@fused.udf
def udf(bbox: fused.types.TileGDF=None, 
        resolution: int=11,
        use_columns=["height", "subtype"], 
        costing="pedestrian", 
        time_steps=[5] # 5 minutes
       ):
    
    import shapely
    import geopandas as gpd
    import pandas as pd
    from utils import get_iso_gdf, get_cells, bushwick_boundary
    
    # This pulls 5 minute walking isochrones around FSQ coffee shops
    gdf_iso = get_iso_gdf(costing=costing, time_steps=time_steps)
    
    # Converts geometries to WKT for DuckDB 
    gdf_iso['geometry'] = gdf_iso['geometry'].apply(shapely.wkt.dumps)

    # Better to use Pandas with DuckDB
    df_iso = pd.DataFrame(gdf_iso)
    if df_iso is None or df_iso.empty:
        return  
    
    # Convert the isochrone Polygons to H3
    gdf_h3 = get_cells(df_iso, resolution)

    # Overlay Bushwick Boundary
    gdf_h3 = bushwick_boundary(gdf_h3)
    
    # Get Overture Buildings
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, use_columns=use_columns, min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(gdf_h3, how="inner", predicate="intersects")

    gdf_joined = gdf_joined.drop(columns='index_right')

    # See the cells 
    # return gdf_h3
    
    return gdf_joined