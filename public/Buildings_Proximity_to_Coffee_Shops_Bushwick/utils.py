import pandas as pd
import shapely
import geopandas as gpd

@fused.cache
def get_coffee(bbox):
        # Pull the points
        df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
        if len(df) < 1:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        # Return only coffee shops 
        return df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)]
       
@fused.cache
def get_single_isochrone(point_data):
    # Function for single isochrone
    point, costing, time_steps = point_data
    try:
        return fused.utils.Get_Isochrone.get_isochrone(
            lat=point.y,
            lng=point.x, 
            costing=costing,
            time_steps=time_steps
        )
    except Exception as e:
        print(f"Error processing point ({point.x}, {point.y}): {str(e)}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
@fused.cache
def get_pool_isochrone(df, costing, time_steps):
    # Run the isochrone requests concurretnly
    if len(df) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Using the Fused common run_pool function 
    arg_list = [(point, costing, time_steps) for point in df.geometry]
    isochrones = fused.utils.common.run_pool(get_single_isochrone, arg_list)
    valid_isochrones = [iso for iso in isochrones if len(iso) > 0]
    
    if not valid_isochrones:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    
    return pd.concat(valid_isochrones)
    
@fused.cache
def get_iso_gdf(costing, time_steps): 
    # Greater Bushwick
    bbox = gpd.GeoDataFrame(
       geometry=[shapely.box(-73.966036,40.666722,-73.875359,40.726179)], 
       crs=4326
    )
    # Coffee shops
    df = get_coffee(bbox)
    # print(df)
    if len(df) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Concurrent isochrones  
    return get_pool_isochrone(df, costing, time_steps)
   
@fused.cache
def get_cells(df_iso, resolution):
    # Connect to DuckDB
    con = fused.utils.common.duckdb_connect()
    # Turn the isochromes into cells and then count them
    query=f"""
    with to_cells as (
      select
        unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex
      from df_iso
    )
    select 
      hex,
      h3_cell_to_boundary_wkt(hex) as boundary,
      count(*) as coffee_score
    from to_cells
    group by hex
    """
    # Run the query and return as a gdf
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))

@fused.cache
def bushwick_boundary(gdf_h3):
    # Get NYC boundaries from NYC Open Data
    gdf_nyc = gpd.read_file('https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON')

    # Filter for Bushwick and dissolve
    gdf_bushwick = gdf_nyc[gdf_nyc['ntaname'].str.contains('Bushwick', na=False)].dissolve(by='ntaname').reset_index()
    
    # Keep only geometry column for overlay
    gdf_bushwick = gdf_bushwick[['geometry']]
    
    # Perform overlay (intersection)
    return gpd.overlay(gdf_h3, gdf_bushwick, how='intersection')
    
        