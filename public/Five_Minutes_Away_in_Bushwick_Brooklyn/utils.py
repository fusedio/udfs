import pandas as pd
import shapely
import geopandas as gpd

@fused.cache
def get_fsq_points(bbox, poi_category):
    # Pull the points
    df = fused.run("UDF_Foursquare_Open_Source_Places", bbox=bbox, min_zoom=0)
    # Check if the df is empty
    if len(df) < 1:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Define the dictionary for filtering
    category_dict = {
        "Bar": df[df["level2_category_name"].str.contains("Bar", case=False, na=False)],
        "Coffee Shop": df[df["level3_category_name"].str.contains("Coffee Shop", case=False, na=False)],
        "Grocery Store": df[df["level3_category_name"].str.contains("Grocery Store", case=False, na=False)],
        "Restaurant" : df[df["level2_category_name"].str.contains("Restaurant", case=False, na=False)],
        "Pharmacy" : df[df["level2_category_name"] == "Pharmacy"],
    }
    
    # Return the filtered DataFrame based on POI category
    return category_dict.get(poi_category, gpd.GeoDataFrame(geometry=[], crs="EPSG:4326"))

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
def get_pool_isochrones(df, costing, time_steps):
    # Run the isochrone requests concurrently
    if len(df) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Using the Fused common run_pool function 
    arg_list = [(point, costing, time_steps) for point in df.geometry]
    isochrones = fused.utils.common.run_pool(get_single_isochrone, arg_list)
    
    # Track which isochrones are valid along with their names
    valid_pairs = [(iso, name) for iso, name in zip(isochrones, df['name']) if len(iso) > 0]
    if not valid_pairs:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    
    # Unzip the pairs and add names to each isochrone in the pair
    valid_isochrones, names = zip(*valid_pairs)
    result = pd.concat(valid_isochrones)
    
    # Add names by repeating each name for its corresponding isochrone's rows
    name_list = []
    for iso, name in zip(valid_isochrones, names):
        name_list.extend([name] * len(iso))
    result['name'] = name_list
    
    return result
def get_fsq_isochrones_gdf(costing, time_steps, poi_category): 
    # Greater Bushwick
    bbox = gpd.GeoDataFrame(
       geometry=[shapely.box(-73.966036,40.666722,-73.875359,40.726179)], 
       crs=4326
    )
    # Coffee shops
    df = get_fsq_points(bbox, poi_category)

    if len(df) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
    # Concurrent isochrones  
    return get_pool_isochrones(df, costing, time_steps)


@fused.cache
def fsq_isochrones_to_h3(df_fsq_isochrones, resolution):
    # Connect to DuckDB
    con = fused.utils.common.duckdb_connect()
    # Convert the isochrones into H3, count the overlap and keep the POI name
    query = f"""
    with to_cells as (
     select
      unnest(h3_polygon_wkt_to_cells(geometry, {resolution})) AS hex,
      name
     from df_fsq_isochrones
    )
    select 
     hex,
     h3_cell_to_boundary_wkt(hex) as boundary,
     count(*) as poi_density,
     string_agg(DISTINCT name, ', ') as poi_names
    from to_cells
    group by hex
    """
    # Run the query and return a GeoDataFrame
    df = con.sql(query).df()
    return gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
@fused.cache
def get_nyc_boundary():
    # Get NYC boundaries from NYC Open Data
    url = 'https://data.cityofnewyork.us/api/geospatial/j2bc-fus8?method=export&format=GeoJSON'
    
    gdf_nyc = gpd.read_file(url, driver='GeoJSON')

    # Filter for Bushwick and dissolve
    gdf_bushwick = gdf_nyc[gdf_nyc['ntaname'].str.contains('Bushwick', na=False)].dissolve(by='ntaname').reset_index()
    
    # Keep only geometry column for overlay
    return gdf_bushwick[['geometry']]

def bushwick_boundary(gdf_h3):
    # Run get_boundary
    gdf_bushwick = get_nyc_boundary()
    
    # Perform overlay (intersection)
    return gpd.overlay(gdf_h3, gdf_bushwick, how='intersection')
    
        