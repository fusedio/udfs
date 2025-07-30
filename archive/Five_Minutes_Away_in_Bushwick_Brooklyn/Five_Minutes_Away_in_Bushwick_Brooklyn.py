@fused.udf
def udf(bounds: fused.types.Bounds = [-73.941,40.690,-73.892,40.740],
        resolution: int  = 11,
        poi_category: str = "Coffee Shop",
        use_columns = ["subtype"], 
        costing = "pedestrian", 
        time_steps = [5] # Single time step
       ):
    
    import shapely
    import geopandas as gpd
    import pandas as pd

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    
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
    overture_maps_example = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/") # Load pinned versions of utility functions.
    gdf_overture = overture_maps_example.get_overture(bounds=tile, use_columns=use_columns, min_zoom=10)
    
    # Join H3 with Buildings using coffe_score to visualize, you can change to a left join
    gdf_joined = gdf_overture.sjoin(gdf_h3_isochrones, how="inner", predicate="intersects")
    gdf_joined = gdf_joined.drop(columns='index_right')
    return gdf_joined



@fused.cache
def get_fsq_points(bounds, poi_category):
    # Pull the points
    import pandas as pd
    import shapely
    import geopandas as gpd
    fsq_udf = fused.load("https://github.com/fusedio/udfs/tree/d1216b1/community/sina/Foursquare_Open_Source_Places")
    df = fused.run(fsq_udf, bounds=bounds, min_zoom=0)
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

def get_single_isochrone(point_data):
    # Function for single isochrone
    point, costing, time_steps = point_data
    try:
        get_isochrone = fused.load("https://github.com/fusedio/udfs/blob/d8030cc/community/sina/Get_Isochrone/")
        return get_isochrone.get_isochrone(
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
    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # Using the Fused common run_pool function 
    arg_list = [(point, costing, time_steps) for point in df.geometry]
    isochrones = common.run_pool(get_single_isochrone, arg_list)
    
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
    import geopandas as gpd
    import shapely
    bounds = gpd.GeoDataFrame(
       geometry=[shapely.box(-73.966036,40.666722,-73.875359,40.726179)], 
       crs=4326
    )
    # Coffee shops
    df = get_fsq_points(bounds, poi_category)

    if len(df) == 0:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
    # Concurrent isochrones  
    return get_pool_isochrones(df, costing, time_steps)


@fused.cache
def fsq_isochrones_to_h3(df_fsq_isochrones, resolution):
    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # Connect to DuckDB
    con = common.duckdb_connect()
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
    
        