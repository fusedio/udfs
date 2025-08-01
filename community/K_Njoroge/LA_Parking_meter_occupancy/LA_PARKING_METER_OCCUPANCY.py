@fused.udf
def udf():
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    import requests
    
    # URL of the PMO dataset
    url_pmo = "https://data.lacity.org/resource/e7h6-4a3e.json"
    # URL of the PIP dataset
    url_pip = "https://data.lacity.org/resource/s49e-q6j2.json"

    # Function to fetch data from API and convert to DataFrame
    def fetch_data(url):
        all_data = []
        offset = 0
        limit = 1000
        while True:
            params = {"$limit": limit, "$offset": offset}
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                all_data.extend(data)
                if len(data) < limit:
                    break
                else:
                    offset += limit
            else:
                print("Failed to fetch data:", response.status_code)
                break
        return pd.DataFrame(all_data)

    # Fetch PMO data
    df_pmo = fetch_data(url_pmo)
    # Fetch PIP data
    df_pip = fetch_data(url_pip)

    # Merge PMO and PIP DataFrames on 'spaceid'
    df_park = pd.merge(df_pmo, df_pip, on='spaceid', how='inner')

    # Convert latlng column to Point geometry in PIP DataFrame
    df_pip['geometry'] = df_pip['latlng'].apply(lambda x: Point(float(x['longitude']), float(x['latitude'])))
    
    # Drop rows with missing geometry values
    df_pip = df_pip.dropna(subset=['geometry'])
    
    # Convert PIP DataFrame to GeoDataFrame
    gdf_pip = gpd.GeoDataFrame(df_pip, geometry='geometry')

    # Merge PMO DataFrame with GeoDataFrame created from PIP data
    gdf = pd.merge(df_pmo, gdf_pip, on='spaceid', how='outer')

    # Filter out None values in the geometry column and occupancy state column
    parking = gdf[['spaceid', 'occupancystate', 'geometry',
                   'blockface', 'metertype', 'ratetype',
                   'raterange', 'timelimit']]
    parking = parking.dropna(subset=['occupancystate','geometry'])

    # Print out the number of occupied and vacant parking spots
    num_occupied = (parking['occupancystate'] == 'OCCUPIED').sum()
    num_vacant = (parking['occupancystate'] == 'VACANT').sum()
    print(f"Number of occupied parking spots: {num_occupied}")
    print(f"Number of vacant parking spots: {num_vacant}")
    
    # Convert parking DataFrame to GeoDataFrame
    parking = gpd.GeoDataFrame(parking, geometry='geometry')
    initial_crs = 'EPSG:4326'
    parking = parking.set_crs(initial_crs)
    utm_crs = parking.estimate_utm_crs()
    print(f"UTM CRS: {utm_crs}")
    parking = parking.to_crs(utm_crs)
    
    # Function to fetch and buffer individual parking spots, and get adjacent buildings
    def fetch_adjacent_buildings(parking_spot):
        buffered_spot = parking_spot.geometry.buffer(2)  # Adjust the buffer distance as needed
        bbox = gpd.GeoDataFrame(index=[0], crs=utm_crs, geometry=[buffered_spot])
        buildings = get_overture(bbox=bbox, overture_type="building", theme="buildings")
        if buildings is not None and not buildings.empty:
            #buildings = buildings.to_crs(utm_crs)
            adjacent_buildings = gpd.sjoin(buildings, bbox, how='inner', predicate='intersects')
            return adjacent_buildings
        return None
    
    # Fetch and process buildings for each parking spot
    all_adjacent_buildings = []
    for index, parking_spot in parking.iterrows():
        adjacent_buildings = fetch_adjacent_buildings(parking_spot)
        if adjacent_buildings is not None:
            adjacent_buildings['spaceid'] = parking_spot['spaceid']
            all_adjacent_buildings.append(adjacent_buildings)
    
    # Concatenate all the adjacent buildings
    if all_adjacent_buildings:
        final_adjacent_buildings = pd.concat(all_adjacent_buildings)
        final_adjacent_buildings = gpd.GeoDataFrame(final_adjacent_buildings, geometry='geometry')
        
        # Save the result to a GeoParquet file
        # final_adjacent_buildings.to_parquet("adjacent_buildings.parquet")
        
        # Output the result
        print(final_adjacent_buildings)
    else:
        print("No buildings found adjacent to parking spots.")

    # Return the DataFrame with adjacent buildings
    return final_adjacent_buildings if all_adjacent_buildings else None


import geopandas as gpd 

def get_overture(
    bbox: fused.types.Tile = None,
    release: str = "2024-03-12-alpha-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: gpd.GeoDataFrame = None,
    point_convert: str = None,
):
    import logging
    import concurrent.futures
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import shape, box

    common = fused.load("https://github.com/fusedio/udfs/tree/6e8abb9/public/common/")

    #if polygon is not None:
        #polygon = polygon.to_crs(epsg=32611)

    if release == "2024-02-15-alpha-0":
        if overture_type == "administrative_boundary":
            overture_type = "administrativeBoundary"
        elif overture_type == "land_use":
            overture_type = "landUse"
        theme_per_type = {
            "building": "buildings",
            "administrativeBoundary": "admins",
            "place": "places",
            "landUse": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    else:
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }

    if theme is None:
        theme = theme_per_type.get(overture_type, "buildings")

    if overture_type is None:
        type_per_theme = {v: k for k, v in theme_per_type.items()}
        overture_type = type_per_theme[theme]

    if num_parts is None:
        num_parts = 1 if overture_type != "building" else 5

    if min_zoom is None:
        if theme == "admins":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        bounds = polygon.geometry.bounds
        bbox = gpd.GeoDataFrame(
            {
                "geometry": [
                    box(
                        bounds.minx.loc[0],
                        bounds.miny.loc[0],
                        bounds.maxx.loc[0],
                        bounds.maxy.loc[0],
                    )
                ]
            }
        )

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return common.table_to_tile(
                bbox, table=part_path, use_columns=use_columns, min_zoom=min_zoom
            )
        except ValueError:
            return None

    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        # Don't bother creating a thread pool to do one thing
        dfs = [get_part(0)] 

    dfs = [df for df in dfs if df is not None]

    if len(dfs):
        gdf = pd.concat(dfs)

    else:
        logging.warn("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    return gdf

