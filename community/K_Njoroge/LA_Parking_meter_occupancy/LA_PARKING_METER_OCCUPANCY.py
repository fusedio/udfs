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

    # Function to fetch data from API and convert to DataFrame (cached)
    @fused.cache
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
    print(f"PMO rows: {len(df_pmo)}, PIP rows: {len(df_pip)}")

    # Convert latlng column to Point geometry in PIP DataFrame
    df_pip['geometry'] = df_pip['latlng'].apply(lambda x: Point(float(x['longitude']), float(x['latitude'])))
    
    # Drop rows with missing geometry values
    df_pip = df_pip.dropna(subset=['geometry'])
    
    # Convert PIP DataFrame to GeoDataFrame
    gdf_pip = gpd.GeoDataFrame(df_pip, geometry='geometry', crs='EPSG:4326')

    # Merge PMO DataFrame with GeoDataFrame created from PIP data
    gdf = pd.merge(df_pmo, gdf_pip[['spaceid', 'geometry']], on='spaceid', how='inner')

    # Filter out None values in the geometry column and occupancy state column
    cols_to_keep = ['spaceid', 'occupancystate', 'geometry']
    for c in ['blockface', 'metertype', 'ratetype', 'raterange', 'timelimit']:
        if c in gdf.columns:
            cols_to_keep.append(c)
    parking = gdf[cols_to_keep]
    parking = parking.dropna(subset=['occupancystate', 'geometry'])

    # Print out the number of occupied and vacant parking spots
    num_occupied = (parking['occupancystate'] == 'OCCUPIED').sum()
    num_vacant = (parking['occupancystate'] == 'VACANT').sum()
    print(f"Number of occupied parking spots: {num_occupied}")
    print(f"Number of vacant parking spots: {num_vacant}")
    
    # Convert parking DataFrame to GeoDataFrame (keep in 4326)
    parking = gpd.GeoDataFrame(parking, geometry='geometry', crs='EPSG:4326')
    print(f"Parking spots with geometry: {len(parking)}")

    # Fetch buildings ONCE for the entire parking area bbox (much faster than per-spot)
    bbox_gdf = gpd.GeoDataFrame(
        geometry=[parking.geometry.unary_union.convex_hull.buffer(0.001)],
        crs='EPSG:4326'
    )
    print("Fetching Overture buildings for full extent...")
    buildings = get_overture(bbox=bbox_gdf, overture_type="building", theme="buildings")
    
    if buildings is None or buildings.empty:
        print("No buildings found in the area.")
        return parking

    print(f"Buildings fetched: {len(buildings)}")
    buildings = buildings.to_crs('EPSG:4326')

    # Buffer parking spots by ~2m in UTM, then back to 4326 for spatial join
    utm_crs = parking.estimate_utm_crs()
    parking_utm = parking.to_crs(utm_crs)
    parking_utm['geometry'] = parking_utm.geometry.buffer(2)
    parking_buffered = parking_utm.to_crs('EPSG:4326')

    # Spatial join: find buildings that intersect each buffered parking spot
    result = gpd.sjoin(buildings, parking_buffered, how='inner', predicate='intersects')
    print(f"Adjacent building-spot pairs: {len(result)}")

    # Keep building geometry + parking attributes
    result = result.rename(columns={'spaceid_right': 'spaceid'})
    if 'spaceid' not in result.columns and 'spaceid_left' in result.columns:
        result = result.rename(columns={'spaceid_left': 'spaceid'})

    return gpd.GeoDataFrame(result, geometry='geometry', crs='EPSG:4326')


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

    common = fused.load("https://github.com/fusedio/udfs/tree/3991434/public/common/")

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

