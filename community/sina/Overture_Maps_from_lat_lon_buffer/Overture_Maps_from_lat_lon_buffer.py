@fused.udf 
def udf(
    lat: float=49.2806, 
    lon: float=-123.1259,
    buffer_amount: float = 500.0,
):
    release:str= "2025-01-22-0"
    s3_bucket:str= "s3://us-west-2.opendata.source.coop"
    
    import geopandas as gpd
    from shapely.geometry import Point


    aoi_gdf = gpd.GeoDataFrame(geometry=[Point(lon, lat)], crs="EPSG:4326")
            
    # Project to a local UTM projection for accurate buffering in meters
    # Get UTM zone from longitude
    utm_zone = int(((lon + 180) / 6) % 60) + 1
    hemisphere = 'north' if lat >= 0 else 'south'
    utm_crs = f"+proj=utm +zone={utm_zone} +{hemisphere} +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    
    gdf_utm = aoi_gdf.to_crs(utm_crs)
    gdf_utm['geometry'] = gdf_utm.buffer(buffer_amount)
    gdf_buffered = gdf_utm.to_crs("EPSG:4326")

    gdf = get_overture(
        bbox=gdf_buffered,
        release=release,
    )

    # Cleanup + adding area stats
    gdf.dropna(subset=['height'], inplace=True)
    gdf['area'] = gdf.to_crs(gdf.estimate_utm_crs()).geometry.area
    gdf['area'] = [int(area) for area in gdf['area']]
    gdf['height'] = [int(height) for height in gdf['height']]

    building_stats = gdf[['height', 'area']]
    building_stats.dropna(inplace=True)

    print(building_stats.sample(3))
    
    return building_stats
    
@fused.cache
def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2025-01-22-0",
    s3_bucket: str = "s3://us-west-2.opendata.source.coop",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    """Returns Overture data as a GeoDataFrame."""
    import logging
    import concurrent.futures
    import json
    
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import shape, box

    # Load Fused helper functions
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

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
    elif release == "2024-03-12-alpha-0":
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    else:
        theme_per_type = {
            "address": "addresses",
            "building": "buildings",
            "infrastructure": "base",
            "land": "base",
            "land_use": "base",
            "land_cover": "base",
            "water": "base",
            "bathymetry": "base",
            "place": "places",
            "division": "divisions",
            "division_area": "divisions",
            "division_boundary": "divisions",
            "segment": "transportation",
            "connector": "transportation",
        }

    if theme is None:
        theme = theme_per_type.get(overture_type, "buildings")

    if overture_type is None:
        type_per_theme = {v: k for k, v in theme_per_type.items()}
        overture_type = type_per_theme[theme]

    if num_parts is None:
        if overture_type == "building":
            if release >= "2025-01-22-0":
                num_parts = 6
            else:
                num_parts = 5
        else:
            num_parts = 1

    if min_zoom is None:
        if theme == "admins" or theme == "divisions":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"{s3_bucket}/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        polygon=gpd.from_features(json.loads(polygon))
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
