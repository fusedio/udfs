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
    from utils import get_overture

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
    
