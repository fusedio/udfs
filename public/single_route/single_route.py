@fused.udf
def udf(lat_start = 34.0154145, lng_start = -118.2253804, lat_end = 33.9422, lng_end = -118.4036, cost = 'auto'): 
    import time
    import pandas as pd
    import geopandas as gpd
    #costing options: auto, pedestrian, bicycle,truck, bus, motor_scooter
    
    from utils import get_route, compute_distance
    from shapely.geometry import Point
    start = time.time()
    gdf = get_route(lat_start, lng_start, lat_end, lng_end, costing = cost)
    
    print('time to create route',time.time()-start, 'sec')
    gdf = compute_distance(gdf, col_name = 'separation')
    print('route length:',gdf['separation'].values[0]/1000.,'km')
    gdf['r']=0
    gdf['g']=255
    gdf['b']=255
    gdf_start = gpd.GeoDataFrame(columns = ['geometry', 'r','g','b'],geometry = [Point(lng_start,lat_start)], crs = 4326)
    gdf_start['g']=255
    
    gdf_end = gpd.GeoDataFrame(columns = ['geometry', 'r','g','b'],geometry = [Point(lng_end,lat_end)], crs = 4326)
    gdf_end['r']=255
    return pd.concat([gdf,gdf_start,gdf_end]) 