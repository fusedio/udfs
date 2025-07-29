@fused.udf
def udf(lat_start = 34.0154145, lng_start = -118.2253804, lat_end = 33.9422, lng_end = -118.4036, cost = 'auto'): 
    import time
    import pandas as pd
    import geopandas as gpd
    #costing options: auto, pedestrian, bicycle,truck, bus, motor_scooter

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




#decode an encoded string
def decode(encoded):

  #six degrees of precision in valhalla
  inv = 1.0 / 1e6
  decoded = []
  previous = [0,0]
  i = 0
  #for each byte
  while i < len(encoded):
    #for each coord (lat, lon)
    ll = [0,0]
    for j in [0, 1]:
      shift = 0
      byte = 0x20
      #keep decoding bytes until you have this coord
      while byte >= 0x20:
        byte = ord(encoded[i]) - 63
        i += 1
        ll[j] |= (byte & 0x1f) << shift
        shift += 5
      #get the final value adding the previous offset and remember it for the next
      ll[j] = previous[j] + (~(ll[j] >> 1) if ll[j] & 1 else (ll[j] >> 1))
      previous[j] = ll[j]
    #scale by the precision and chop off long coords also flip the positions so
    #its the far more standard lon,lat instead of lat,lon
    decoded.append([float('%.6f' % (ll[1] * inv)), float('%.6f' % (ll[0] * inv))])
  #hand back the list of coordinates
  return decoded

def get_route(lat_start,  lng_start, lat_end,  lng_end, costing = 'auto'):
        '''
        costing options: auto, pedestrian, bicycle, truck, bus, motor_scooter
        TODO: add costing_options: e.g. exclude_polygons
        ''' 
        import requests
        import pandas as pd
        import geopandas as gpd
        import time
        import random
        from shapely.geometry import LineString
        url = 'https://valhalla1.openstreetmap.de/route'
        params = {
            "locations": [{"lon": lng_start, "lat": lat_start},{"lon": lng_end, "lat": lat_end}]
            ,"costing":costing 
            ,"units":"miles"
        }
        response = requests.post(url, json=params)
        result = response.json()
        print('Driving Instructions:',[el['instruction'] for el in result['trip']['legs'][0]['maneuvers']])
        encoded_shape = result['trip']['legs'][0]['shape']
        decoded_shape = decode(encoded_shape)
        #print(decoded_shape)
        gdf_route = gpd.GeoDataFrame(columns = ['geometry'],geometry = [LineString(decoded_shape)], crs = 4326)
        #print(gdf_route)
        return gdf_route

def compute_distance(shortest_path_gdf, col_name = 'separation'):
    '''
    Compute distance in EPSG:3387
    
    '''
    
    # project WGS84 to EPSG3387
    distances = shortest_path_gdf.to_crs("EPSG:3387").geometry.length
    
    # add
    shortest_path_gdf[col_name] = distances
    
    return shortest_path_gdf