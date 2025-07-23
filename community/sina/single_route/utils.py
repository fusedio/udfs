import fused
#import sys

#six degrees of precision in valhalla
inv = 1.0 / 1e6

#decode an encoded string
def decode(encoded):
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