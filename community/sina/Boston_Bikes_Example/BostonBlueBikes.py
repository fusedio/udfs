@fused.udf
def udf():
    import geopandas as gpd
    import requests
    import pytz
    from datetime import datetime

    geo_json_data = {
       "type": "FeatureCollection",
       "features": []
    }
    stations = requests.get(
        "https://gbfs.lyft.com/gbfs/1.1/bos/es/station_information.json"
    ).json()
    stations_status = requests.get(
        "https://gbfs.lyft.com/gbfs/1.1/bos/es/station_status.json"
    ).json()
    stations_dict = {}
    for station in stations['data']['stations']:
        stations_dict[station['station_id']] = [station['name'],\
                station['capacity'] ,\
                station['lon'],\
                station['lat']]
    for status in stations_status['data']['stations']:
        unixts = int(status['last_reported'])

        ts = datetime.fromtimestamp(unixts, pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        if status['station_id'] in stations_dict:
            r,g,b = [100,100,100]
            if int(status['num_bikes_available']) == 0:
                r,g,b = [250,20,20]
            elif int(status['num_bikes_available']) <= 2:
                r,g,b = [250,165,20]
            else :
                r,g,b = [20,250,20]
            stations_dict[status['station_id']] \
                .extend([status['num_bikes_available'], \
                         status['num_docks_available'], \
                         r,g,b,ts])
            
    for k,attrs in stations_dict.items():
        feature = {
           "type": "Feature",
           "id": k,
           "geometry": {
               "type": "Point",
               "coordinates": [attrs[2], attrs[3]]
           },
           "properties": {
               'name':attrs[0],
               'capacity': attrs[1],
               'num_bikes_available': attrs[4],
               'num_docks_available': attrs[5],
               'timestamp': attrs[9],
               'r':attrs[6],
               'g':attrs[7],
               'b':attrs[8]
           }
        }
        geo_json_data['features'].append(feature)
            
        
    gdf = gpd.GeoDataFrame.from_features(geo_json_data)
    return gdf
