@fused.udf(cache_max_age=0)
def udf(date: str = "2026-04-08"):  # e.g. "2026-04-08", defaults to today
    import pandas as pd
    from geopy.geocoders import Nominatim
    import time
    
    gcal = fused.load("gcal_today_events")
    data = gcal(date=date)
    
    print(data.T)
    
    @fused.cache
    def geocode_location(location):
        if not location or not str(location).strip():
            return None, None
        try:
            geocoder = Nominatim(user_agent="fused_geocoder")
            result = geocoder.geocode(str(location), timeout=10)
            time.sleep(0.2)
            if result:
                return result.latitude, result.longitude
            else:
                return None, None
        except Exception:
            return None, None
    
    lats = []
    lons = []
    for location in data['location'].fillna(''):
        lat, lon = geocode_location(location)
        lats.append(lat)
        lons.append(lon)
    
    data['latitude'] = lats
    data['longitude'] = lons
    
    print(data.T)
    return data