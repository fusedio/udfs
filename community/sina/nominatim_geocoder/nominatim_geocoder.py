@fused.udf
def udf(location: str = "Vancouver, Canada"):
    """
    Geocodes a location string into latitude and longitude coordinates.
    
    Parameters:
    -----------
    location : str
        The location string to geocode (e.g. "San Francisco, CA", "1600 Pennsylvania Ave, Washington DC")
        
    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the geocoded location with columns for location name, latitude, and longitude
    """
    import pandas as pd
    from geopy.geocoders import Nominatim
    
    # Initialize the geocoder with a user agent
    geolocator = Nominatim(user_agent="fused_geocoding_udf")
    
    try:
        # Geocode the location
        location_data = geolocator.geocode(location)
        
        if location_data:
            # Create a DataFrame with the geocoded information
            df = pd.DataFrame({
                'location': [location],
                'lat': [location_data.latitude],
                'lon': [location_data.longitude],
                'address': [location_data.address]
            })
            
            # Create a geometry point for the location
            import geopandas as gpd
            from shapely.geometry import Point
            gdf = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df.lon, df.lat),
                crs="EPSG:4326"
            )
            
            return gdf
        else:
            # If geocoding failed, return an empty DataFrame with the expected columns
            print(f"Geocoding failed for location: {location}")
            return pd.DataFrame(columns=['location', 'lat', 'lon', 'address', 'geometry'])
            
    except Exception as e:
        print(f"Error geocoding location: {e}")
        return pd.DataFrame(columns=['location', 'lat', 'lon', 'address', 'geometry'])