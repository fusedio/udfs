@fused.udf
def udf(date: str = "2026-04-08"):
    """
    Computes driving routes between consecutive meetings on a given day.

    Args:
        date: Date string in 'YYYY-MM-DD' format. To get today's meetings,
              manually pass date="" (empty string) — the upstream geocode UDF
              will then default to today. If left as-is, it uses the hardcoded
              default date above.

    Returns:
        GeoDataFrame with one row per consecutive meeting pair, including route
        geometry (polyline), distance, duration, and on-time status.
    """
    import requests
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import LineString
    from datetime import datetime
    
    # Load NYC meeting locations from the other UDF
    geocode = fused.load("gcal_events_geocode")
    meetings_df = geocode(date=date)
    
    # Filter out meetings without valid coordinates (lat/lon)
    meetings_df = meetings_df[
        (meetings_df['latitude'].notna()) & 
        (meetings_df['longitude'].notna())
    ].reset_index(drop=True)
    
    print(f"Filtered meetings (removed those without lat/lon): {len(meetings_df)} meetings")
    for idx, row in meetings_df.iterrows():
        print(f"  {idx}. {row['title']} ({row['latitude']:.4f}, {row['longitude']:.4f})")
    
    google_maps_api = fused.secrets["google_maps_api"]
    
    def format_time_range(start_str, end_str):
        """Format ISO datetime strings to 'HH:MM - HH:MM' format"""
        try:
            start_time = datetime.fromisoformat(start_str.replace('Z', '+00:00')).strftime('%H:%M')
            end_time = datetime.fromisoformat(end_str.replace('Z', '+00:00')).strftime('%H:%M')
            return f"{start_time} - {end_time}"
        except:
            return 'N/A'
    
    @fused.cache()
    def get_route(origin_lat, origin_lon, dest_lat, dest_lon):
        # Google Routes API (new version) - uses POST with JSON body
        url = "https://routes.googleapis.com/directions/v2:computeRoutes"
        
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': google_maps_api,
            'X-Goog-FieldMask': 'routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline'
        }
        
        payload = {
            "origin": {
                "location": {
                    "latLng": {
                        "latitude": origin_lat,
                        "longitude": origin_lon
                    }
                }
            },
            "destination": {
                "location": {
                    "latLng": {
                        "latitude": dest_lat,
                        "longitude": dest_lon
                    }
                }
            },
            "travelMode": "DRIVE",
            "routingPreference": "TRAFFIC_AWARE"
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            data = response.json()
            
            if 'routes' in data and len(data['routes']) > 0:
                return data['routes'][0]
            else:
                print(f"No route found: {data}")
                return None
                
        except Exception as e:
            print(f"Error getting route: {e}")
            return None
    
    # Helper function to decode Google's polyline
    def decode_polyline(encoded, origin_lon, origin_lat, dest_lon, dest_lat):
        try:
            # Using a simple approach to decode Google's polyline format
            import polyline
            coords = polyline.decode(encoded)
            return [(lon, lat) for lat, lon in coords]  # Return as (lon, lat) for shapely
        except ImportError:
            # Fallback: return straight line between points
            return [(origin_lon, origin_lat), (dest_lon, dest_lat)]
    
    # Prepare routing data for consecutive meetings
    routes_data = []
    
    for i in range(len(meetings_df) - 1):
        origin = meetings_df.iloc[i]
        destination = meetings_df.iloc[i + 1]
        
        # Get cached route
        route = get_route(
            origin['latitude'], origin['longitude'],
            destination['latitude'], destination['longitude']
        )

        if route:
            # Decode polyline to get route geometry
            geometry_coords = None
            if 'polyline' in route and 'encodedPolyline' in route['polyline']:
                geometry_coords = decode_polyline(
                    route['polyline']['encodedPolyline'],
                    origin['longitude'], origin['latitude'],
                    destination['longitude'], destination['latitude']
                )
            else:
                # Fallback: straight line between points
                geometry_coords = [(origin['longitude'], origin['latitude']), 
                                 (destination['longitude'], destination['latitude'])]
            # Create LineString geometry
            geometry = LineString(geometry_coords)
            
            route_info = {
                'from_meeting': origin['title'],
                'to_meeting': destination['title'],
                'from_time': format_time_range(origin['start'], origin['end']),
                'from_location': origin['location'],
                'to_time': format_time_range(destination['start'], destination['end']),
                'to_location': destination['location'],
                'distance_meters': route.get('distanceMeters'),
                'duration': route.get('duration'),
                'from_lat': origin['latitude'],
                'from_lon': origin['longitude'],
                'to_lat': destination['latitude'],
                'to_lon': destination['longitude'],
                'geometry': geometry
            }
            
            routes_data.append(route_info)
            print(f"Route: {origin['title']} -> {destination['title']}")
            print(f"Distance: {route.get('distanceMeters')}m, Duration: {route.get('duration')}")
    
    # Convert to GeoDataFrame
    routes_gdf = gpd.GeoDataFrame(routes_data, geometry='geometry', crs='EPSG:4326')

    # --- Compute status columns ---
    def parse_end_minutes(time_range):
        """Extract end time from 'HH:MM - HH:MM' and return total minutes since midnight."""
        try:
            end = time_range.split(' - ')[1].strip()
            h, m = map(int, end.split(':'))
            return h * 60 + m
        except:
            return None

    def parse_start_minutes(time_range):
        """Extract start time from 'HH:MM - HH:MM' and return total minutes since midnight."""
        try:
            start = time_range.split(' - ')[0].strip()
            h, m = map(int, start.split(':'))
            return h * 60 + m
        except:
            return None

    def parse_duration_seconds(duration_str):
        """Parse '1373s' -> 1373 (int seconds)."""
        try:
            return int(str(duration_str).replace('s', '').strip())
        except:
            return None

    routes_gdf['gap_minutes'] = (
        routes_gdf['to_time'].apply(parse_start_minutes) -
        routes_gdf['from_time'].apply(parse_end_minutes)
    )
    routes_gdf['travel_minutes'] = routes_gdf['duration'].apply(
        lambda d: round(parse_duration_seconds(d) / 60) if parse_duration_seconds(d) is not None else None
    )
    routes_gdf['buffer_minutes'] = routes_gdf['gap_minutes'] - routes_gdf['travel_minutes']
    routes_gdf['status'] = routes_gdf['buffer_minutes'].apply(
        lambda b: 'On Time' if b is not None and b >= 0 else 'Will Be Late'
    )

    print("\nAll Routes with status:")
    print(routes_gdf[['from_meeting', 'to_meeting', 'gap_minutes', 'travel_minutes', 'buffer_minutes', 'status']].to_string())

    return routes_gdf