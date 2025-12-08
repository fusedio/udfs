@fused.udf
def udf(
    bounds: fused.types.Bounds = None,  # Added to satisfy vector_tile type
    lat: float = 28.6556,
    lng: float = 77.1872,
    buffer_distance: float = 100,  # meters
    layer_name: str = "global3D:lod1_global",
    max_features: int = 500
):
    import geopandas as gpd
    import pandas as pd  # Added missing import
    import requests
    from shapely.geometry import Point
    import json
    import time
    
    @fused.cache
    def fetch_wfs_buffer(lat, lng, buffer_m, layer, max_feat):
        """Fetch WFS features within buffer around a point"""
        # Create point and buffer it
        point = gpd.GeoDataFrame(
            geometry=[Point(lng, lat)],
            crs='EPSG:4326'
        )
        
        # Project to UTM for accurate meter-based buffer
        # Estimate UTM zone from longitude
        utm_zone = int((lng + 180) / 6) + 1
        utm_crs = f'EPSG:326{utm_zone}' if lat >= 0 else f'EPSG:327{utm_zone}'
        
        # Buffer in meters
        point_utm = point.to_crs(utm_crs)
        buffer_utm = point_utm.buffer(buffer_m)
        buffer_geom = buffer_utm.to_crs('EPSG:4326').iloc[0]
        
        # Get bounding box of buffer
        bbox = buffer_geom.bounds  # (minx, miny, maxx, maxy)
        
        print(f"Searching for buildings within {buffer_m}m of ({lat}, {lng})")
        print(f"Bounding box: {bbox}")
        
        url = "https://tubvsig-so2sat-vm1.srv.mwn.de/geoserver/ows"
        
        params = {
            'service': 'WFS',
            'version': '1.1.0',
            'request': 'GetFeature',
            'typeName': layer,
            'outputFormat': 'application/json',
            'srsName': 'EPSG:4326',
            'bbox': f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]},EPSG:4326",
            'maxFeatures': max_feat
        }
        
        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code != 200:
                    print(f"Attempt {attempt+1}: Status {response.status_code}")
                    print(f"Response text: {response.text[:300]}")
                    time.sleep(2 ** attempt)
                    continue
                
                # Check if response is empty
                if not response.text or len(response.text) < 10:
                    print(f"Attempt {attempt+1}: Empty response")
                    time.sleep(2 ** attempt)
                    continue
                
                # Print what we got for debugging
                print(f"Response length: {len(response.text)}")
                print(f"First 200 chars: {response.text[:200]}")
                
                # Parse JSON directly
                data = json.loads(response.text)
                
                if 'features' not in data or len(data['features']) == 0:
                    print(f"No features returned from server")
                    return gpd.GeoDataFrame(), buffer_geom
                
                gdf = gpd.GeoDataFrame.from_features(data['features'], crs='EPSG:4326')
                
                # Filter to only buildings within buffer
                gdf = gdf[gdf.geometry.intersects(buffer_geom)]
                
                print(f"Found {len(gdf)} buildings within buffer")
                if len(gdf) > 0:
                    print(f"Columns: {list(gdf.columns)}")
                    print(gdf.head().T)
                
                return gdf, buffer_geom
                
            except json.JSONDecodeError as je:
                print(f"Attempt {attempt+1} JSON error: {str(je)}")
                print(f"Response was: {response.text[:500]}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"Attempt {attempt+1} error: {str(e)[:200]}")
                time.sleep(2 ** attempt)
        
        return gpd.GeoDataFrame(), buffer_geom
    
    # Fetch buildings within buffer
    gdf, buffer_geom = fetch_wfs_buffer(lat, lng, buffer_distance, layer_name, max_features)
    
    if len(gdf) == 0:
        print(f"No buildings found. Server may be overloaded or no data at this location.")
        return gpd.GeoDataFrame()
    
    print(f"Returning {len(gdf)} buildings")
    return gdf