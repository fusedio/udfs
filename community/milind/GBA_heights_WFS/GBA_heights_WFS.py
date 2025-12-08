@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    layer_name: str = "global3D:lod1_global",
    max_features: int = 200000,
    use_wms: bool = False
):
    import geopandas as gpd
    import numpy as np
    import requests
    from PIL import Image
    from io import BytesIO
    from shapely.geometry import box
    import time
    
    @fused.cache
    def fetch_wfs_small(bbox, layer, max_feat):
        """Fetch s mall number of WFS features with retry"""
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
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                
                # Parse JSON directly
                import json
                data = json.loads(response.text)
                gdf = gpd.GeoDataFrame.from_features(data['features'], crs='EPSG:4326')
                
                print(f"Success! Fetched {len(gdf)} features")
                print(f"Columns: {list(gdf.columns)}")
                print(gdf.head().T)
                
                return gdf
                
            except Exception as e:
                print(f"Attempt {attempt+1} error: {str(e)[:150]}")
                time.sleep(2 ** attempt)
        
        return gpd.GeoDataFrame()
    
    @fused.cache
    def fetch_wms_styled(bbox, layer, width=256, height=256):
        """Fetch WMS with height-based styling"""
        url = "https://tubvsig-so2sat-vm1.srv.mwn.de/geoserver/ows"
        
        params = {
            'service': 'WMS',
            'version': '1.3.0',
            'request': 'GetMap',
            'layers': layer,
            'bbox': f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]}",
            'width': width,
            'height': height,
            'crs': 'EPSG:4326',
            'format': 'image/png',
            'transparent': 'true'
        }
        
        try:
            response = requests.get(url, params=params, timeout=20)
            
            if 'image' not in response.headers.get('Content-Type', ''):
                print(f"WMS Error: {response.text[:300]}")
                return np.zeros((4, height, width), dtype=np.uint8)
            
            img = Image.open(BytesIO(response.content))
            if img.mode != 'RGBA':
                img = img.convert('RGBA')
            
            arr = np.array(img, dtype=np.uint8)
            arr = np.transpose(arr, (2, 0, 1))
            
            return arr
        except Exception as e:
            print(f"WMS error: {e}")
            return np.zeros((4, height, width), dtype=np.uint8)
    
    if bounds is None:
        return "Zoom to an area to load data"
    
    if use_wms:
        # Use WMS for faster raster display
        arr = fetch_wms_styled(bounds, layer_name)
        return arr, bounds
    else:
        # Use WFS for vector data with attributes
        gdf = fetch_wfs_small(bounds, layer_name, max_features)
        
        if len(gdf) == 0:
            print(f"No features. Server may be overloaded. Try: max_features={max_features//2} or use_wms=True")
            return gpd.GeoDataFrame()
        
        bound_geom = box(*bounds)
        gdf = gdf[gdf.geometry.intersects(bound_geom)]
        
        print(f"{len(gdf)} buildings in view")
        return gdf