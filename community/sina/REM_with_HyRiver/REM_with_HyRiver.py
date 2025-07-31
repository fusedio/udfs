@fused.udf
def udf(bounds= [-119.59, 39.24, -119.47, 39.30] ):
    import nest_asyncio
    import numpy as np
    import py3dep
    import shapely.ops as ops
    import xarray as xr
    import xrspatial as xs
    from datashader import transfer_functions as tf
    from datashader.colors import Greys9, inferno
    nest_asyncio.apply()
    
    @fused.cache
    def fn(bounds, res=10):
        dem = py3dep.get_dem(bounds, res)
        flw = get_osm_waterways(bounds)
        if 'waterway' in flw.columns:
            main_rivers = flw[flw.waterway.isin(['river', 'stream'])]
            if len(main_rivers) > 0:
                flw = main_rivers
        
        lines = ops.linemerge(flw.geometry.tolist())
        if hasattr(lines, 'geoms'):
            lines = max(lines.geoms, key=lambda x: x.length)
        elif not hasattr(lines, 'coords'):
            lines = max(flw.geometry, key=lambda x: x.length)
            
        riv_dem = py3dep.elevation_profile(lines, 10, crs=flw.crs)
        elevation = idw(riv_dem, dem, 20)
        rem = dem - elevation
        return rem, dem
    
    bounds = (-119.59, 39.24, -119.47, 39.30) if bounds is None else bounds
    rem, dem = fn(bounds)
    illuminated = xs.hillshade(dem, angle_altitude=10, azimuth=90)
    tf.Image.border = 0
    img = tf.stack(
        tf.shade(dem, cmap=Greys9, how="linear"),
        tf.shade(illuminated, cmap=["black", "white"], how="linear", alpha=180),
        tf.shade(rem, cmap=inferno[::-1], span=[0, 7], how="log", alpha=200),
    )
    return img[::-1]

def get_osm_waterways(bounds):
    import geopandas as gpd
    import requests
    from shapely.geometry import LineString
    
    west, south, east, north = bounds
    overpass_query = f"""
    [out:json][timeout:25];
    (
      way["waterway"~"^(river|stream|creek|canal)$"]({south},{west},{north},{east});
    );
    out geom;
    """
    
    response = requests.get(
        "http://overpass-api.de/api/interpreter",
        params={'data': overpass_query},
        timeout=30
    )
    data = response.json()
    
    features = []
    for element in data.get('elements', []):
        if element['type'] == 'way' and 'geometry' in element:
            coords = [(node['lon'], node['lat']) for node in element['geometry']]
            if len(coords) >= 2:
                features.append({
                    'geometry': LineString(coords),
                    'waterway': element.get('tags', {}).get('waterway', 'unknown'),
                    'name': element.get('tags', {}).get('name', '')
                })
    
    return gpd.GeoDataFrame(features, crs='EPSG:4326')

import numpy as np
import xarray as xr
from scipy.spatial import KDTree

def idw(riv_dem: xr.DataArray, dem: xr.DataArray, n_nb: int) -> xr.DataArray:
    riv_coords = np.column_stack((riv_dem.x, riv_dem.y))
    kdt = KDTree(riv_coords)
    dem_grid = np.dstack(np.meshgrid(dem.x, dem.y)).reshape(-1, 2)
    distances, idx = kdt.query(dem_grid, k=n_nb)
    weights = np.reciprocal(distances)
    weights = weights / weights.sum(axis=1, keepdims=True)
    interp = weights * riv_dem.to_numpy()[idx]
    interp = interp.sum(axis=1).reshape((dem.sizes["y"], dem.sizes["x"]))
    return xr.DataArray(interp, dims=("y", "x"), coords={"x": dem.x, "y": dem.y})