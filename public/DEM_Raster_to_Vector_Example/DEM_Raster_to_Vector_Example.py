import numpy as np
import rasterio.transform
import rasterio.features
import shapely
import geopandas as gpd   
from utils import get_dem

@fused.udf
def udf(min_elevation: float = 500):
    overview: int = 5
    bbox = gpd.GeoDataFrame({}, geometry=[shapely.box(-125.08156811704808,32.03901094104144,-113.60059018729473,42.28088093779323)], crs=4326)
    data = get_dem(bbox, overview)
    if data is not None:
        transform = rasterio.transform.from_bounds(*bbox.total_bounds, *data.shape)
        data2 = data > min_elevation
        shapes = rasterio.features.shapes(data2.astype(np.uint8), data2, transform=transform)
    
        geometries = [shapely.geometry.shape(shape) for shape, shape_value in shapes]
        gdf = gpd.GeoDataFrame({}, geometry=geometries, crs=4326)

        gdf = gdf.to_crs(gdf.estimate_utm_crs())
        gdf['area'] = gdf.geometry.area
        gdf = gdf.to_crs(4326)
        gdf['wkt'] = gdf.geometry.apply(shapely.wkt.dumps)
        return gdf
