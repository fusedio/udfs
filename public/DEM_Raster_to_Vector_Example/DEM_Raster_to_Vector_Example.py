import geopandas as gpd  
import numpy as np
import rasterio
from rasterio import features
import shapely
import utils

@fused.udf
def udf(
    min_elevation: float = 3962  # 3962 ~= 13000ft
):
    """Return polygons for Colorado areas over 13,000ft of elevation."""
    bounds = gpd.GeoDataFrame(
        geometry=[shapely.box(-109.046667, 37.0, -102.046667, 41.0)],
        crs=4326
    )
    
    xr_data = utils.get_dem(bounds)

    # Calculate the affine transformation matrix for the bounding box.
    height, width = xr_data.shape
    transform = rasterio.transform.from_bounds(*bounds.total_bounds, width, height)

    # Create a binary image showing where the elevation threshold is exceeded.
    xr_data2 = (xr_data > min_elevation)
    
    # Convert to vector features.
    shapes = features.shapes(
        source=xr_data2.astype(np.uint8),
        mask=xr_data2,
        transform=transform
    )
    
    gdf = gpd.GeoDataFrame(
        geometry=[shapely.geometry.shape(shape) for shape, shape_value in shapes],
        crs=4326
    )

    # Store the Well Known Text (WKT) representation of the polygon as an attribute.
    gdf['wkt'] = gdf.geometry.apply(shapely.wkt.dumps)

    # Store the area of the polygon as an attribute.
    gdf['area'] = gdf.to_crs(gdf.estimate_utm_crs()).geometry.area
    
    return gdf
