@fused.udf
def udf(
    min_elevation: float = 3962  # 3962 ~= 13000ft
):
    import geopandas as gpd
    import numpy as np
    import rasterio
    from rasterio import features
    import shapely
    import odc.stac
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
       
    """Return polygons for Colorado areas over 13,000ft of elevation."""
    bounds = gpd.GeoDataFrame(
        geometry=[shapely.box(-109.046667, 37.0, -102.046667, 41.0)],
        crs=4326
    )
    
    xr_data = get_dem(bounds)

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


@fused.cache
def get_dem(bounds):
    import odc.stac
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    collection = 'cop-dem-glo-90'
    # Reduce the resolution to get a quicker, but less accurate, results.
    resolution = 90*2

    odc.stac.configure_s3_access(aws_unsigned=True)
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")

    items = catalog.search(
        collections=[collection],
        bbox=bounds.total_bounds,
    ).item_collection() 
    
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=["data"],
        resolution=resolution,
        bounds=bounds.total_bounds,
    ).astype(float)
    xr_data = ds["data"].max(dim="time")
    return xr_data
