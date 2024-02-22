@fused.udf
def udf(bbox, thresh=10):
    import geopandas as gpd
    import shapely
    # x,y,z = bbox.iloc[0][['x','y','z']].values
    minx, miny, maxx, maxy = bbox.to_crs(4326).total_bounds
    size=256
    url = f"https://ic.imagery1.arcgis.com/arcgis/rest/services/Sentinel2_10m_LandCover/ImageServer/exportImage?f=image&bbox={minx}%2C{miny}%2C{maxx}%2C{maxy}&bboxSR=4326&imageSR=3857&size={size}%2C{size}&format=tiff"
    print(url)
    import requests
    import rasterio
    from rasterio.plot import show
    from io import BytesIO
    response = requests.get(url)
    print(response.status_code)
    if response.status_code == 200:
        with rasterio.open(BytesIO(response.content)) as dataset:
            arr = dataset.read()[0]
    print(arr.shape)
    return (arr*10).astype('uint8')