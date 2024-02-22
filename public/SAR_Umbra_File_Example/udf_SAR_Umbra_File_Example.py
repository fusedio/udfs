##todo: need to rotate to match with actual bbox
@fused.udf
def udf(raster_url='https://www.historicalcharts.noaa.gov/jpgs/NYH.jpg', 
        overview_level=1, 
        do_tranform=True):
    import geopandas as gpd
    meta_url='https://s3.us-west-2.amazonaws.com/umbra-open-data-catalog/stac/2023/2023-04/2023-04-30/657068a8-6237-452d-bfdc-d535df37900b/657068a8-6237-452d-bfdc-d535df37900b.json'
    tiff_url = 'https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/ad%20hoc/Washington,%20DC/Washington,%20DC%20/672ad2cf-2ba7-4774-a4f4-023c0767bef8/2023-04-30-14-49-27_UMBRA-05/2023-04-30-14-49-27_UMBRA-05_GEC.tif'
    df = gpd.read_file(meta_url).simplify(.00001).to_crs(32618)
    print(df.T)
    from shapely import wkb
    # _drop_z = lambda geom: wkb.loads(wkb.dumps(geom, output_dimension=2))
    # geo_extend = df.geometry.transform(_drop_z).values[0]
    geo_extend = df.geometry.values[0]
    from utils import rio_transform_bbox, arr_to_plasma
    import shapely
    print(geo_extend)
    geo_extend = shapely.affinity.rotate(geo_extend, 1.5)
    arr, bbox = rio_transform_bbox(tiff_url, geo_extend, do_tranform=do_tranform, overview_level=overview_level)
    print(arr.shape)
    bbox = gpd.GeoDataFrame(geometry=[shapely.box(*bbox)],crs='32618').to_crs(4326).buffer(-0.0005).total_bounds
    return arr.squeeze(), bbox 
    # return arr_to_plasma(arr.squeeze(),min_max=(10,180)), bbox