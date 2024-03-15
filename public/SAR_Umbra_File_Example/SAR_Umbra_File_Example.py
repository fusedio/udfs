@fused.udf
def udf(name="colombia_taparal"):

    import geopandas as gpd
    import shapely
    from utils import CATALOG, rio_transform_bbox

    @fused.cache
    def get_image(meta_url, tiff_url, overview_level=1, do_tranform=True, rotation=0):
        df = gpd.read_file(meta_url).simplify(0.00001).to_crs(32618)
        from shapely import wkb

        geo_extend = df.geometry.values[0]
        geo_extend = shapely.affinity.rotate(geo_extend, rotation)
        arr, bbox = rio_transform_bbox(
            tiff_url, geo_extend, do_tranform=do_tranform, overview_level=overview_level
        )
        return arr, bbox

    arr, bbox = get_image(
        meta_url=CATALOG[name]["meta_url"],
        tiff_url=CATALOG[name]["tiff_url"],
        overview_level=1,
        rotation=CATALOG[name]["rotation"],
    )

    bounds = (
        gpd.GeoDataFrame(geometry=[shapely.box(*bbox)], crs="32618")
        .to_crs(4326)
        .buffer(-0.0005)
        .total_bounds
    )
    return arr.squeeze(), bounds
