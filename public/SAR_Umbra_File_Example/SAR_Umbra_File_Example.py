@fused.udf
def udf(name="colombia_taparal"):

    rotation = 90

    CATALOG = {
        "washington": {
            "meta_url": "http://umbra-open-data-catalog.s3.amazonaws.com/stac/2023/2023-12/2023-12-22/1a74966a-1dbf-4241-abc4-01b43a519b9a/1a74966a-1dbf-4241-abc4-01b43a519b9a.json",
            "tiff_url": "https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/ad%20hoc/WashingtonDC_50cm_12xML/4b4886d7-35a1-43c8-b114-546eb87b2f74/2023-12-22-02-58-04_UMBRA-08/2023-12-22-02-58-04_UMBRA-08_GEC.tif",
            "rotation": 90,
        },
        "panama_canal": {
            "meta_url": "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/stac/2023/2023-04/2023-04-16/5e71b39b-6f8b-46e4-a4f5-3225216b24e3/5e71b39b-6f8b-46e4-a4f5-3225216b24e3.json",
            "tiff_url": "https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/Panama%20Canal,%20Panama/9b985407-6ed5-4bfe-be48-93b3a1c394af/2023-04-16-02-26-41_UMBRA-04/2023-04-16-02-26-41_UMBRA-04_GEC.tif",
            "rotation": 90,
        },
        "mexico_sanmartin": {
            "meta_url": "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/stac/2024/2024-02/2024-02-14/22c9f410-43e1-4e62-957c-ac3cd2d2cfa3/22c9f410-43e1-4e62-957c-ac3cd2d2cfa3.json",
            "tiff_url": "https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/ad%20hoc/San_Martin_de_las_piramides_Mexico/44f7aae4-0ca5-4a27-b733-12965da9f458/2024-02-14-16-11-28_UMBRA-04/2024-02-14-16-11-28_UMBRA-04_GEC.tif",
            "rotation": -90,
        },
        "colombia_taparal": {
            "meta_url": "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/stac/2024/2024-02/2024-02-09/a0090fe3-7c8f-495d-bb3f-e40f9293ef7b/a0090fe3-7c8f-495d-bb3f-e40f9293ef7b.json",
            "tiff_url": "https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/Taparal,%20Colombia/3b906f59-63e9-42e3-b6c1-ef574943a9f9/2024-02-09-03-18-50_UMBRA-06/2024-02-09-03-18-50_UMBRA-06_GEC.tif",
            "rotation": 90,
        },
        "mexico_acapulco": {
            "meta_url": "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/stac/2023/2023-10/2023-10-25/c9cdc9d2-70b3-4d83-9cb4-6848a5a80477/c9cdc9d2-70b3-4d83-9cb4-6848a5a80477.json",
            "tiff_url": "https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/ad%20hoc/Acapulco%20Flooding/328c176c-f99a-4e48-b08d-0be0021c4d86/2023-10-25-04-29-02_UMBRA-04/2023-10-25-04-29-02_UMBRA-04_GEC.tif",
            "rotation": 110,
        },
    }

    import geopandas as gpd
    import shapely
    from utils import rio_transform_bbox

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
