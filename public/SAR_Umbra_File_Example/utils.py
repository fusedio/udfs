def rio_transform_bbox(raster_url, geo_extend, do_tranform=True, overview_level=None):
    """This function takes the image and the extend polygon and returns the transformed image and the bbox"""
    import numpy as np
    import rasterio
    import rasterio.warp
    import shapely

    # Open the downloaded raster dataset with SRC_METHOD=NO_GEOTRANSFORM option
    with rasterio.open(raster_url, OVERVIEW_LEVEL=overview_level) as src:
        # Get the width and height of the raster
        width, height = src.width, src.height
        arr = src.read()
        if not do_tranform:
            return arr, geo_extend.bounds

        # Transform using affine based on geo_extend to bbox_bounds
        crs = 32618
        dst_shape = src.height, src.width
        destination_data = np.zeros(dst_shape, src.dtypes[0])

        # Extract the four corner coordinates
        corners = list(geo_extend.exterior.coords)
        lower_left_corner = corners[4]
        upper_left_corner = corners[3]
        upper_right_corner = corners[2]
        lower_right_corner = corners[1]
        print(
            lower_left_corner, upper_left_corner, upper_right_corner, lower_right_corner
        )
        # Create GCPs from the corner coordinates
        gcps = [
            rasterio.control.GroundControlPoint(row, col, x=lon, y=lat, z=z)
            for (row, col), (lon, lat, z) in zip(
                [(0, 0), (height - 1, 0), (height - 1, width - 1), (0, width - 1)],
                [
                    upper_left_corner,
                    lower_left_corner,
                    lower_right_corner,
                    upper_right_corner,
                ],
            )
        ]
        # Create a transformation from GCPs
        transform_gcps = rasterio.transform.from_gcps(gcps)
        # Create a transformation from destination bbox (bounds of orig geom)
        minx, miny, maxx, maxy = geo_extend.bounds
        # minx, miny, maxx, maxy = rasterio.warp.transform_bounds(crs, 4326, *geo_extend.bounds)
        dy = (maxx - minx) / src.height
        dx = (maxy - miny) / src.width
        transform = [dx, 0.0, minx, 0.0, -dy, maxy]

        print(transform_gcps)
        print(transform)
        # print(src.crs)
        # print(src.bounds)
        with rasterio.Env():
            rasterio.warp.reproject(
                arr.squeeze()[::-1, ::-1],
                destination_data,
                src_transform=transform_gcps,  # from georefernce points
                dst_transform=transform,  # to transform of the dest bbox which is the bounds of the geom
                src_crs=crs,
                dst_crs=crs,  # Assuming the same CRS
                resampling=rasterio.enums.Resampling.nearest,  # Adjust as needed
            )
    return destination_data, geo_extend.bounds


CATALOG = {
    "washington": {
        "meta_url": "http://umbra-open-data-catalog.s3.amazonaws.com/stac/2023/2023-12/2023-12-22/1a74966a-1dbf-4241-abc4-01b43a519b9a/1a74966a-1dbf-4241-abc4-01b43a519b9a.json",
        "tiff_url": "https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/ad%20hoc/WashingtonDC_50cm_12xML/4b4886d7-35a1-43c8-b114-546eb87b2f74/2023-12-22-02-58-04_UMBRA-08/2023-12-22-02-58-04_UMBRA-08_GEC.tif",
        "rotation": -90,
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
        "rotation": -90,
    },
}
