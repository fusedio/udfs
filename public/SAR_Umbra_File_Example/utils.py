
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
        arr=src.read()
        if not do_tranform:
            return arr, geo_extend.bounds
        
        # Transform using affine based on geo_extend to bbox_bounds
        crs=32618
        dst_shape = src.height, src.width
        destination_data = np.zeros(dst_shape, src.dtypes[0])
        
        # Extract the four corner coordinates
        corners = list(geo_extend.exterior.coords)
        lower_left_corner = corners[4]
        upper_left_corner = corners[3]
        upper_right_corner = corners[2]
        lower_right_corner = corners[1]
        print(lower_left_corner, upper_left_corner, upper_right_corner, lower_right_corner)
        # Create GCPs from the corner coordinates
        gcps = [
            rasterio.control.GroundControlPoint(row, col, x=lon, y=lat, z=z)
            for (row, col), (lon, lat, z) in zip(
                [(0, 0), (height - 1, 0), (height - 1, width - 1), (0, width - 1)],
                [upper_left_corner, lower_left_corner, lower_right_corner, upper_right_corner])
        ]
        # Create a transformation from GCPs
        transform_gcps = rasterio.transform.from_gcps(gcps)
        # Create a transformation from destination bbox (bounds of orig geom)        
        minx, miny, maxx, maxy = geo_extend.bounds
        # minx, miny, maxx, maxy = rasterio.warp.transform_bounds(crs, 4326, *geo_extend.bounds)
        dy = (maxx-minx)/src.height
        dx = (maxy-miny)/src.width
        transform=[dx, 0.0, minx, 0., -dy, maxy]
        
        print(transform_gcps)
        print(transform)
       # print(src.crs)
        # print(src.bounds)
        with rasterio.Env():
            rasterio.warp.reproject(
                arr.squeeze()[::-1,::-1],
                destination_data,
                src_transform=transform_gcps, #from georefernce points
                dst_transform=transform, #to transform of the dest bbox which is the bounds of the geom
                src_crs=crs,
                dst_crs=crs,  # Assuming the same CRS
                resampling=rasterio.enums.Resampling.nearest  # Adjust as needed
                )
    return destination_data, geo_extend.bounds
