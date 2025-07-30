@fused.udf
@fused.cache(path="read_tiff2")
def udf(west="-120.485537", south="34.879334",  east="-120.400163", north="34.951613",zoom="15", year="2022",crop_type=""):
    north=float(north);south=float(south);east=float(east);west=float(west);year=int(year);zoom=int(zoom);
    import numpy as np
    import rasterio
    import geopandas as gpd
    import matplotlib.pyplot as plt
    import numpy as np
    import shapely
    from matplotlib import colors
    from rasterio.warp import reproject, Resampling 
    
    bounds = [west, south, east,north]
    bbox = bounds_to_gdf(bounds)
    
    input_tiff_path = f"s3://fused-asset/data/cdls/{year}_30m_cdls.tif"
    
    with rasterio.open(input_tiff_path) as src:
        bbox_dist = bbox.to_crs(3857)
        dst_crs = bbox_dist.crs
        original_window = src.window(*bbox_dist.to_crs(src.crs).total_bounds)
        gridded_window = rasterio.windows.round_window_to_full_blocks(
                original_window, [(1, 1)]
            )
        window = gridded_window  # Expand window to nearest full pixels
        source_data = src.read(window=window, boundless=True, masked=True)
        nodata_value = src.nodatavals[0]
        src_transform = src.window_transform(window)
        src_crs = src.crs
        color_map = src.colormap(1)
        minx, miny, maxx, maxy = bbox_dist.total_bounds
        dx = (maxx - minx) 
        dy = (maxy - miny)
        resolution = src.res[0]
        print('resolution is:',resolution)
        x_diff = int(dx / resolution)
        y_diff = int(dy / resolution)
        print('pixel dimensions:',x_diff, y_diff)
        if zoom == 14:
            dst_transform = [resolution/3, 0.0, minx, 0.0, -1*resolution/3, maxy, 0.0, 0.0, 1.0]
            destination_data = np.zeros((3*y_diff, 3*x_diff), src.dtypes[0])
            reproject(
                    source_data,
                    destination_data,
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest,
                )
        elif zoom == 15:
            dst_transform = [resolution/6, 0.0, minx, 0.0, -1*resolution/6, maxy, 0.0, 0.0, 1.0]
            destination_data = np.zeros((6*y_diff, 6*x_diff), src.dtypes[0])
            reproject(
                    source_data,
                    destination_data,
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest,
                )
    arr = destination_data
    print('destination array size d:',arr.shape)
    return np.array(arr,'uint8'), bbox.total_bounds



def bounds_to_gdf(bounds_list, crs=4326):
    import shapely
    import geopandas as gpd
    box = shapely.box(*bounds_list) 
    return gpd.GeoDataFrame(geometry=[box], crs=crs)


