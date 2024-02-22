import numpy as np
LULC_IO_COLORS = {1:( 65, 155, 223), #Water
                  2:( 57, 125, 73), #Trees
                  4:( 57, 125, 73), #Flooded vegetation
                  5:( 228, 150, 53), #Crops
                  7:(196, 40, 27), #Built area
                  8:(165, 155, 143), #Bare ground
                  9:(168, 235, 255), #Snow
                  10:(97, 97, 97), #Clouds
                  11:(227, 226, 195), #Rangeland
                  }

def s3_to_https(path):
    arr = path[5:].split('/')
    out = 'https://'+arr[0]+'.s3.amazonaws.com/'+'/'.join(arr[1:])
    return out

def arr_to_color(arr, color_map={1:( 65, 155, 223 ), 2:( 57, 125, 73 )}):
    import numpy as np
    def map_values(value, band=0):
        if value in color_map:
            return color_map[value][band]
        else:
            return 0
    mapped_arr = [np.vectorize(lambda x:map_values(x,i))(arr) for i in [0,1,2]]
    return np.asarray(mapped_arr).astype('uint8')

def bbox_stac_items(bbox, table):
    import fused
    import pandas as pd
    df = fused.utils.get_chunks_metadata(table)
    df = df[df.intersects(bbox)]
    List = df[['file_id','chunk_id']].values
    rows_df = pd.concat([fused.utils.get_chunk_from_table(table, fc[0], fc[1]) for fc in List])
    return rows_df[rows_df.intersects(bbox)]

def read_tiff(bbox, input_tiff_path, output_shape=(256,256), overview_level=None, buffer_degree=0.000, resample_order=0):
    import rasterio
    import numpy as np
    if buffer_degree != 0:
        bbox.geometry=bbox.geometry.buffer(buffer_degree)
    import rasterio
    with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:            
        bbox = bbox.to_crs(3857)
        transform_bounds = rasterio.warp.transform_bounds(3857, src.crs, *bbox["geometry"].bounds.iloc[0])            
        from rasterio.warp import reproject, Resampling
        bbox_buffered = bbox.to_crs(src.crs)
        buffer=0.25*(bbox_buffered.total_bounds[2] - bbox_buffered.total_bounds[0])
        bbox_buffered = bbox_buffered.geometry.buffer(buffer)
        window = src.window(*bbox_buffered.total_bounds)
        source_data =src.read(window=window, boundless=True)
        src_transform=src.window_transform(window)
        minx, miny, maxx, maxy = bbox.total_bounds
        d = (maxx-minx)/output_shape[-1]
        dst_transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
        with rasterio.Env():
            src_crs = src.crs
            dst_shape = output_shape
            dst_crs = bbox.crs
            destination_data = np.zeros(dst_shape, src.dtypes[0])
            reproject(
                source_data,
                destination_data,
                src_transform=src_transform,
                src_crs=src_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=Resampling.nearest)
    arr = destination_data
    return arr

def mosaic_tiff(bbox, tiff_list, reduce_function=lambda x:np.max(x, axis=0), output_shape=(256,256), overview_level=None, buffer_degree=0.000, resample_order=0):
    import numpy as np
    a=[]
    for input_tiff_path in tiff_list:
        if not input_tiff_path: continue 
        a.append(read_tiff(bbox, input_tiff_path, output_shape, overview_level, buffer_degree, resample_order))
    if len(a)==1:
        data=a[0]
    else:
        data = reduce_function(a)
    return data
 
