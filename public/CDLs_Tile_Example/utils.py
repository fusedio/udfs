def read_tiff(bbox, input_tiff_path, filter_list=None, output_shape=(256,256), overview_level=None, return_colormap=False, aws_session=None):
    import os
    import rasterio
    import numpy as np
    from scipy.ndimage import zoom
    from contextlib import ExitStack
    if not aws_session:
        context = rasterio.Env()
    else:
        context = rasterio.Env(aws_session,
                      GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',
                      GDAL_HTTP_COOKIEFILE=os.path.expanduser('/tmp/cookies.txt'),
                      GDAL_HTTP_COOKIEJAR=os.path.expanduser('/tmp/cookies.txt'))  
    with ExitStack() as stack:
        stack.enter_context(context)
        with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:
        # with rasterio.Env():
            from rasterio.warp import reproject, Resampling
            bbox = bbox.to_crs(3857)
            # transform_bounds = rasterio.warp.transform_bounds(3857, src.crs, *bbox["geometry"].bounds.iloc[0])            
            window = src.window(*bbox.to_crs(src.crs).total_bounds)
            original_window = src.window(*bbox.to_crs(src.crs).total_bounds)
            gridded_window = rasterio.windows.round_window_to_full_blocks(original_window, [(1, 1)])
            window=gridded_window # Expand window to nearest full pixels
            source_data =src.read(window=window, boundless=True)
            if filter_list:
                mask = np.isin(source_data, filter_list, invert=True)
                source_data[mask] = 0
            src_transform=src.window_transform(window)
            src_crs = src.crs
            minx, miny, maxx, maxy = bbox.total_bounds
            d = (maxx-minx)/output_shape[-1]
            dst_transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
            dst_shape = output_shape
            dst_crs = bbox.crs

            
            destination_data = np.zeros(dst_shape, src.dtypes[0])
            if return_colormap:
                colormap = src.colormap(1)
            reproject(
                source_data,
                destination_data,
                src_transform=src_transform,
                src_crs=src_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                #TODO: rather than nearest, get all the values and then get pct
                resampling=Resampling.nearest)
    if return_colormap:
        colormap[0]=[0,0,0,0]
        return destination_data, colormap
    else:
        return destination_data

def filter_crops(arr, crop_type, verbose=True):
        import numpy as np
        values_to_keep = crop_to_int(crop_type, verbose=verbose)
        if len(values_to_keep)>0:
            mask = np.isin(arr, values_to_keep, invert=True)
            arr[mask] = 0
            return arr
        else:
            print(f'{crop_type=} was not found in the list of crops')
            return arr

def crop_counts(arr):
    import numpy as np
    import pandas as pd
    import requests
    unique_values, counts = np.unique(arr, return_counts=True)
    df = pd.DataFrame(counts, index=unique_values,columns=['n_pixel']).sort_values(by='n_pixel', ascending=False)
    url = 'https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json'
    df_meta = pd.DataFrame(requests.get(url).json()['summaries']['eo:bands'][0]['gee:classes'])
    df = df_meta.set_index('value').join(df)[['description','color','n_pixel']]
    df['color']='#'+df['color']
    df.columns=['crop_type','color','n_pixel']
    df = df.sort_values('n_pixel', ascending=False)
    return df.dropna()

def crop_to_int(crop_type='corn', verbose=True):
    import pandas as pd
    import requests
    url = 'https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json'
    df_meta = pd.DataFrame(requests.get(url).json()['summaries']['eo:bands'][0]['gee:classes'])
    mask = df_meta.description.map(lambda x:crop_type.lower() in x.lower())
    df_meta_masked = df_meta[mask]
    if verbose:
        print(f'{df_meta_masked=}')
        print('crop type options', df_meta.description.values)
    return df_meta_masked.value.values