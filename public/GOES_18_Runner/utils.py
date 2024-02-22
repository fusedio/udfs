import fused
@fused.cache
def cache_valid_list(roi, product_name, bucket_name, chunk_size): 
    from utils import file_of_day, chunkify, roi_ds_chunks
    import rioxarray
    file_path = file_of_day(0, bucket_name, product_name, band, '2024-01-01') 
    print(file_path)
    url=f'https://{bucket_name}.s3.amazonaws.com/{file_path}'
    xds = rioxarray.open_rasterio(url)
    xy_size = (len(xds.x),len(xds.y))
    slice_list = chunkify(chunk_size=chunk_size, xy_size=xy_size, pad=10)
    valid_list = roi_ds_chunks(roi, xds, slice_list, return_data=False) 
    return valid_list  
    
def file_of_day(i, bucket_name, product_name, band, start_datestr='2024-01-30', hour_offset='7'):
    if product_name in ('ABI-L1b-RadF', 'ABI-L2-CMIPF'):
        nfile=6
    elif product_name in ('ABI-L1b-RadC', 'ABI-L2-CMIPC'):
        nfile=12
    else:
        print(f'{product_name} not in ABI-L1b-RadF | ABI-L1b-RadC | ABI-L2-CMIPF')

    import datetime
    import numpy as np
    import s3fs
    start_time = int(datetime.datetime.strptime(start_datestr, '%Y-%m-%d').timestamp())
    hourly_timestamps = []
    hour = i//nfile
    hour_timestamp = start_time + hour * 3600  
    start_datetime = datetime.datetime.strptime(start_datestr, '%Y-%m-%d')
    hour_datetime = start_datetime + datetime.timedelta(hours=hour+int(hour_offset))
    day_of_year = hour_datetime.timetuple().tm_yday
    hourly_timestamps.append([hour_datetime.year, day_of_year, hour_datetime.hour])
    blob = f'{product_name}/{hour_datetime.year}/{day_of_year:03.0f}/{hour_datetime.hour:02.0f}'
    prefix = f'OR_{product_name}-M6C{band:02.0f}'
    s3 = s3fs.S3FileSystem(anon=True)
    flist = s3.ls(f'{bucket_name}/{blob}')
    flist = np.sort([i for i in flist if i.split('/')[-1].startswith(prefix)])
    return flist[i%nfile][len(product_name):]


def chunkify(chunk_size, xy_size, pad=10):
    """
    e.g. slice_list = chunkify(chunk_size=(500,500), xy_size=(1200,2000), pad=10)
    """
    x_len, y_len = xy_size
    # Calculate the number of chunks along each dimension
    n_chunks_x = x_len // (chunk_size[1] - pad) + 1
    n_chunks_y = y_len // (chunk_size[0] - pad) + 1
    xy_slices = []
    # Create chunks
    for i in range(n_chunks_y):
        for j in range(n_chunks_x):
            start_idx_x = j * (chunk_size[1] - pad)
            end_idx_x = min(start_idx_x + chunk_size[1], x_len)
            start_idx_y = i * (chunk_size[0] - pad)
            end_idx_y = min(start_idx_y + chunk_size[0],  y_len)
            xy_slices.append([slice(start_idx_x,end_idx_x), slice(start_idx_y,end_idx_y)])
    return xy_slices

def roi_ds_chunks(roi, xds, slice_list, valid_list=[], return_data=True):
    import numpy as np
    if len(valid_list)>0:
        slice_list=valid_list
    ds_list=[]
    for xy_slice in slice_list:
        try:
            ds = xds.isel(x=xy_slice[0], y=xy_slice[1])
            ds_list.append(ds.rio.reproject(roi.crs).rio.clip(roi.geometry))
            if xy_slice not in valid_list:
                valid_list.append(xy_slice)
            print(xy_slice)
        except Exception as e:
            print(xy_slice, e)
    if return_data:
        return ds_list
    else: 
        return valid_list

def combine_resample(ds_chunks,variable='CMI', xy_res=(2000,2000)):
    import numpy as np
    minx, miny = np.min([[i.x.min().values,i.y.min().values] for i in ds_chunks],axis=0)
    maxx, maxy = np.max([[i.x.max().values,i.y.max().values] for i in ds_chunks],axis=0)
    xrange = np.arange(minx, maxx, xy_res[0])
    # Nonte: y axis need to be top to bottom
    yrange = np.arange(maxy, miny, -xy_res[1])
    da_interp=[i.interp(x=xrange,y=yrange)[variable] for i in ds_chunks]
    #TODO: if in the jupyter-notebook change the nanmin to nanmax
    da_interp[0][:]=np.nanmin(np.stack(da_interp), axis=0)
    return da_interp[0]
        
#todo: use arr_to_color in arr_to_plasma
def arr_to_plasma(data, min_max=(0,255), colormap=''): 
    import numpy as np 
    data = data.astype(float)
    if min_max:
        norm_data = (data - min_max[0])/(min_max[1]-min_max[0])
        norm_data = np.clip(norm_data,0,1)           
    else:
        print(f'min_max:({round(np.nanmin(data),3)},{round(np.nanmax(data),3)})')
        norm_data = (data - np.nanmin(data))/(np.nanmax(data)-np.nanmin(data))            
    norm_data255 = (norm_data*255).astype('uint8')
    if colormap:
        #ref: https://matplotlib.org/stable/users/explain/colors/colormaps.html
        from matplotlib import colormaps
        colormap=[(np.array(colormaps[colormap](i)[:3])*255).astype('uint8') for i in range(256)][::-1]
        mapped_colors = np.array([colormap[val] for val in norm_data255.flat])
        return mapped_colors.reshape(data.shape+(3,)).astype('uint8').transpose(2,0,1)
    else:
        return norm_data255


