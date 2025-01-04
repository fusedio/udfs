def get_masked_array(gdf_aoi, arr_aoi):
        import numpy as np 
        from rasterio.transform import from_bounds
        from rasterio.features import geometry_mask            
        transform = from_bounds(*gdf_aoi.total_bounds, arr_aoi.shape[-1], arr_aoi.shape[-2])
        geom_mask = geometry_mask(
                gdf_aoi.geometry,
                transform=transform,
                invert=True,
                out_shape=arr_aoi.shape[-2:],
            )
        masked_value = np.ma.MaskedArray(data=arr_aoi, mask=[~geom_mask])
        return masked_value


def get_da(path, coarsen_factor=1, variable_index=0,  xy_cols=['longitude', 'latitude']):
    # Load data
    import xarray
    path = fused.download(path, path) 
    ds = xarray.open_dataset(path, engine='h5netcdf')
    try:
        var = list(ds.data_vars)[variable_index] 
        print(var)
        if coarsen_factor>1:
            da = ds.coarsen({xy_cols[0]:coarsen_factor, xy_cols[1]:coarsen_factor}, boundary='trim').max()[var]
        else:
            da = ds[var]
        print('done')
        return da 
    except Exception as e:
        print(e) 
        ValueError()
    

def get_da_bounds(da, xy_cols=('longitude','latitude')):
    x_list=da[xy_cols[0]].values
    y_list=da[xy_cols[1]].values
    pixel_width = x_list[1]-x_list[0]
    pixel_height = y_list[1]-y_list[0]
    
    minx = x_list[0]-pixel_width/2 
    maxx = x_list[-1]+pixel_width/2
    miny = y_list[-1]+pixel_height/2
    maxy = y_list[0]-pixel_height/2
    
    return (minx, miny, maxx, maxy)

def clip_arr(arr, bounds_aoi, bounds_total=(-180, -90, 180, 90)): 
    #ToDo: fix antimeridian issue by spliting and merging arr around lng=360
    from rasterio.transform import from_bounds
    transform = from_bounds(*bounds_total, arr.shape[-1], arr.shape[-2])
    if bounds_total[2]>180 and bounds_total[0]>=0:
        print('Normalized longitude for bounds_aoi to (0,360) to match bounds_total')
        bounds_aoi = ((bounds_aoi[0]+360)%360, bounds_aoi[1], 
                      (bounds_aoi[2]+360)%360, bounds_aoi[3])
    x_slice, y_slice = bbox_to_xy_slice(bounds_aoi, arr.shape, transform)
    arr_aoi = arr[y_slice, x_slice]  
    if bounds_total[1]>bounds_total[3]:
        if len(arr_aoi.shape)==3:
            arr_aoi = arr_aoi[:,::-1]
        else:
            arr_aoi = arr_aoi[::-1]
    return  arr_aoi

def bbox_to_xy_slice(bounds, shape, transform):
    import rasterio
    from affine import Affine

    if transform[4] < 0:  # if pixel_height is negative
        original_window = rasterio.windows.from_bounds(*bounds, transform=transform)
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        y_slice, x_slice = gridded_window.toslices()
        return x_slice, y_slice
    else:  # if pixel_height is not negative
        original_window = rasterio.windows.from_bounds(
            *bounds,
            transform=Affine(
                transform[0],
                transform[1],
                transform[2],
                transform[3],
                -transform[4],
                -transform[5],
            ),
        )
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        y_slice, x_slice = gridded_window.toslices()
        y_slice = slice(shape[0] - y_slice.stop, shape[0] - y_slice.start + 0)
        return x_slice, y_slice
