# TODO: remove h3 from inner, don't want inner
@fused.cache
def create_h3_buffer_scored(gdf, buffers, remove_inner=False, h3_res=12):
    import pandas as pd
    import h3
    if len(gdf) == 0:
        return pd.DataFrame({})

    # TODO: handle overlapping bridges
    def create_buffer_scores(gdf, buffers):
        gdfs = []
        for buffer_name, buffer in buffers.items():
            _gdf = gdf.copy()
            # Create a buffer
            _gdf = _gdf.to_crs(_gdf.estimate_utm_crs())
            _gdf["geometry"] = _gdf["geometry"].buffer(buffer["distance"])
            _gdf["score"] = buffer["score"]
            _gdf = _gdf.to_crs("EPSG:4326")
            _gdf["buffer_name"] = buffer_name
            gdfs.append(_gdf)
        return pd.concat(gdfs).to_crs('EPSG:4326')

    gdf = create_buffer_scores(gdf, buffers)
    gdf['cell_id'] = gdf.geometry.apply(lambda x: h3.geo_to_cells(x,h3_res))
    gdf_exploded = gdf.explode('cell_id')

    # 5. Group by H3, metrics
    gdf3 = gdf_exploded.dissolve(by='cell_id', as_index=False, aggfunc={
        'id': 'count',
        'score': 'sum'
    })
    gdf3.rename(columns={'id': 'cnt'}, inplace=True)
    return gdf3
    
@fused.cache
def get_strahler_gdf(bounds):
    import ee
    import xarray
    import numpy as np
    import rasterio
    from rasterio import features
    import shapely
    import geopandas as gpd
    import pandas as pd
    VARNAME='b1'

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils

    # Set your own creds
    utils.ee_initialize(service_account_name='fused-nyt-gee@fused-nyt.iam.gserviceaccount.com',key_path="/mnt/cache/gee_creds/fusedbenchmark-513a57ac463f.json")
    geom = ee.Geometry.Rectangle(*bounds.total_bounds)
    ic = ee.ImageCollection("projects/sat-io/open-datasets/HYDROGRAPHY90/stream-order/order_strahler")
    
    ds = xarray.open_dataset(
        ic,
        engine='ee',
        geometry=geom,
        scale=1/2**max(0,bounds.z[0]) 
    )
    ds=ds.max(dim='time') 

    print('vars', list(ds.data_vars.keys()))
    print('coords', ds.coords)


    # Select the first time index (time=0)
    b1_at_time_0 = ds['b1']
    
    # Get the unique values
    unique_values = set(b1_at_time_0.values.flatten())
    print('unique_values', unique_values)
    # return
    x = ds[VARNAME].values
    out = np.unique(x, return_counts=True)
    unique_values, counts = np.unique(x, return_counts=True)
    
    
    arr = ds[VARNAME].values.T
    print(type(arr))
    print(arr.shape)

    xr_data = arr

    # Calculate the affine transformation matrix for the bounding box.
    height, width = xr_data.shape
    transform = rasterio.transform.from_bounds(*bounds.total_bounds, width, height)

    @fused.cache
    def create_strahler_vector(arr):
        gdfs = []
        for strahler_value in range(0,8):
            print(strahler_value)
    
            xr_data = (arr == strahler_value)
            
            # Convert to vector features.
            shapes = features.shapes(
                source=xr_data.astype(np.uint8),
                mask=xr_data.astype(np.uint8),
                transform=transform
            )
            
            gdf = gpd.GeoDataFrame(
                geometry=[shapely.geometry.shape(shape) for shape, shape_value in shapes],
                crs=4326
            )
            gdf['strahler_value'] = strahler_value
    
            if len(gdf) ==0: 
                continue
            else:
                gdfs.append(gdf)
            
    
        print('gdfs', gdfs)
        
        if len(gdfs) ==0: return
        
        gdf = pd.concat(gdfs)
        return gdf
    gdf = create_strahler_vector(arr)
    if len(gdf) ==0: return pd.DataFrame({})
    # Store the area of the polygon as an attribute.
    gdf['area'] = gdf.to_crs(gdf.estimate_utm_crs()).geometry.area
    gdf['geometry'] = gdf['geometry'].buffer(0.0001)
    gdf = gdf[gdf['strahler_value'] != 0]
    return gdf
