@fused.udf
def udf(engine='realtime', year: str = "2016", month: str = "07", period: str ="a"):
    import geopandas as gpd
    import pandas as pd
    
    # Get the geometry
    @fused.cache
    def load(crop_type='corn'):
        
        gdf_counties = gpd.read_parquet('s3://fused-asset/data/tiger/county/tl_rd22_us_county small.parquet')
        target_counties = gdf_counties[gdf_counties['GEOID'].str.startswith(('19'))].GEOID.values

        # Create array of all parameter combinations
        all_params = [{'geoid': geoid, 'year': year, 'crop_type': crop_type} for geoid in target_counties]

        # Load pinned versions of utility functions.
        common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

        # Run UDF in parallel
        @fused.cache
        def hammer_udf(params):
            version="1.0"
            geoid = params['geoid']
            year = params['year']
            crop_type = params['crop_type']
            df = fused.run(udf_nail, geoid=geoid, year=year, month=month, period=period, crop_type=crop_type, engine=engine)
            return df
    
        dfs_out = common.run_pool(hammer_udf, all_params, max_workers=64)
    
        # Concatenate all output tables
        gdf = pd.concat(dfs_out)
        return gdf
    gdf = load('corn')
    gdf['metric']= gdf['county_sif_sum']/gdf['corn_sif_sum']
    return gdf


# Rename `udf` to `udf_nail` to run in loop
@fused.udf
def udf(crop_type ="corn", geoid: str = '19119', year: str = "2015", month: str = "08", period: str ="b", output_suffix='out1'):
    from skimage.transform import resize
    import geopandas as gpd
    import pandas as pd
    import numpy as np
    import pandas as pd
    xy_cols=['lon','lat']
    
    # Get the geometry
    gdf = gpd.read_parquet('s3://fused-asset/data/tiger/county/tl_rd22_us_county 25pct.parquet')
    gdf['geometry'] = gdf['geometry'].buffer(0)
    gdf = gdf[gdf['GEOID'] == geoid]
    area=gdf.to_crs(gdf.estimate_utm_crs()).area.round(2)
    print(area)

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    # Get box around the geometry
    geom_bbox = common.geo_bbox(gdf)
    
    # Dynamically construct the path based on the year and month
    path = f's3://soldatanasasifglobalifoco2modis1863/Global_SIF_OCO2_MODIS_1863/data/sif_ann_{year}{month}{period}.nc'
    
    # Load SIF Array
    da = get_da(path, coarsen_factor=1, variable_index=0, xy_cols=xy_cols)
    
    # Clip the array to the RECTANGULAR area of interest (AOI)
    arr_aoi = clip_arr(da.values, bounds_aoi=geom_bbox.total_bounds, bounds_total=get_da_bounds(da, xy_cols=xy_cols))
    img = (arr_aoi * 255).astype('uint8') # TODO: perhaps change to improve magnitudes
    
    # Load crop UDF
    udf = fused.load('https://github.com/fusedio/udfs/tree/2ea46f3/public/CDLs_Tile_Example/')
    arr_crop = fused.run(udf, engine='local', colored=False, bounds=geom_bbox, year= year, crop_type=crop_type)
    sif_resized = resize(img, (arr_crop.shape[0],arr_crop.shape[1]), anti_aliasing=True, preserve_range=True).astype('uint8')

    # Sif for entire county prior to corn mask
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    county_geom_mask = common.gdf_to_mask_arr(gdf, sif_resized.shape[-2:], first_n=1) 
    sif_resized_county = np.ma.masked_array(sif_resized, mask=county_geom_mask)
    # return sif_resized_county, geom_bbox.total_bounds
    # return utils.arr_to_plasma(sif_resized), geom_bbox.total_bounds # preview

    # Preview clipped raster
    sif_resized_county_corn = sif_resized_county.copy()
    sif_resized_county_corn[arr_crop == 0] = 0

    # Calculate SIF Values
    df_final= gpd.GeoDataFrame([
        {
            'county_sif_count': (sif_resized_county>0).sum(), # Assume "no sif" == 0
            'county_sif_sum': sif_resized_county.sum()/255,
            'county_sif_mean': sif_resized_county.mean()/255,

            f'{crop_type}_sif_count': (sif_resized_county_corn>0).sum(), 
            f'{crop_type}_sif_sum': sif_resized_county_corn.sum()/255, 
            f'{crop_type}_sif_mean': sif_resized_county_corn.sum() / (sif_resized_county_corn>0).sum() / 255, 

            'sif_ratio': (sif_resized_county_corn.sum() / (sif_resized_county_corn>0).sum()) / sif_resized_county.mean(),
            'GEOID': geoid,
            'geometry': gdf.geometry.iloc[0],
            'year': year,
            'month': month,
            'period': period
        }])
    df_final.crs = "EPSG:4326"
    return df_final


@fused.cache
def get_county(fips='19119', path2: str='s3://soldatanasasifglobalifoco2modis1863/USDA/data/Actual_yields/USDA_crop_yields_2015.csv'):
    #Load corn yield
    import pandas as pd
    df = pd.read_csv(path2)
    df['GEOID'] = df['State ANSI'].map(lambda x:f'{int(x):02}' if x>0 else '99') +  df['County ANSI'].map(lambda x:f'{int(x):03}' if x>0 else '999')     
    df = df[(df['GEOID'] == fips)]
    #Load counties geometry
    import geopandas as gpd
    gdf = gpd.read_parquet('s3://fused-asset/data/tiger/county/tl_rd22_us_county 25pct.parquet')

    #Join corn yield with geometry
    gdf = gdf[['geometry','GEOID']].merge(df) 

    import matplotlib.pyplot as plt
    # Plot the data, coloring based on the 'Value' column using a green colormap
    gdf.plot(column='Value', cmap='Greens', legend=True, edgecolor='black')
    plt.show()
    
    return gdf



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


