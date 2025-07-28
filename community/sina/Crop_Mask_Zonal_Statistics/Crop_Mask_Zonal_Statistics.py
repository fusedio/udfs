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
        common = fused.load(
        "https://github.com/fusedio/udfs/tree/ee9bec5/public/common/"
        ).utils

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
    from utils2 import get_masked_array, get_da, get_da_bounds, clip_arr
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
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/ee9bec5/public/common/"
    ).utils

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
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/ee9bec5/public/common/"
    ).utils
    county_geom_mask = utils.gdf_to_mask_arr(gdf, sif_resized.shape[-2:], first_n=1) 
    sif_resized_county = np.ma.masked_array(sif_resized, mask=county_geom_mask)
    # return sif_resized_county, geom_bbox.total_bounds
    # return common.arr_to_plasma(sif_resized), geom_bbox.total_bounds # preview

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
