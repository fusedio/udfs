@fused.udf
def udf(path:str = f"s3://fused-asset/data/cdls/2022_30m_cdls.tif", 
        bounds:list=[-120.530,37.980,-120.418,38.032], 
        crs:str='4326', 
        out_crs=None,
        out_bounds=None, 
        out_shape:list=None,
        as_table: bool=False,
        fused_metadata:dict={}, 
        verbose:bool=True):
    from utils import transform_to_gdf, reproject_bounds_shape, reproject_transform_crs, reproject_crs_shape
    utils = fused.load("https://github.com/fusedio/udfs/tree/fd32f9c/public/common/").utils
    if not crs:
        crs='4326'
    ############ read_tiff (raw pixels) ########### 
    import geopandas as gpd
    import shapely
    bbox = gpd.GeoDataFrame(geometry=[shapely.box(*bounds)], crs=crs)
    # bbox = gpd.GeoDataFrame(geometry=[shapely.box(-120.53035513070277,37.98012159397518,-120.41893689940078,38.03204439357735)], crs=4326)
    arr, metadata = utils.read_tiff(bbox, path, output_shape=None, return_colormap=False, return_transform=True, return_crs=True, return_bounds=True, return_meta=True,)
    if verbose:
        print(metadata["meta"])
        print(arr.shape)
    if out_bounds:
        if not out_crs:
            raise ValueError('`out_bounds` is not `None` therefore, you need to pass `out_crs`.')
    else:
        if not out_crs: 
            out_crs = crs
            out_bounds = bounds
            # if not out_shape:
            #     return arr, metadata["bounds"]#, metadata["crs"]
            # else:
                # out_crs = metadata["crs"]
                # out_bounds = metadata["bounds"]
        else:
            out_bounds = bbox.to_crs(out_crs).total_bounds
    # return utils.bounds_to_gdf(metadata['bounds'], metadata['crs']).to_crs(4326)

    ############### reproject array ###############
    ## given output's bounds & shape
    from pyproj import CRS
    out_crs = CRS.from_user_input(out_crs)
    dst_shape = out_shape#(300, 300)
    out_arr = reproject_bounds_shape(
        arr,
        src_crs=metadata["crs"],
        src_bounds=metadata["bounds"],
        dst_crs=out_crs,
        dst_bounds=out_bounds,
        dst_shape=dst_shape,
    )
    # print(out_bounds)
    if not as_table:
        return out_arr, out_bounds
    else:
        import pandas as pd
        import rasterio
        df = pd.DataFrame([{'path':path, 'out_data':out_arr.flatten(), 'out_shape':out_arr.shape, 'out_crs':str(out_crs), 'out_bounds':out_bounds, 'out_transform':rasterio.transform.from_bounds(*out_bounds, arr.shape[-1], arr.shape[-2]),
                             'native_data':arr.flatten(), 'native_shape':arr.shape, 'native_crs':str(metadata['crs']), 'native_bounds':metadata['bounds'], 'native_transform':metadata['transform']}])
        for k in fused_metadata:
            df[k]=[fused_metadata[k]]
        return df
    # # given output's transform & shape
    # import rasterio
    # dst_shape = (300, 300)
    # return reproject_transform_crs(
    #     arr,
    #     src_crs=metadata["crs"],
    #     src_transform=metadata["transform"],
    #     dst_crs=bbox.crs,
    #     dst_transform=rasterio.transform.from_bounds(*bbox.total_bounds, dst_shape[-1], dst_shape[-2]),
    #     dst_shape=dst_shape,
    # ), bbox.total_bounds

    # ## given output's crs & shape #WIP
    # dst_shape = (300, 300)
    # bbox_src = transform_to_gdf(metadata["transform"], arr.shape, crs=metadata["crs"])
    # bounds = utils.bounds_to_gdf(bbox_src.total_bounds, crs=metadata["crs"]).to_crs('4326').total_bounds
    # return reproject_crs_shape(
    #     arr,
    #     src_crs=metadata["crs"],
    #     src_transform=metadata["transform"],
    #     dst_crs=bbox.crs,
    #     dst_shape=dst_shape,
    # ), bounds
