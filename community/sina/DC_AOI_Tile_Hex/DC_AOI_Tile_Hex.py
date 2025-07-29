@fused.udf
def udf(bounds: fused.types.Bounds=[-77.083,38.804,-76.969,38.983], time_of_interest="2021-12-01/2021-12-30", chip_len:int=256, scale:float=0.1):
    import geopandas as gpd
    import shapely
    import pandas as pd
    import numpy as np

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    # find the tiles with intersecting geom
    gdf = gpd.read_file('s3://fused-asset/data/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_tract.zip')
    gdf_clipped = gdf.dissolve().to_crs(4326).clip(tile)
    gdf_w_bounds = pd.concat([gdf_clipped,tile])
    if len(gdf_w_bounds)<=1: 
        print('No bounds is intersecting with the given geometry.')
        return 
        
    # read sentinel data
    udf_sentinel = fused.load('https://github.com/fusedio/udfs/blob/b2381e/community/sina/DC_AOI_Example/')
    arr = udf_sentinel.get_arr(tile, time_of_interest=time_of_interest, output_shape=(chip_len, chip_len), max_items=100)
    arr = np.clip(arr *  scale, 0, 255).astype("uint8")[:3]
     
    # create a geom mask
    geom_mask = common.gdf_to_mask_arr(gdf_w_bounds, arr.shape[-2:], first_n=1)    
    arr = np.ma.masked_array(arr, [geom_mask]*arr.shape[0])
    
    # convert arr to xyz dataframe
    df = tile_to_df(tile, arr)
    zoom = tile.z[0]
    h3_size = min(int(3+zoom/1.5),15)
    print(h3_size) 
    data_cols = [f'band{i+1}' for i in range(len(arr))]
    df = df_to_hex(df, data_cols=data_cols, h3_size=h3_size, hex_col='hex', return_avg_lalng=True)

    # calculate stats: mean pixel value for each hex
    mask=1
    for col in data_cols:
        df[f'agg_{col}']=df[f'agg_{col}'].map(lambda x:x.mean())
        mask=mask*df[f'agg_{col}']>0
    df = df[mask]

    # convert the h3_int to h3_hex
    df['hex'] = df['hex'].map(lambda x:hex(x)[2:])
    
    return df



def df_to_hex(df, data_cols=['data'], h3_size=9, hex_col='hex', latlng_col=['lat','lng'], ordered=False, return_avg_lalng=True):
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    agg_term = ', '.join([f'ARRAY_AGG({col}) as agg_{col}' for col in data_cols])
    if return_avg_lalng:
        agg_term+=f', avg({latlng_col[0]}) as {latlng_col[0]}_avg, avg({latlng_col[1]}) as {latlng_col[1]}_avg'
    qr = f"""
        SELECT h3_latlng_to_cell({latlng_col[0]}, {latlng_col[1]}, {h3_size}) AS {hex_col}, {agg_term}
        FROM df
        group by 1 
    """
    if ordered:
        qr += '\norder by 1'
    con = common.duckdb_connect()
    return con.query(qr).df()

def tile_to_df(bounds, arr, return_geometry=False):
    import numpy as np
    import pandas as pd
    if len(arr.shape)==2:
        arr = np.stack([arr])
    shape_transform_to_xycoor = fused.load(
        "https://github.com/fusedio/udfs/tree/a1c01c6/public/common/"
    ).utils.shape_transform_to_xycoor
    
    # calculate transform
    minx, miny, maxx, maxy = bounds.to_crs(3857).total_bounds
    dx = (maxx - minx) / arr.shape[-1]
    dy = (maxx - minx) / arr.shape[-2]
    transform = [dx, 0.0, minx, 0.0, -dy, maxy, 0.0, 0.0, 1.0]
    
    # calculate meshgrid
    x_list, y_list = shape_transform_to_xycoor(arr.shape[-2:], transform)
    X, Y = np.meshgrid(x_list, y_list)
    df = pd.DataFrame(X.flatten(), columns=["lng"])
    df["lat"] = Y.flatten()

    # Load pinned versions of utility functions. 
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils

    # convert back to 4326
    df = utils.geo_convert(df).set_crs(3857, allow_override=True).to_crs(bounds.crs)
    df["lat"]=df.geometry.y
    df["lng"]=df.geometry.x
    if not return_geometry:
        del df['geometry']
        
    # convert all the bands to dataframe
    for i,v in enumerate(arr):
        df[f"band{i+1}"] = v.flatten()
    return df



    