@fused.udf
def udf(bounds: fused.types.Bounds = [-90.691,-21.719,-21.300,75.897], stats_type="mean", type='hex', color_scale:float=1):
    import pandas as pd
    import rioxarray

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    # convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)

    # 1. Initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    url = f"https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{z}/{x}/{y}.tif"
    if type=='png':
        return url_to_plasma(url, min_max=(-1000,2000/color_scale**0.5), colormap='plasma')
    else:
        
        res_offset = 0  # lower makes the hex finer
        h3_size = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
        print(h3_size)
    
        # 2. Read tiff
        da_tiff = rioxarray.open_rasterio(url).squeeze(drop=True).rio.reproject("EPSG:4326")
        df_tiff = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
    
        # 3. Hexagonify & aggregate
        df = aggregate_df_hex(
            df_tiff, h3_size, latlng_cols=["y", "x"], stats_type=stats_type
        )
        df["elev_scale"] = int((15 - z) * 1)
        df["metric"]=df["metric"]*color_scale
        df = df[df["metric"] > 0]
        return df



def url_to_plasma(url, min_max=None, colormap='plasma'):
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    return common.arr_to_plasma(common.url_to_arr(url).squeeze(), min_max=min_max, colormap=colormap, reverse=False)


@fused.cache
def df_to_hex(df, res, latlng_cols=("lat", "lng")):  
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, ARRAY_AGG(data) as agg_data
            FROM df
            group by 1
          --  order by 1
        """
    con = common.duckdb_connect()
    return con.query(qr).df()


@fused.cache
def aggregate_df_hex(df, res, latlng_cols=("lat", "lng"), stats_type="mean"):
    import pandas as pd

    df = df_to_hex(df, res=res, latlng_cols=latlng_cols)
    if stats_type == "sum":
        fn = lambda x: pd.Series(x).sum()
    elif stats_type == "mean":
        fn = lambda x: pd.Series(x).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    df["metric"] = df.agg_data.map(fn)
    return df
