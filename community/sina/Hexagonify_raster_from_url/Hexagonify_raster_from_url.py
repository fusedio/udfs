@fused.udf
def udf(raster_path = 'https://s3.amazonaws.com/elevation-tiles-prod/geotiff/4/4/6.tif', 
        stats_type="mean", 
        h3_res: int = 4,
        height_scale: int = 10,
         color_scale:float=1
    ):
    import pandas as pd
    import rioxarray

    # 2. Read tiff
    da_tiff = rioxarray.open_rasterio(raster_path).squeeze(drop=True).rio.reproject("EPSG:4326")
    df_tiff = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]

    # 3. Hexagonify & aggregate
    df = aggregate_df_hex(
        df_tiff, h3_res, latlng_cols=["y", "x"], stats_type=stats_type
    )
    df["elev_scale"] = height_scale
    df["metric"]=df["metric"]*color_scale
    df = df[df["metric"] > 0]
    print(f"{df.shape=}")
    return df

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

