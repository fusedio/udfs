utils = fused.load("https://github.com/fusedio/udfs/tree/be3bc93/public/common/").utils
url_to_arr=utils.url_to_arr
def url_to_plasma(url, min_max=None, colormap='plasma'):
    return utils.arr_to_plasma(utils.url_to_arr(url).squeeze(), min_max=min_max, colormap=colormap, reverse=False)
    
@fused.cache
def df_to_hex(df, res, latlng_cols=("lat", "lng")):
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, ARRAY_AGG(data) as agg_data
            FROM df
            group by 1
          --  order by 1
        """
    con = utils.duckdb_connect()
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
