@fused.udf
def udf(
    tiff_path: str = "s3://fused-asset/gfc2020/JRC_GFC2020_V1_N60_E20.tif",
    chunk_id: int = 0,
    x_chunks: int = 1,
    y_chunks: int = 1,
    coarsen: int = 100,
    h3_size: int = 6,
    stats_type: str = 'mean',
):
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box
    from utils import chunked_tiff_to_points, duckdb_connect

    df_tiff = chunked_tiff_to_points(
        tiff_path, i=chunk_id, x_chunks=x_chunks, y_chunks=y_chunks, coarsen=coarsen
    )

    qr = f"""
        SELECT h3_latlng_to_cell(lat, lng, {h3_size}) AS hex, ARRAY_AGG(data) as agg_data
        FROM df_tiff
        group by 1
      --  order by 1
    """
    con=duckdb_connect()
    df = con.query(qr).df()
    if stats_type=='sum':
        fn = lambda x: pd.Series(x).sum()
    elif stats_type=='mean':
        fn = lambda x: pd.Series(x).mean()
    else:
        fn = lambda x: pd.Series(x).mean()
    df["agg_data"] = df.agg_data.map(fn)
    print(df)
    return df
