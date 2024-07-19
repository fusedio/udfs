@fused.udf
def udf(
    tiff_path: str = "s3://fused-asset/gfc2020/JRC_GFC2020_V1_N60_E20.tif",
    chunk_id: int = 0,
    x_chunks: int = 1,
    y_chunks: int = 1,
    coarsen: int = 100,
    h3_size=6,
):
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box
    from utils import chunked_tiff_to_points, run_query

    df_tiff = chunked_tiff_to_points(
        tiff_path, i=chunk_id, x_chunks=x_chunks, y_chunks=y_chunks, coarsen=coarsen
    )

    qr = f"""
        SELECT h3_latlng_to_cell(lat, lng, {h3_size}) AS hex, ARRAY_AGG(data) as agg_data
        FROM df_tiff
        group by 1
      --  order by 1

    """

    df = run_query(qr, return_arrow=True)
    df = df.to_pandas()
    df["agg_data"] = df.agg_data.map(lambda x: pd.Series(x).sum())
    df["hex"] = df["hex"].map(lambda x: hex(x)[2:])
    df["metric"] = df.agg_data / df.agg_data.max() * 100
    print(df)
    # Use this return statement to locate the output on the map
    # return gpd.GeoDataFrame(geometry=[box(df_tiff.lng.min(), df_tiff.lat.min(), df_tiff.lng.max(), df_tiff.lat.max())])
    return df
