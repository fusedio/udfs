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

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    df_tiff = common.chunked_tiff_to_points(
        tiff_path, i=chunk_id, x_chunks=x_chunks, y_chunks=y_chunks)

    qr = f"""
        SELECT h3_latlng_to_cell(lat, lng, {h3_size}) AS hex, ARRAY_AGG(data) as agg_data
        FROM df_tiff
        group by 1
      --  order by 1
    """  
    con= common.duckdb_connect() 
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


def chunked_tiff_to_points(
    tiff_path, i: int = 0, x_chunks: int = 2, y_chunks: int = 2, coarsen: int = 1
):
    import numpy as np
    import pandas as pd
    import rasterio
    from rasterio.enums import Resampling

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    with rasterio.open(tiff_path) as src:
        if coarsen == 1:
            shape0 = shape = src.shape
            transform = src.transform
        else:
            shape0 = src.shape
            if len(shape0) == 2:
                shape = (1, shape0[0] // coarsen, shape0[1] // coarsen)
            elif len(shape0) == 3:
                shape = (shape0[0], shape0[1] // coarsen, shape0[2] // coarsen)
            transform = list(src.transform)
            transform[0] = transform[0] * coarsen
            transform[4] = transform[4] * coarsen

        x_list, y_list = common.shape_transform_to_xycoor(shape[-2:], transform)
        x_slice, y_slice = common.get_chunk_slices_from_shape(
            shape[-2:], x_chunks, y_chunks, i
        )

        x_list = x_list[x_slice]
        y_list = y_list[y_slice]
        X, Y = np.meshgrid(x_list, y_list)
        shape = (y_slice.stop - y_slice.start, x_slice.stop - x_slice.start)

        # todo: clean the xy slice
        x_slice, y_slice = common.get_chunk_slices_from_shape(
            shape0[-2:], x_chunks, y_chunks, i
        )
        arr = src.read(
            window=(y_slice, x_slice), out_shape=shape, resampling=Resampling.bilinear
        ).flatten()
        df = pd.DataFrame(X.flatten(), columns=["lng"])
        df["lat"] = Y.flatten()
        df["data"] = arr
    return df