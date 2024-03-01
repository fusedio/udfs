import numpy as np


def bbox_stac_items(bbox, table):
    import fused
    import pyarrow.parquet as pq
    import pandas as pd
    import geopandas as gpd
    import shapely

    df = fused.get_chunks_metadata(table)
    df = df[df.intersects(bbox)]
    if len(df) > 10 or len(df) == 0:
        return None  # fault
    matching_images = []
    for idx, row in df.iterrows():
        file_url = table + row["file_id"] + ".parquet"
        chunk_table = pq.ParquetFile(file_url).read_row_group(row["chunk_id"])
        chunk_gdf = gpd.GeoDataFrame(chunk_table.to_pandas())
        if "geometry" in chunk_gdf:
            chunk_gdf.geometry = shapely.from_wkb(chunk_gdf.geometry)
        matching_images.append(chunk_gdf)

    ret_gdf = pd.concat(matching_images)
    ret_gdf = ret_gdf[ret_gdf.intersects(bbox)]
    return ret_gdf


# todo: switch to public.common
def read_tiff(
    bbox, input_tiff_path, crs, buffer_degree, output_shape, resample_order=0
):
    from rasterio.session import AWSSession
    import rasterio
    from scipy.ndimage import zoom
    from io import BytesIO

    out_buf = BytesIO()
    with rasterio.Env(AWSSession(requester_pays=True)):
        with rasterio.open(input_tiff_path) as src:
            if buffer_degree != 0:
                bbox.geometry = bbox.geometry.buffer(buffer_degree)
            bbox_projected = bbox.to_crs(crs)
            window = src.window(*bbox_projected.total_bounds)
            data = src.read(window=window, boundless=True)
            zoom_factors = np.array(output_shape) / np.array(data[0].shape)
            rgb = np.array(
                [zoom(arr, zoom_factors, order=resample_order) for arr in data]
            )
        return rgb


def arr_to_png(rgb, nodata=0):
    if len(rgb.shape) == 2:
        count = 1
        single_band = True
    else:
        count = rgb.shape[0]
        single_band = False
    import rasterio
    from io import BytesIO

    out_buf = BytesIO()
    with rasterio.open(
        out_buf,
        "w",
        driver="PNG",
        height=rgb.shape[-2],
        width=rgb.shape[-1],
        count=count,
        dtype=rgb.dtype,
        nodata=nodata,
        compress="deflate",
    ) as dst:
        if single_band:
            dst.write(rgb, 1)
        else:
            dst.write(rgb)
    return out_buf
