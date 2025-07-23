shape_transform_to_xycoor = fused.load(
    "https://github.com/fusedio/udfs/tree/af57963/public/common/"
).utils.shape_transform_to_xycoor

chunked_tiff_to_points = fused.load(
    "https://github.com/fusedio/udfs/tree/af57963/public/common/"
).utils.chunked_tiff_to_points

duckdb_connect = fused.load(
    "https://github.com/fusedio/udfs/tree/af57963/public/common/"
).utils.duckdb_connect

get_chunk_slices_from_shape = fused.load(
    "https://github.com/fusedio/udfs/tree/af57963/public/common/"
).utils.get_chunk_slices_from_shape


def chunked_tiff_to_points(
    tiff_path, i: int = 0, x_chunks: int = 2, y_chunks: int = 2, coarsen: int = 1
):
    import numpy as np
    import pandas as pd
    import rasterio
    from rasterio.enums import Resampling

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

        x_list, y_list = shape_transform_to_xycoor(shape[-2:], transform)
        x_slice, y_slice = get_chunk_slices_from_shape(
            shape[-2:], x_chunks, y_chunks, i
        )

        x_list = x_list[x_slice]
        y_list = y_list[y_slice]
        X, Y = np.meshgrid(x_list, y_list)
        shape = (y_slice.stop - y_slice.start, x_slice.stop - x_slice.start)

        # todo: clean the xy slice
        x_slice, y_slice = get_chunk_slices_from_shape(
            shape0[-2:], x_chunks, y_chunks, i
        )
        arr = src.read(
            window=(y_slice, x_slice), out_shape=shape, resampling=Resampling.bilinear
        ).flatten()
        df = pd.DataFrame(X.flatten(), columns=["lng"])
        df["lat"] = Y.flatten()
        df["data"] = arr
    return df
