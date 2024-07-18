def chunked_tiff_to_points(tiff_path, i: int = 0, x_chunks: int = 2, y_chunks: int = 2):
    import numpy as np
    import pandas as pd
    import rasterio

    with rasterio.open(tiff_path) as src:
        x_list, y_list = shape_transform_to_xycoor(src.shape[-2:], src.transform)
        x_slice, y_slice = get_chunk_slices_from_shape(
            src.shape[-2:], x_chunks, y_chunks, i
        )
        x_list = x_list[x_slice]
        y_list = y_list[y_slice]
        X, Y = np.meshgrid(x_list, y_list)
        arr = src.read(window=(y_slice, x_slice)).flatten()
        df = pd.DataFrame(X.flatten(), columns=["lng"])
        df["lat"] = Y.flatten()
        df["data"] = arr
    return df


def shape_transform_to_xycoor(shape, transform):
    import numpy as np

    n_y = shape[-2]
    n_x = shape[-1]
    w, _, x, _, h, y, _, _, _ = transform
    x_list = np.arange(x + w / 2, x + n_x * w + w / 2, w)[:n_x]
    y_list = np.arange(y + h / 2, y + n_y * h + h / 2, h)[:n_y]
    return x_list, y_list


def get_chunk_slices_from_shape(array_shape, x_chunks, y_chunks, i):
    # Unpack the dimensions of the array shape
    rows, cols = array_shape

    # Calculate the size of each chunk
    chunk_height = rows // y_chunks
    chunk_width = cols // x_chunks

    # Calculate row and column index for the i-th chunk
    row_start = (i // x_chunks) * chunk_height
    col_start = (i % x_chunks) * chunk_width

    # Create slice objects for the i-th chunk
    y_slice = slice(row_start, row_start + chunk_height)
    x_slice = slice(col_start, col_start + chunk_width)

    return x_slice, y_slice


# @fused.cache
def run_query(query, return_arrow=False):
    import duckdb

    # con = duckdb.connect()
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    # TODO: con.sql("INSTALL h3 FROM community;")
    fused.load(
        "https://github.com/fusedio/udfs/tree/fb65aff/public/DuckDB_H3_Example/"
    ).utils.load_h3_duckdb(con)
    con.sql(
        """SET home_directory='/tmp/';
    install 'httpfs';
    load 'httpfs';
    INSTALL spatial;
    LOAD spatial;
    """
    )
    print("duckdb version:", duckdb.__version__)
    if return_arrow:
        return con.sql(query).fetch_arrow_table()
    else:
        return con.sql(query).df()
