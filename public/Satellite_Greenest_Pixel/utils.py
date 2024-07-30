@fused.cache
def run_pool_tiffs(bbox, df_tiffs, chip_len):
    import numpy as np

    columns = df_tiffs.columns

    @fused.cache
    def fn_read_tiff(tiff_url, bbox=bbox, chip_len=chip_len):
        read_tiff = fused.load(
            "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
        ).utils.read_tiff
        return read_tiff(bbox, tiff_url, output_shape=(chip_len, chip_len))

    tiff_list = []
    for band in columns:
        for i in range(len(df_tiffs)):
            tiff_list.append(df_tiffs[band].iloc[i])

    arrs_tmp = fused.utils.common.run_pool(fn_read_tiff, tiff_list)
    arrs_out = np.stack(arrs_tmp)
    arrs_out = arrs_out.reshape(len(columns), len(df_tiffs), chip_len, chip_len)
    return arrs_out


def search_pc_catalog(
    bbox,
    time_of_interest,
    query={"eo:cloud_cover": {"lt": 5}},
    collection="sentinel-2-l2a",
):
    import planetary_computer
    import pystac_client

    # Instantiate PC client
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    # Search catalog
    items = catalog.search(
        collections=[collection],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query=query,
    ).item_collection()

    if len(items) == 0:
        print(f"empty for {time_of_interest}")
        return

    return items


def create_tiffs_catalog(stac_items, band_list):
    import pandas as pd

    input_paths = []
    for selected_item in stac_items:
        max_key_length = len(max(selected_item.assets, key=len))
        input_paths.append([selected_item.assets[band].href for band in band_list])
    return pd.DataFrame(input_paths, columns=band_list)


@fused.cache
def fn_read_tiff(tiff_url, bbox, chip_len):
    return read_tiff(bbox, tiff_url, output_shape=(chip_len, chip_len))


def get_greenest_pixel(arr_rgbi, how="median", fillna=True):
    import numpy as np
    import pandas as pd

    # First 3 bands to visualize, last 2 bands to calculate NDVI
    out = (arr_rgbi[-1] * 1.0 - arr_rgbi[-2] * 1.0) / (
        arr_rgbi[-1] * 1.0 + arr_rgbi[-2] * 1.0
    )
    t_len = out.shape[0]
    out_flat = out.reshape(out.shape[0], out.shape[1] * out.shape[2])
    # Find greenest pixels
    sorted_indices = np.argsort(out_flat, axis=0)
    if how == "median":
        median_index = sorted_indices[t_len // 2]
    elif how == "min":
        median_index = np.argmin(out_flat, axis=0)
    else:
        median_index = np.argmax(out_flat, axis=0)

    out_flat = out_flat[median_index, np.arange(out.shape[1] * out.shape[2])]

    output_bands = []

    for b in [0, 1, 2, 3]:
        out_flat = arr_rgbi[b].reshape(t_len, out.shape[1] * out.shape[2])

        # Replace 0s with NaNs
        out_flat = np.where(out_flat == 0, np.nan, out_flat)
        if fillna:
            out_flat = pd.DataFrame(out_flat).ffill().bfill().values
        out_flat = out_flat[median_index, np.arange(out.shape[1] * out.shape[2])]
        output_bands.append(out_flat.reshape(out.shape[1], out.shape[2]))
    return np.stack(output_bands)
