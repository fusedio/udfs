@fused.cache
def run_pool_tiffs(bbox, df_tiffs, chip_len):
    import numpy as np

    columns = df_tiffs.columns

    @fused.cache
    def fn_read_tiff(tiff_url, bbox=bbox, chip_len=chip_len):
        read_tiff = fused.load(
            "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
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

    # TODO: Fused Team to streamline execution
    # arr = fused.run(
    #     "50400e341f90e9a288c7259630c200d296bf2a9b4b9edd63ff22ead08c6968f4",
    #     bbox=bbox,
    #     chip_len=chip_len,
    #     tiff_url=tiff_url)
    # data = arr.image.data.astype('uint16')
    # arr = ma.masked_array(data, mask=data==0)
    # return arr


# band_list = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B11', 'B12']
