import json

sentinel_params = json.dumps(
    {
        "collection": "sentinel-2-l2a",
        "band_list": ["B02", "B03", "B04", "B08"],
        "time_of_interest": "2021-09-01/2021-12-30",
        "query": {"eo:cloud_cover": {"lt": 5}},
        "scale": 0.1,
    }
)


def run_udf(
    bbox, collection_params=sentinel_params, chip_len=512, how="max", fillna=False
):
    import json

    import numpy as np
    import numpy.ma as ma
    import pandas as pd

    collection, band_list, time_of_interest, query, scale = json.loads(
        collection_params
    ).values()

    print(collection, band_list, time_of_interest, query)

    stac_items = search_pc_catalog(
        bbox=bbox, time_of_interest=time_of_interest, query=query, collection=collection
    )
    if not stac_items:
        return

    print("Processing stac_items: ", len(stac_items))
    print(stac_items[0].assets.keys())
    df_tiff_catalog = create_tiffs_catalog(stac_items, band_list)
    arrs_out = run_pool_tiffs(bbox, df_tiff_catalog, chip_len)

    # Calculate NDVI
    # First 3 bands to visualize, last 2 bands to calculate NDVI
    out = (arrs_out[-1] * 1.0 - arrs_out[-2] * 1.0) / (
        arrs_out[-1] * 1.0 + arrs_out[-2] * 1.0
    )
    t_len = out.shape[0]
    out_flat = out.reshape(t_len, chip_len * chip_len)
    # Find greenest pixels
    sorted_indices = np.argsort(out_flat, axis=0)
    if how == "median":
        median_index = sorted_indices[t_len // 2]
    elif how == "min":
        median_index = np.argmin(out_flat, axis=0)
    else:
        median_index = np.argmax(out_flat, axis=0)

    out_flat = out_flat[median_index, np.arange(chip_len * chip_len)]

    output_bands = []

    for b in [0, 1, 2]:
        out_flat = arrs_out[b].reshape(t_len, chip_len * chip_len)

        # Replace 0s with NaNs
        out_flat = np.where(out_flat == 0, np.nan, out_flat)
        if fillna:
            out_flat = pd.DataFrame(out_flat).ffill().bfill().values
        out_flat = out_flat[median_index, np.arange(chip_len * chip_len)]
        output_bands.append(out_flat.reshape(chip_len, chip_len))

    stacked = np.stack(output_bands) * 1.0 * scale
    stacked = np.clip(stacked, 0, 255).astype("uint8")
    # TODO: color correction https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/poor_mans_atcor/
    return stacked


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
