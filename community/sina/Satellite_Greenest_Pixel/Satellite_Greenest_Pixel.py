import json

# https://planetarycomputer.microsoft.com/dataset/modis-09A1-061
modis_params = json.dumps(
    {
        "collection": "modis-09A1-061",
        "band_list": [
            "sur_refl_b01",
            "sur_refl_b04",
            "sur_refl_b03",
            "sur_refl_b02",
        ],
        "time_of_interest": "2020-09/2020-10",
        "query": {},
        "scale": 0.1,
    }
)

# https://planetarycomputer.microsoft.com/dataset/landsat-c2-l2
landsat_params = json.dumps(
    {
        "collection": "landsat-c2-l2",
        "band_list": ["blue", "green", "red", "nir08"],
        "time_of_interest": "2016-09-01/2016-12-30",
        "query": {"eo:cloud_cover": {"lt": 5}},
        "scale": 0.01,
    }
)

# https://planetarycomputer.microsoft.com/dataset/sentinel-2-l2a
sentinel_params = json.dumps(
    {
        "collection": "sentinel-2-l2a",
        "band_list": ["B02", "B03", "B04", "B08"],
        "time_of_interest": "2021-09-01/2021-12-30",
        "query": {"eo:cloud_cover": {"lt": 5}},
        "scale": 0.1,
    }
)


@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.463,37.755,-122.376,37.803],
    collection_params=sentinel_params,
    chip_len: int = 512,
    how: str = "max",  # median, min. default is max
    fillna: bool = False,  # This adresses stripes
):
    import json

    import fused
    import numpy as np
    import numpy.ma as ma
    import pandas as pd

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    tile = common.get_tiles(bounds, clip=True)

    collection, band_list, time_of_interest, query, scale = json.loads(
        collection_params
    ).values()

    print(collection, band_list, time_of_interest, query)

    stac_items = search_pc_catalog(
        bounds=tile, time_of_interest=time_of_interest, query=query, collection=collection
    )
    if not stac_items:
        return

    print("Processing stac_items: ", len(stac_items))
    print(stac_items[0].assets.keys())
    df_tiff_catalog = create_tiffs_catalog(stac_items, band_list)

    arrs_out = run_pool_tiffs(tile, df_tiff_catalog, output_shape=(chip_len, chip_len))

    # Generate arr with imagery
    arr = get_greenest_pixel(arrs_out, how=how, fillna=fillna)[:3,:,:]

    arr_scaled = arr * 1.0 * scale
    arr_scaled = np.clip(arr_scaled, 0, 255).astype("uint8")
    return arr_scaled, bounds
    # TODO: color correction https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/poor_mans_atcor/


@fused.cache
def run_pool_tiffs(bounds, df_tiffs, output_shape):
    import numpy as np

    columns = df_tiffs.columns

    @fused.cache
    def fn_read_tiff(tiff_url, bounds=bounds, output_shape=output_shape):
        common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

        return common.read_tiff(bounds, tiff_url, output_shape=output_shape)

    tiff_list = []
    for band in columns:
        for i in range(len(df_tiffs)):
            tiff_list.append(df_tiffs[band].iloc[i])

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    arrs_tmp = common.run_pool(fn_read_tiff, tiff_list)
    arrs_out = np.stack(arrs_tmp)
    arrs_out = arrs_out.reshape(len(columns), len(df_tiffs), output_shape[-2], output_shape[-1])
    return arrs_out


def search_pc_catalog(
    bounds,
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
        bbox=bounds.total_bounds,
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

