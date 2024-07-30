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
    bbox: fused.types.TileGDF = None,
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
    from utils import create_tiffs_catalog, run_pool_tiffs, search_pc_catalog

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
    return stacked
    # TODO: color correction https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/poor_mans_atcor/
