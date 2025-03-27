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
    bounds: fused.types.Bounds = None,
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
    from utils import (
        create_tiffs_catalog,
        get_greenest_pixel,
        run_pool_tiffs,
        search_pc_catalog,
    )

    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    tile = utils.get_tiles(bounds, zoom=zoom)

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
    return arr_scaled
    # TODO: color correction https://custom-scripts.sentinel-hub.com/custom-scripts/sentinel-2/poor_mans_atcor/
