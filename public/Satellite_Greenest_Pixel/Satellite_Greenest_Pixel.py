import json

import geopandas as gpd

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
    gdf_geom: gpd.GeoDataFrame = None,
    collection_params=sentinel_params,
    chip_len: int = 512,
    how: str = "max",  # median, min. default is max
    fillna: bool = False,  # This adresses stripes
):
    import fused
    import numpy as np
    import numpy.ma as ma
    import pandas as pd
    from utils import create_tiffs_catalog, run_pool_tiffs, run_udf, search_pc_catalog

    if gdf_geom is not None:
        bbox = gdf_geom

    return run_udf(
        bbox=bbox,
        collection_params=collection_params,
        chip_len=chip_len,
        how=how,
        fillna=fillna,
    )
