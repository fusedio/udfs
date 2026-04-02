@fused.udf(cache_max_age=0)
def udf():
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/6800334/community/milind/map_utils/")

    data_udf = fused.load('us_counties_from_api')
    gdf = data_udf() # Load the default values

    return map_utils.deckgl_map(
        gdf=gdf,
        basemap="light",
        config={
            "vectorLayer": {
                "stroked": True,
                "filled": True,
                "getFillColor": [100, 160, 220, 80],
                "getLineColor": [255, 0, 0, 220],
                "lineWidthMinPixels": 3,
            }
        }
    )
