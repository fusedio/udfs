@fused.udf(cache_max_age=0)
def udf(bounds:fused.types.Bounds=[-125,32,-114,42]):
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/5c526cc/community/milind/map_utils/")

    era5_udf = fused.load("copdem_elevation")
    data = era5_udf(bounds=bounds)
    
    val_min = float(data["data_avg"].min())
    val_max = float(data["data_avg"].max())

    # Layer config for Census population data
    config_census = {
        "style": {
            "fillColor": {
                "type": "continuous",
                "attr": "data_avg",
                "domain": [val_min, val_max],
                "steps": 20,
                "palette": "Earth",
            },
            "filled": True,
            "stroked": False,
            "opacity": 0.9,
        },
        "tooltip": ["hex", "data_avg"],
    }

    widgets = {
        "controls": "bottom-right",
        "scale": "bottom-left",
        "basemap": "bottom-right",
        "layers": {"position": "top-right", "expanded": False},
        "legend": {"position": "top-right", "expanded": True},
    }

    html = map_utils.deckgl_layers(
        layers=[
            {
                "type": "hex",
                "data": data,
                "config": config_census,
                "visible": True,
                "name": "Elevation",
            }
        ],
        basemap="dark",
        theme="dark",
        initialViewState=None,
        widgets=widgets,
        debug=False,
    )
    return html