@fused.udf(cache_max_age=0)
def udf(bounds:fused.types.Bounds=[-125,32,-114,42]):
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/5c526cc/community/milind/map_utils/")

    era5_udf = fused.load("era5_temp_month")
    data = era5_udf(month='2025-03')
    
    val_min = float(data["monthly_mean_temp"].min())
    val_max = float(data["monthly_mean_temp"].max())

    # Layer config for Census population data
    config_census = {
        "style": {
            "fillColor": {
                "type": "continuous",
                "attr": "monthly_mean_temp",
                "domain": [val_min, val_max],
                "steps": 20,
                "palette": "DarkMint",
            },
            "filled": True,
            "stroked": False,
            "opacity": 0.9,
        },
        "tooltip": ["hex", "monthly_mean_temp"],
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
                "name": "Monthly Mean temperature",
            }
        ],
        basemap="dark",
        theme="dark",
        initialViewState=None,
        widgets=widgets,
        debug=False,
    )
    return html