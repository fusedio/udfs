@fused.udf(cache_max_age=0)
def udf(bounds: fused.types.Bounds = [-125, 32, -114, 42], res: int = 4, month: str = '2024-05'):
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/6800334/community/milind/map_utils/")

    
    join_udf = fused.load("temp_elev_corr")
    data = join_udf(bounds=bounds, res=res, month=month)

    elev_min = float(data["elevation_avg"].min())
    elev_max = float(data["elevation_avg"].max())
    temp_min = float(data["monthly_mean_temp"].min())
    temp_max = float(data["monthly_mean_temp"].max())

    config_elev = {
        "style": {
            "fillColor": {
                "type": "continuous",
                "attr": "elevation_avg",
                "domain": [elev_min, elev_max],
                "steps": 20,
                "palette": "Earth",
            },
            "filled": True,
            "stroked": False,
            "opacity": 0.9,
        },
        "tooltip": ["hex", "elevation_avg", "monthly_mean_temp"],
    }

    config_temp = {
        "style": {
            "fillColor": {
                "type": "continuous",
                "attr": "monthly_mean_temp",
                "domain": [temp_min, temp_max],
                "steps": 20,
                "palette": "Temps",
            },
            "filled": True,
            "stroked": False,
            "opacity": 0.9,
        },
        "tooltip": ["hex", "elevation_avg", "monthly_mean_temp"],
    }

    widgets = {
        "controls": "bottom-right",
        "scale": "bottom-left",
        "basemap": "bottom-right",
        "layers": {"position": "top-right", "expanded": True},
        "legend": {"position": "top-right", "expanded": True},
    }

    html = map_utils.deckgl_layers(
        layers=[
            {
                "type": "hex",
                "data": data,
                "config": config_elev,
                "visible": True,
                "name": "Joined Datasets",
                "legend": False,
            },
        ],
        basemap="dark",
        theme="dark",
        initialViewState=None,
        widgets=widgets,
        debug=False,
    )
    return html