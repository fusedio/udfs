@fused.udf
def udf():
    data = fused.run('vancouver_open_data')

    map_utils = fused.load("https://github.com/fusedio/udfs/tree/4d61cd1/community/milind/map_utils/")

    widgets = {
      "controls": "bottom-right",
      "scale": "bottom-left",
      "basemap": "bottom-right",
      "layers": {"position": "top-right", "expanded": True},
      "legend": False,
      "geocoder": "top-left",
    }

    config = {
        "style": {
            "filled": False,
            "stroked": True,
            "opacity": 0.1,
            "lineWidth": 2,
            "lineColor": {
                "type": "continuous",
                "attr": "growth",
                "domain": [-1, 1],
                "palette": "ArmyRose",
                "steps": 7,
            },
        },
        "tooltip": ["valuePerSqm", "growth"],
    }

    return map_utils.deckgl_layers(
        layers=[{
            "type": "vector",
            "data": data,
            "config": config,
            "name": "Vancouver Price Increase",
        }],
        sidebar = "show",
        widgets=widgets,
    )