@fused.udf(cache_max_age=0)
def udf():
    """NYC 311 Noise Hex Map"""
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/5c526cc/community/milind/map_utils/")

    print(fused.context.this_udf())
    hex_udf = fused.load("ny411_noise_h3")
    data = hex_udf()
    data = data[['hex', 'cnt']]

    print(f"Loaded {len(data)} hex cells")
    print(data.T)

    # Layer config for hex data
    config_hex = {
        "style": {
            "fillColor": {
                "type": "continuous",
                "attr": "cnt",
                "domain": [0, 100], # Hard coding to see highest areas
                "steps": 20,
                "palette": "OrYel"
            },
            "filled": True,
            "stroked": False,
            "opacity": 0.9,
            "coverage": 0.9
        },
        "tooltip": ["hex", "cnt"]
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
                "config": config_hex,
                "visible": True,
                "name": "411 Calls (noise complaints)"
            }
        ],
        basemap="dark",
        theme="dark",
        initialViewState=None,
        widgets=widgets,
    )
    return html