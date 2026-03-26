@fused.udf(cache_max_age=0)
def udf():
    """Daily Mean Temperature"""
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/751ce06/community/milind/map_utils/")

    # Layer config for hex data
    config_hex = {
        "style": {
            "fillColor": {
                "type": "continuous",
                "attr": "monthly_mean_temp",
                "domain": [-10, 30], # Hard coding to see highest areas
                "steps": 20,
                "palette": "DarkMint"
            },
            "filled": True,
            "stroked": False,
            "opacity": 0.9,
            "coverage": 0.9,
        },
        "tooltip": ["hex", "monthly_mean_temp"]
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
                "parquetUrl": "https://unstable.udf.ai/fsh_4KJ70bZvE4xZskPQ9GS2aV.parquet",
                "config": config_hex,
                "visible": True,
                "name": "Monthly Mean Temperatyure",
                "sql": """SELECT 
hex, monthly_mean_temp
FROM data 
WHERE monthly_mean_temp > 10
                """,
            }
        ],
        basemap="dark",
        theme="dark",
        initialViewState={
          "longitude": -98.5795,
          "latitude": 39.8283,
          "zoom": 3,
          "pitch": 0,
          "bearing": 0,
      },
        widgets=widgets,
        sidebar='show',
    )
    return html