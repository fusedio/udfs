@fused.udf(cache_max_age = 0)
def udf():
    points = fused.load('ny411_noise_points')
    data = points()

    map_utils = fused.load("https://github.com/fusedio/udfs/tree/5e293a4/community/milind/map_utils/")

    config = {
        "style": {
            "pointRadius": 2,
        },
    }

    return map_utils.deckgl_layers(
        layers=[{
            "type": "vector",
            "data": data.sample(5_000), # Visualise only a subet of points
            "config": config,
            "name": "NYC 411 Calls",
        }],
    )