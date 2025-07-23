@fused.udf
def udf(
    lat=40.776732, lng=-73.873708, costing="auto", time_steps=[1, 5, 10, 15, 20, 25, 30]
):
    # costing options: auto, pedestrian, bicycle, truck, bus, motor_scooter
    from utils import get_isochrone

    gdf = get_isochrone(lat, lng, costing=costing, time_steps=time_steps)
    print("Please go to New York City to see your results.")
    return gdf
