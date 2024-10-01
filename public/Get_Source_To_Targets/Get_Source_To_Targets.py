@fused.udf
def udf(
    origins:str="40.,-73.945|40.743,-73.945",
    destinations:str="40.742,-73.943|40.741,-73.949",
    costing:str="auto"):
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import LineString
    # costing options: auto, pedestrian, bicycle, truck, bus, motor_scooter
    from utils import get_sources_to_targets

    sources_all = origins.split('|')
    sources_coords = [[float(s.split(',')[0]), float(s.split(',')[1])] for s in sources_all]
    destinations_all = destinations.split('|')
    destinations_coords = [[float(s.split(',')[0]), float(s.split(',')[1])] for s in destinations_all]
    result = get_sources_to_targets(sources_coords, destinations_coords)
    print("Please go to New York City to see your results.")

    df = pd.DataFrame({"sources_coords": sources_coords, "destinations_coords": destinations_coords})
    df = pd.concat([df, result], axis=1)
    df['geometry'] = df.apply(lambda x: LineString([x.sources_coords[::-1], x.destinations_coords[::-1]]), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    print(gdf.columns)
    return gdf
