@fused.udf
def udf(
    noise_311_link: str = "https://gist.githubusercontent.com/kashuk/670a350ea1f9fc543c3f6916ab392f62/raw/4c5ced45cc94d5b00e3699dd211ad7125ee6c4d3/NYC311_noise.csv",
):
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    
    df = pd.read_csv(noise_311_link)

    # Create GeoDataFrame with point geometries
    geometry = [Point(xy) for xy in zip(df["lng"], df["lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    print(gdf.T)

    return gdf