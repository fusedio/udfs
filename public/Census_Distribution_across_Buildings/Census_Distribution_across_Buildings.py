import geopandas as gpd

@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-03-12-alpha-0",
    polygon: gpd.GeoDataFrame = None,
    resolution: int = 10

):
    import geopandas as gpd
    import concurrent.futures
    from utils import get_buildings_h3,acs_5yr_bbox, get_census
    import h3
    import pandas as pd
    from shapely.geometry import shape, box, Polygon
    import logging

    # Getting Overture buildings Data in H3 Format
    building_data = get_buildings_h3(bbox, release, resolution)

    # Getting Census Data in H3 Format
    census_df = get_census(bbox, census_variable='Total Pop', scale_factor=200, is_density=True, year=2022)

    print(census_df)
    print(building_data)

    # Performing SJoin on buildings data and Census data to find the population distribution
    joined_gdf = gpd.sjoin(building_data, census_df, how="left", op="intersects")

    # Calculation of population count to visualize population
    joined_gdf['cnt'] = joined_gdf['cnt'].fillna(0)


    return joined_gdf
