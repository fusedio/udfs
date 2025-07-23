@fused.udf
def udf():
    import requests
    import geopandas as gpd
    from shapely.geometry import shape

    params = {"limit": -1}

    url = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/community-gardens-and-food-trees/records"
    r = requests.get(url, params=params)

    gdf = gpd.GeoDataFrame(r.json()["results"])
    gdf["geometry"] = gdf["geom"].apply(lambda x: shape(x["geometry"]) if x else None)
    gdf = gdf.set_geometry("geometry")
    
    del gdf["geom"]
    del gdf['geometry']
    del gdf['mapid']
    del gdf['steward_or_managing_organization']
    del gdf['public_e_mail']
    del gdf['website']
    del gdf['jurisdiction']

    print(f"{gdf.sample(3)=}")
    print(gdf.columns)

    return gdf