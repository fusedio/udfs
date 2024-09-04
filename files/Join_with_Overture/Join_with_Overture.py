@fused.udf
def udf(
    bbox: fused.types.TileGDF, path: str, overture_type="place", clip: bool = False
):
    theme_type = {
        "building": "buildings",
        "segment": "transportation",
        "connector": "transportation",
        "place": "places",
        "address": "addresses",
        "water": "base",
        "land_use": "base",
        "infrastructure": "base",
        "land": "base",
        "division": "divisions",
        "division_area": "divisions",
        "division_boundary": "divisions",
    }
    import geopandas as gpd

    try:
        gdf = gpd.read_parquet(path).to_crs("EPSG:4326")
    except:
        print("This file does not contain geometry")
        return

    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme=theme_type[overture_type],
        overture_type=overture_type,
        bbox=bbox,
    )
    if len(gdf_overture) == 0:
        print(
            "There is no data in this viewprot. Please move around to find your data."
        )
        return
    if clip:
        gdf_joined = gdf_overture.clip(gdf)
    else:
        gdf_joined = gdf_overture.sjoin(gdf)
    return gdf_joined
