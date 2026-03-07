@fused.udf
def udf(
    bounds: fused.types.Bounds = [-75.175, 39.948, -75.155, 39.958],
    path: str = "https://hub.arcgis.com/api/v3/datasets/ab9e89e1273f445bb265846c90b38a96_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",
    overture_type="place",
    clip: bool = False,
):
    import geopandas as gpd
    from shapely.geometry import box

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
    @fused.cache
    def load_file(file_path):
        if file_path.endswith(('.parquet', '.pq', '.gpq', '.geoparquet')):
            return gpd.read_parquet(file_path)
        else:
            return gpd.read_file(file_path)

    try:
        if path:
            gdf = load_file(path).to_crs("EPSG:4326")
            print(f"Loaded file: {len(gdf)} rows")
        else:
            gdf = gpd.GeoDataFrame(
                {"name": ["Demo Area"]},
                geometry=[box(*bounds)],
                crs="EPSG:4326",
            )
            print("Using demo area polygon. Pass a 'path' to use your own data.")
    except Exception as e:
        print("Could not read file:", str(e))
        return
    overture_udf = fused.load('https://github.com/fusedio/udfs/tree/1762605/public/Overture_Maps_Example/')
    gdf_overture = overture_udf(
        theme=theme_type[overture_type],
        overture_type=overture_type,
        bounds=bounds,
    )
    if len(gdf_overture) == 0:
        print(
            "There is no data in this viewport. Please move around to find your data."
        )
        return
    if clip:
        gdf_joined = gdf_overture.clip(gdf)
    else:
        gdf_joined = gdf_overture.sjoin(gdf)
    return gdf_joined
