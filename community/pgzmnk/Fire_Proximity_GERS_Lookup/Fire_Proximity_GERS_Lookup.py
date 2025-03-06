@fused.udf
def udf(gers_id: str = "08b29a02ee122fff0200e9729b3c4470"):
    import geopandas as gpd
    import h3
    import pandas as pd
    import requests
    from shapely.geometry import Polygon

    # 0. H3 from GERS
    h3_index = gers_id[:16]
    print("h3_index", h3_index)

    # 1. Polygon from H3
    bounds = Polygon([coord[::-1] for coord in h3.cell_to_boundary(h3_index)])
    bbox = gpd.GeoDataFrame({"h3_index": [h3_index], "geometry": [bounds]})

    # 2. Load Fire data and create buffers
    @fused.cache
    def load_data():
        # Source: https://data-nifc.opendata.arcgis.com/datasets/nifc::interagencyfireperimeterhistory-all-years-view/about
        geojson_url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters_Current/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
        response = requests.get(geojson_url)
        response.raise_for_status()  # Raise an exception if the request fails
        geojson_data = response.json()
        # Convert the GeoJSON data to a GeoDataFrame
        gdf_fire = gpd.GeoDataFrame.from_features(geojson_data["features"])
        return gdf_fire

    gdf_fire = load_data()
    gdf_fire.crs = "EPSG:4326"

    buffers = {
        "historic_perimeter": {"distance": 0, "score": 3},
        "near_historic_perimeter": {"distance": 1_000, "score": 2},
        "outside_perimeter": {"distance": 10_000, "score": 1},
    }
    gdfs = []
    for buffer_name, buffer in buffers.items():
        _gdf = gdf_fire
        # Create a buffer
        _gdf = _gdf.to_crs(_gdf.estimate_utm_crs())
        _gdf["geometry"] = _gdf["geometry"].buffer(buffer["distance"])
        _gdf["score"] = buffer["score"]
        _gdf = _gdf.to_crs("EPSG:4326")
        _gdf["buffer_name"] = buffer_name
        gdfs.append(_gdf)

    gdf_fire = pd.concat(gdfs)

    # 2. Load Overture Buildings
    gdf = fused.run(
        "UDF_Overture_Maps_Example", bbox=bbox, overture_type="building", min_zoom=10
    )
    if (len(gdf) == 0) or "sources" not in gdf:
        return

    gdf = gdf.sjoin(gdf_fire, how="left")
    gdf["buffer_name"] = gdf["buffer_name"].apply(
        lambda x: x if pd.notnull(x) else "not_even_close"
    )
    gdf["gers_h3"] = gdf["id"].str[:16]
    # 3. Dedupe
    gdf = gdf.sort_values(by=["score"], ascending=False)
    gdf = gdf.drop_duplicates(subset="id", keep="first")
    gdf = gdf.reset_index(drop=True)

    cols = ["id", "buffer_name", "gers_h3", "score", "geometry"]
    print(gdf[["id", "buffer_name", "score"]])

    return gdf[cols]
