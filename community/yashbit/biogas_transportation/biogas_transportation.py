@fused.udf
def udf(bbox: fused.types.TileGDF = None, n=10):
    import json
    import pandas
    import numpy as np
    import core_utils
    import ee
    import geopandas as gpd
    import pandas as pd
    import shapely

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils

    """
    User Needs to: Create GCP Account (Free) and do the following:
    
    Credentials:
    1. Create Google Cloud Project
    2. IAM -> Service Accounts
    3. Grant Full Access
    4. Copy Details
    5. Register project for Google Earth Engine
    6. Enable GEE API for Google Earth Engine
    
    @@@ NOTE: PLEASE ADD YOUR API KEYS in core_utils.py module

    Visit the website:
    https://developers.google.com/earth-engine/guides/access
    """

    try:
        service_account_info = core_utils.generate_service_account_info()
        credentials = ee.ServiceAccountCredentials("", service_account_info)
        ee.Initialize(
            opt_url="https://earthengine.googleapis.com", credentials=credentials
        )
    except Exception as e:
        print(
            "Error initializing Earth Engine. Please ensure that you have set up your API keys correctly in core_utils.py."
        )
        print(f"Detailed error: {e}")
        return None

    # Plot Methane Hotspots
    methane_air_l4 = ee.FeatureCollection(
        "EDF/MethaneSAT/MethaneAIR/methaneair-L4point-2021"
    )
    info = methane_air_l4.getInfo()
    features_2 = info["features"]
    gdf = gpd.GeoDataFrame.from_features(features_2)
    features_2 = gdf.apply(core_utils.create_geojson_feature, axis=1).tolist()
    gdf_with_geojson = gpd.GeoDataFrame(gdf, geometry="geometry")
    gdf_with_geojson["geometry"] = features_2
    features_2 = []
    gdf_with_geojson.columns = [str(col) for col in gdf_with_geojson.columns]
    for key, values in gdf_with_geojson.items():
        if key == "geometry":
            features_2.extend(values)

    gdf_from_features_2 = gpd.GeoDataFrame.from_features(features_2)

    # Let's Plot the Gas Pipelines
    file_path_gas_pipelines = "https://docs.google.com/spreadsheets/d/12sd92oueYtnSqyJPCk1KY4RetzRS4L18/export?format=csv"
    gas_pipeline_df = pd.read_csv(file_path_gas_pipelines)
    clean_df = gas_pipeline_df.drop(
        gas_pipeline_df[
            (gas_pipeline_df["WKTFormat"] == "--")
            | (gas_pipeline_df["LengthEstimateKm"] == "--")
            | (gas_pipeline_df["CapacityBcm/y"] == "--")
        ].index
    )
    df_sum = (
        clean_df.groupby("StartCountry")
        .agg({"LengthEstimateKm": "sum", "CapacityBcm/y": "sum"})
        .reset_index()
    )
    geo_df = gpd.GeoDataFrame(clean_df)
    geo_df = geo_df.drop(geo_df[(geo_df["WKTFormat"] == "--")].index)
    us_geo_df = geo_df[geo_df["StartCountry"] == "USA"]
    linestring_df = us_geo_df[
        ~us_geo_df.WKTFormat.str.contains("MULTILINESTRING")
    ].copy()
    linestring_df["ls"] = linestring_df.WKTFormat.apply(
        core_utils.convert_linestr_to_linestr
    )
    linestring_df.head()

    features = []
    for feature, name, startcountry, pipe in zip(
        linestring_df.ls,
        linestring_df.Countries,
        linestring_df.StartCountry,
        linestring_df.PipelineName,
    ):
        if isinstance(feature, shapely.geometry.linestring.LineString):
            linestrings = [feature]
        elif isinstance(feature, shapely.geometry.multilinestring.MultiLineString):
            linestrings = feature.geoms
        else:
            continue
        for linestring in linestrings:
            new_coordinates = [[x, y] for x, y in linestring.coords]
            feature_dict = {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": new_coordinates},
                "properties": {
                    "Name": name + "*" + pipe,
                    "Country": startcountry,
                    "r": 173,
                    "g": 216,
                    "b": 230,
                },
            }
            features.append(feature_dict)

    geojson_data = {"type": "FeatureCollection", "features": features}
    gdf_from_features = gpd.GeoDataFrame.from_features(features)
    geojson_data = gdf_from_features.__geo_interface__
    geojson_str = json.dumps(geojson_data)
    combined_gdf = core_utils.combine_gdfs(gdf_from_features, gdf_from_features_2)
    return combined_gdf
