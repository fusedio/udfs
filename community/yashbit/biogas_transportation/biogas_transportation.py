@fused.udf
def udf(bbox: fused.types.Tile = None, n=10):
    import json
    import ee
    import geopandas as gpd
    import pandas as pd
    import shapely

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")


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
        service_account_info = generate_service_account_info()
        common.ee_initialize(service_account_name='fused-nyt-gee@fused-nyt.iam.gserviceaccount.com',key_path="/mnt/cache/geecreds.json")

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
    features_2 = gdf.apply(create_geojson_feature, axis=1).tolist()
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
        convert_linestr_to_linestr
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
    combined_gdf = combine_gdfs(gdf_from_features, gdf_from_features_2)
    return combined_gdf



def combine_gdfs(gdf_from_features, gdf_from_features_2):
    import geopandas as gpd
    import pandas as pd
    
    combined_gdf = gpd.GeoDataFrame(
        pd.concat([gdf_from_features, gdf_from_features_2], ignore_index=True)
    )
    return combined_gdf


def generate_service_account_info():
    import json
    
    service_account_info = {
        "type": "",
        "project_id": "",
        "private_key_id": "",
        "private_key": "-----BEGIN PRIVATE KEY-----",
        "client_email": "",
        "client_id": "",
        "auth_uri": "",
        "token_uri": "",
        "auth_provider_x509_cert_url": "",
        "client_x509_cert_url": "",
        "universe_domain": "",
    }
    file_path = "service_account_info.json"
    with open(file_path, "w") as json_file:
        json.dump(service_account_info, json_file)
    return file_path


def convert_linestr_to_linestr(s):
    import pandas as pd
    import shapely.geometry
    
    cc = s.lstrip("LINESTRING (").rstrip(")").split(",")
    cc[0] = " " + cc[0]
    ccc = pd.DataFrame(cc)[0].str.split(" ", expand=True)
    ls = shapely.geometry.linestring.LineString(
        list(ccc[[1, 2]].astype(float).to_records(index=False))
    )
    return ls


def create_geojson_feature(row):
    import shapely.geometry
    
    lon, lat = row["geometry"].x, row["geometry"].y
    delta = 0.005
    row_flux_multiple = row["Flux_kg_hr"]
    true_delta = delta * (row_flux_multiple / 500)
    lon2 = lon + true_delta
    lat2 = lat + true_delta
    lon3 = lon - true_delta
    lat3 = lat + true_delta
    coordinates = [
        [lon, lat],
        [lon2, lat2],
        [lon, lat + 2 * true_delta],
        [lon3, lat3],
        [lon, lat],
    ]
    properties = {
        "Name": "Methane Gas Density",
        "Country": "USA",
        "Flux_kg_hr": row["Flux_kg_hr"],
        "flt": row["flt"],
        "plume_numb": row["plume_numb"],
        "sd": row["sd"],
        "r": 255,
        "g": 255,
        "b": 0,
    }
    feature = {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [coordinates]},
        "properties": properties,
    }
    return feature