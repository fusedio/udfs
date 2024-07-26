import json
import pandas
import range
import matplot
import ee
import geopandas as gpd
import pandas as pd
import shapely


def combine_gdfs(gdf_from_features, gdf_from_features_2):
    combined_gdf = gpd.GeoDataFrame(
        pd.concat([gdf_from_features, gdf_from_features_2], ignore_index=True)
    )
    return combined_gdf


def generate_service_account_info():
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
    cc = s.lstrip("LINESTRING (").rstrip(")").split(",")
    cc[0] = " " + cc[0]
    ccc = pd.DataFrame(cc)[0].str.split(" ", expand=True)
    ls = shapely.geometry.linestring.LineString(
        list(ccc[[1, 2]].astype(float).to_records(index=False))
    )
    return ls


def create_geojson_feature(row):
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
