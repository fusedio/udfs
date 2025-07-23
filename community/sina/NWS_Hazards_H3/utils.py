import fused
import pandas as pd
import geopandas as gpd
import requests

@fused.cache
def fetch_all_features(url, params):
    """
    Fetch all features from a paginated API. Makes multiple requests if the number of features exceeds maxRecordCount of a single call.

    Args:
    url (str): The API endpoint URL.
    params (dict): The query parameters for the API call, including 'maxRecordCount'.

    Returns:
    list: A list of all features retrieved from the API.
    """
    
    all_features = []
    max_record_count = params["maxRecordCount"]
    
    while True:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if "features" in data and data["features"]:
                all_features.extend(data["features"])
                if len(data["features"]) < max_record_count:
                    break
                # Update params for the next page
                params['resultOffset'] = params.get('resultOffset', 0) + max_record_count
            else:
                break
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    return all_features

@fused.cache
def add_rgb_cmap(gdf, key_field, cmap_dict):
    """
    Apply a colormap dictionary to a GeoDataFrame based on a specified key field.

    This function adds 'r', 'g', and 'b' columns to a GeoDataFrame, where the values
    are determined by a colormap dictionary based on the values in a specified key field.

    Args:
    gdf (GeoDataFrame): The GeoDataFrame to which the colormap will be applied.
    key_field (str): The column in the GeoDataFrame whose values will be used to look up the colormap.
    cmap_dict (dict): A dictionary mapping key_field values to RGB color lists.

    Returns:
    GeoDataFrame: The input GeoDataFrame with additional 'r', 'g', and 'b' columns.
    """
    
    gdf[["r", "g", "b"]] = gdf[key_field].apply(
        lambda key_field: pd.Series(cmap_dict.get(key_field, [255, 0, 255]))
    )
    return gdf

"""NWS Hazard Type Colormap"""
CMAP = {
    "Tsunami Warning": [253, 99, 71],
    "Tornado Warning": [255, 0, 0],
    "Extreme Wind Warning": [255, 140, 0],
    "Severe Thunderstorm Warning": [255, 165, 0],
    "Flash Flood Warning": [139, 0, 0],
    "Flash Flood Statement": [139, 0, 0],
    "Severe Weather Statement": [0, 255, 255],
    "Shelter In Place Warning": [250, 128, 114],
    "Evacuation Immediate": [127, 255, 0],
    "Civil Danger Warning": [255, 182, 193],
    "Nuclear Power Plant Warning": [75, 0, 130],
    "Radiological Hazard Warning": [75, 0, 130],
    "Hazardous Materials Warning": [75, 0, 130],
    "Fire Warning": [160, 82, 45],
    "Civil Emergency Message": [255, 182, 193],
    "Law Enforcement Warning": [192, 192, 192],
    "Storm Surge Warning": [181, 36, 247],
    "Hurricane Force Wind Warning": [205, 92, 92],
    "Hurricane Warning": [220, 20, 60],
    "Typhoon Warning": [220, 20, 60],
    "Special Marine Warning": [255, 165, 0],
    "Blizzard Warning": [255, 69, 0],
    "Snow Squall Warning": [199, 21, 133],
    "Ice Storm Warning": [139, 0, 139],
    "Winter Storm Warning": [255, 105, 180],
    "High Wind Warning": [218, 165, 32],
    "Tropical Storm Warning": [178, 34, 34],
    "Storm Warning": [148, 0, 211],
    "Tsunami Advisory": [210, 105, 30],
    "Tsunami Watch": [255, 0, 255],
    "Avalanche Warning": [30, 144, 255],
    "Earthquake Warning": [139, 69, 19],
    "Volcano Warning": [47, 79, 79],
    "Ashfall Warning": [169, 169, 169],
    "Coastal Flood Warning": [34, 139, 34],
    "Lakeshore Flood Warning": [34, 139, 34],
    "Flood Warning": [0, 255, 0],
    "High Surf Warning": [34, 139, 34],
    "Dust Storm Warning": [255, 228, 196],
    "Blowing Dust Warning": [255, 228, 196],
    "Lake Effect Snow Warning": [0, 139, 139],
    "Excessive Heat Warning": [199, 21, 133],
    "Tornado Watch": [255, 255, 0],
    "Severe Thunderstorm Watch": [219, 112, 147],
    "Flash Flood Watch": [46, 139, 87],
    "Gale Warning": [221, 160, 221],
    "Flood Statement": [0, 255, 0],
    "Wind Chill Warning": [176, 196, 222],
    "Extreme Cold Warning": [0, 0, 255],
    "Hard Freeze Warning": [148, 0, 211],
    "Freeze Warning": [72, 61, 139],
    "Red Flag Warning": [255, 20, 147],
    "Storm Surge Watch": [219, 127, 247],
    "Hurricane Watch": [255, 0, 255],
    "Hurricane Force Wind Watch": [153, 50, 204],
    "Typhoon Watch": [255, 0, 255],
    "Tropical Storm Watch": [240, 128, 128],
    "Storm Watch": [255, 228, 181],
    "Hurricane Local Statement": [255, 228, 181],
    "Typhoon Local Statement": [255, 228, 181],
    "Tropical Storm Local Statement": [255, 228, 181],
    "Tropical Depression Local Statement": [255, 228, 181],
    "Avalanche Advisory": [205, 133, 63],
    "Winter Weather Advisory": [123, 104, 238],
    "Wind Chill Advisory": [175, 238, 238],
    "Heat Advisory": [255, 127, 80],
    "Urban and Small Stream Flood Advisory": [0, 255, 127],
    "Small Stream Flood Advisory": [0, 255, 127],
    "Arroyo and Small Stream Flood Advisory": [0, 255, 127],
    "Flood Advisory": [0, 255, 127],
    "Hydrologic Advisory": [0, 255, 127],
    "Lakeshore Flood Advisory": [124, 252, 0],
    "Coastal Flood Advisory": [124, 252, 0],
    "High Surf Advisory": [186, 85, 211],
    "Heavy Freezing Spray Warning": [0, 191, 255],
    "Dense Fog Advisory": [112, 128, 144],
    "Dense Smoke Advisory": [240, 230, 140],
    "Small Craft Advisory": [216, 191, 216],
    "Brisk Wind Advisory": [216, 191, 216],
    "Hazardous Seas Warning": [216, 191, 216],
    "Dust Advisory": [189, 183, 107],
    "Blowing Dust Advisory": [189, 183, 107],
    "Lake Wind Advisory": [210, 180, 140],
    "Wind Advisory": [210, 180, 140],
    "Frost Advisory": [100, 149, 237],
    "Ashfall Advisory": [105, 105, 105],
    "Freezing Fog Advisory": [0, 128, 128],
    "Freezing Spray Advisory": [0, 191, 255],
    "Low Water Advisory": [165, 42, 42],
    "Local Area Emergency": [192, 192, 192],
    "Avalanche Watch": [244, 164, 96],
    "Blizzard Watch": [173, 255, 47],
    "Rip Current Statement": [64, 224, 208],
    "Beach Hazards Statement": [64, 224, 208],
    "Gale Watch": [255, 192, 203],
    "Winter Storm Watch": [70, 130, 180],
    "Hazardous Seas Watch": [72, 61, 139],
    "Heavy Freezing Spray Watch": [188, 143, 143],
    "Coastal Flood Watch": [102, 205, 170],
    "Lakeshore Flood Watch": [102, 205, 170],
    "Flood Watch": [46, 139, 87],
    "High Wind Watch": [184, 134, 11],
    "Excessive Heat Watch": [128, 0, 0],
    "Extreme Cold Watch": [0, 0, 255],
    "Wind Chill Watch": [95, 158, 160],
    "Lake Effect Snow Watch": [135, 206, 250],
    "Hard Freeze Watch": [65, 105, 225],
    "Freeze Watch": [0, 255, 255],
    "Fire Weather Watch": [255, 222, 173],
    "Extreme Fire Danger": [233, 150, 122],
    "911 Telephone Outage": [192, 192, 192],
    "Coastal Flood Statement": [107, 142, 35],
    "Lakeshore Flood Statement": [107, 142, 35],
    "Special Weather Statement": [255, 228, 181],
    "Marine Weather Statement": [255, 239, 213],
    "Air Quality Alert": [128, 128, 128],
    "Air Stagnation Advisory": [128, 128, 128],
    "Hazardous Weather Outlook": [238, 232, 170],
    "Hydrologic Outlook": [144, 238, 144],
    "Short Term Forecast": [152, 251, 152],
    "Administrative Message": [192, 192, 192],
    "Test": [240, 255, 255],
    "Child Abduction Emergency": [255, 255, 255],
    "Blue Alert": [255, 255, 255],
}