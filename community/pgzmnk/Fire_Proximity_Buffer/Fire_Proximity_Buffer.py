@fused.udf
def udf(
    # bbox: fused.types.TileGDF=None,
    bbox: fused.types.ViewportGDF = None,
    date_start: int = 2,
    date_end: int = 1,
    h3_size: int = 7,
):
    import datetime

    import geopandas as gpd
    import h3
    import pandas as pd
    import requests
    import shapely

    # Function to convert unix to datetime
    date_string = lambda x: datetime.datetime.utcfromtimestamp(x / 1000).strftime(
        "%Y-%m-%d"
    )

    # Fetch the GeoJSON data from the remote URL
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
    cols = [
        "geometry",
        "poly_IncidentName",
        "poly_CreateDate",
        "poly_DateCurrent",
        "poly_PolygonDateTime",
        "attr_ContainmentDateTime",
        "attr_ControlDateTime",
        "attr_EstimatedCostToDate",
        "attr_FFReportApprovedDate",
        "attr_FireBehaviorGeneral",
        "attr_FireCause",
        "attr_FireDiscoveryDateTime",
        "attr_FireMgmtComplexity",
        "attr_FireOutDateTime",
        "attr_ICS209ReportDateTime",
        "attr_InitialResponseDateTime",
    ]

    gdf_fire = gdf_fire[cols]
    gdf_fire["_attr_FireDiscoveryDateTime"] = gdf_fire[
        "attr_FireDiscoveryDateTime"
    ].apply(date_string)
    gdf_fire["_poly_DateCurrent"] = gdf_fire["poly_DateCurrent"].apply(date_string)

    # Create a buffer of 1000 meters
    gdf_fire.crs = "EPSG:4326"
    gdf_fire = gdf_fire.to_crs(gdf_fire.estimate_utm_crs())
    # gdf_fire['geometry'] = gdf_fire['geometry'].buffer(1000)

    buffers = {
        "historic_perimeter": 0,
        "near_historic_perimeter": 1_000,
        "outside_perimeter": 10_000,
    }
    gdfs = []
    for buffer_name, buffer in buffers.items():
        _gdf = gdf_fire
        # Create a buffer
        _gdf = _gdf.to_crs(_gdf.estimate_utm_crs())
        _gdf["geometry"] = _gdf["geometry"].buffer(buffer)
        _gdf = _gdf.to_crs("EPSG:4326")
        _gdf["buffer_name"] = buffer_name
        gdfs.append(_gdf)

    gdf_fire = pd.concat(gdfs)
    return gdf_fire
