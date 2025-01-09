import geopandas as gpd

aoi = [-117.64193653, 34.41836955, -117.64157577, 34.41861018]


@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    # bbox: fused.types.ViewportGDF=None,
    # bbox: fused.types.Bbox=aoi,
    h3_size: int = 7,
):
    import datetime

    import geopandas as gpd
    import h3
    import pandas as pd
    import requests
    import shapely

    # Handle custom bbox
    if isinstance(bbox, shapely.geometry.polygon.Polygon):
        bbox = gpd.GeoDataFrame({}, geometry=[bbox])

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
    gdf_fire.crs = "EPSG:4326"

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
    cols = ["geometry", "id", "buffer_name", "gers_h3"]
    print(gdf[["id", "buffer_name"]])

    # OUTPUT A: Buildings
    # 3. Return subset of buildings within the perimeter
    # return gdf[cols]

    # 4. Load Overture Places
    gdf = fused.run(
        "UDF_Overture_Maps_Example", bbox=bbox, overture_type="place", min_zoom=10
    )
    if (len(gdf) == 0) or "categories" not in gdf:
        return

    # 5. Normalize the 'categories' column into individual columns
    categories_df = pd.json_normalize(gdf["categories"]).reset_index(drop=True)
    categories_df.rename(columns={"primary": "categories_primary"}, inplace=True)
    names_df = pd.json_normalize(gdf["names"]).reset_index(drop=True)
    names_df.rename(columns={"primary": "names_primary"}, inplace=True)

    # 6. Concatenate the new columns back into the original GeoDataFrame
    gdf2 = pd.concat(
        [
            gdf.drop(columns=["categories", "names"]).reset_index(),
            categories_df,
            names_df,
        ],
        axis=1,
    )
    gdf2["h3_index"] = gdf2.geometry.apply(
        lambda p: h3.latlng_to_cell(p.y, p.x, h3_size)
    )

    # 7. Group by H3, create categories primary set
    gdf3 = gdf2.dissolve(
        by="h3_index",
        as_index=False,
        aggfunc={
            "categories_primary": lambda x: list([y for y in set(x) if pd.notna(y)]),
            "sources": "count",
        },
    )
    gdf3.rename(columns={"sources": "cnt"}, inplace=True)
    # OUTPUT B: H3 categories
    # Return subset H3 Rollup
    return gdf3[["h3_index", "categories_primary", "cnt"]]

    return gdf_fire
