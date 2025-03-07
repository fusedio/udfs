@fused.udf
def udf(bounds: fused.types.TileGDF = None, join_with_nsi: bool=True):
    import geopandas as gpd
    import pandas as pd
    import requests

    if bounds.iloc[0].z < 10:
        return None

    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.
    
    # 1. Load Overture Buildings
    gdf_overture = overture_utils.get_overture(bounds=bounds)

    if not join_with_nsi:
        gdf_overture['metric'] = gdf_overture['height']
        return gdf_overture
        
    # 2. Load NSI from API
    response = requests.post(
        url="https://nsi.sec.usace.army.mil/nsiapi/structures?fmt=fc",
        json=bounds.__geo_interface__,
    )

    # 3. Create NSI gdf
    gdf = gpd.GeoDataFrame.from_features(response.json()["features"])
    if len(gdf) == 0:
        return None

    # 4. Join Overture and NSI
    cols = [
        "id",
        "geometry",
        "metric",
        "ground_elv_m",
        "height",
        "num_floors",
        "num_story",
    ]
    join = gdf_overture.sjoin(gdf, how='left')
    join["metric"] = join.apply(lambda row: row.height if pd.notnull(row.height) else row.num_story*3, axis=1)
    return join[cols]
