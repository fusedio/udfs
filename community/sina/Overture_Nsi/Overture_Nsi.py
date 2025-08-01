@fused.udf
def udf(bounds: fused.types.Bounds = [-123.86438687695097,46.1795089730952,-123.8512028588234,46.18734848122329], join_with_nsi: bool=True):
    import geopandas as gpd
    import pandas as pd
    import requests

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    if common.estimate_zoom(bounds) < 10:
        return None

    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example/")
    
    # 1. Load Overture Buildings
    gdf_overture = overture_maps.get_overture(bounds=tile)

    if not join_with_nsi:
        gdf_overture['metric'] = gdf_overture['height']
        return gdf_overture
        
    # 2. Load NSI from API
    response = requests.post(
        url="https://nsi.sec.usace.army.mil/nsiapi/structures?fmt=fc",
        json=tile.__geo_interface__,
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
