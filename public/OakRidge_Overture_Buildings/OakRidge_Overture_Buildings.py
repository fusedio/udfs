@fused.udf
def udf(bounds: fused.types.Tile = None):
    import geopandas as gpd
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils # Load pinned versions of utility functions.

    # 1. Load Overture Buildings
    gdf_overture = overture_utils.get_overture(bounds=bounds)

    # 2. Load Oak Ridge Buildings
    gdf_oakridge = utils.table_to_tile(
        bounds, table="s3://fused-asset/infra/building_oak_states/state=ca/", min_zoom=10
    )

    # 3. Calculate intersection
    intersection = gpd.overlay(gdf_overture, gdf_oakridge, how="intersection")

    # 4. Calculate the areas (per tile)
    intersection_area = intersection.geometry.area.sum()
    gdf_overture_area = gdf_overture.geometry.area.sum()
    gdf_oakridge_area = gdf_oakridge.geometry.area.sum()

    # 5. Calculate Intersection over Union (IoU)
    iou = intersection_area / (
        gdf_overture_area + gdf_oakridge_area - intersection_area
    )
    print(f"IOU: {iou}")

    # 6. Overture sources
    gdf_overture["source"] = gdf_overture["sources"].apply(lambda x: x[0]["dataset"])

    # return intersection
    return gdf_overture[["geometry", "source"]]
    return gdf_oakridge
