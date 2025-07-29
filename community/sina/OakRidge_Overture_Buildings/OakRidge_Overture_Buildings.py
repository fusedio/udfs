@fused.udf
def udf(bounds: fused.types.Bounds = [-122.432,37.800,-122.430,37.802]):
    import geopandas as gpd
    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    overture = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/Overture_Maps_Example/")

    # Convert bounds to tile
    tile = common.get_tiles(bounds, clip=True)

    # 1. Load Overture Buildings
    gdf_overture = overture.get_overture(bounds=bounds)

    # 2. Load Oak Ridge Buildings
    gdf_oakridge = common.table_to_tile(
        tile, table="s3://fused-asset/infra/building_oak_states/state=ca/", min_zoom=10
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
    gdf_overture["source"] = "Overture"

    # return intersection
    return gdf_overture[["geometry", "source"]]
    return gdf_oakridge
