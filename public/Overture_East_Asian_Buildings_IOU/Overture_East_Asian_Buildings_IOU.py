@fused.udf
def udf(bounds: fused.types.Bounds = None, res=14):
    import h3
    import pandas as pd

    # Load pinned versions of utility functions.
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/99dbfec/public/Overture_Maps_Example/").utils
    utils = fused.load("https://github.com/fusedio/udfs/tree/d0e8eb0/public/common/").utils

    # convert bounds to tile
    tile = utils.get_tiles(bounds)

    # 1. Load East Asia Zenodo Buildings
    path = "s3://fused-asset/misc/jennings/East_Asian_buildings_parquet3_ingested_3dec/"
    gdf_zenodo = utils.table_to_tile(bounds, table=path)
    print("Bulding Count EA: ", len(gdf_zenodo))

    # 2. Load Overture Buildings
    gdf_overture = overture_utils.get_overture(bounds=tile)
    print("Bulding Count Overture: ", len(gdf_overture))

    # 3. IOU
    out = gdf_zenodo.overlay(gdf_overture, how="union")
    out = gdf_zenodo.overlay(gdf_overture, how="symmetric_difference")

    # 3.1 Intersection
    out = gdf_zenodo.overlay(gdf_overture, how="intersection")
    out["id_count"] = out.groupby("id")["id"].transform("count")
    out["hex"] = out.geometry.apply(lambda x: h3.geo_to_cells(x, res))
    out2 = out.explode("hex")
    out2["how"] = "intersection"

    # 3.2 Symmetric Difference
    out = gdf_zenodo.overlay(gdf_overture, how="symmetric_difference")
    out["id_count"] = out.groupby("id")["id"].transform("count")
    out["hex"] = out.geometry.apply(lambda x: h3.geo_to_cells(x, res))
    out3 = out.explode("hex")
    out3["how"] = "symmetric_difference"

    # 3.3 Concat
    out4 = pd.concat([out2, out3])

    # 4. Calculate IOU for each 'id'
    counts = out4.groupby(["id", "how"]).size().unstack(fill_value=0)
    counts["ratio"] = counts["intersection"] / (
        counts["intersection"] + counts["symmetric_difference"]
    )
    out4 = out4.merge(counts["ratio"], on="id")

    cols = ["hex", "how", "ratio", "id"]
    print(out4[cols])
    return out4[cols]
