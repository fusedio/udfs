@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    h3_size: int = 7,
):
    import geopandas as gpd
    import h3
    import shapely
    import pandas as pd

    from utils import load_fire_buffer_gdf

    # Handle custom bbox
    if isinstance(bbox, shapely.geometry.polygon.Polygon):
        bbox = gpd.GeoDataFrame({}, geometry=[bbox])

    # 1. Load fire buffers
    gdf_fire = load_fire_buffer_gdf()

    # 2. Load Overture Buildings
    gdf = fused.run("UDF_Overture_Maps_Example", bbox=bbox, overture_type="building", min_zoom=10)
    if (len(gdf) == 0) or "sources" not in gdf: return

    gdf = gdf.sjoin(gdf_fire, how="left")
    gdf["buffer_name"] = gdf["buffer_name"].apply(lambda x: x if pd.notnull(x) else "not_even_close")
    gdf["gers_h3"] = gdf["id"].str[:16]
    cols = ["geometry", "id", "buffer_name", "gers_h3"]
    
    # 3. Dedupe
    gdf = gdf.sort_values(by=["score"], ascending=False)
    gdf = gdf.drop_duplicates(subset="id", keep="first")
    gdf = gdf.reset_index(drop=True)
    
    # OUTPUT A: Return subset of buildings within the perimeter
    return gdf[cols]

    # 4. Load Overture Places
    gdf = fused.run("UDF_Overture_Maps_Example", bbox=bbox, overture_type="place", min_zoom=10)
    if (len(gdf) == 0) or "categories" not in gdf:return

    # 5. Normalize the 'categories' column into individual columns
    categories_df = pd.json_normalize(gdf["categories"]).reset_index(drop=True)
    categories_df.rename(columns={"primary": "categories_primary"}, inplace=True)
    names_df = pd.json_normalize(gdf["names"]).reset_index(drop=True)
    names_df.rename(columns={"primary": "names_primary"}, inplace=True)

    # 6. Concatenate the new columns back into the original GeoDataFrame
    gdf2 = pd.concat([gdf.drop(columns=["categories", "names"]).reset_index(),categories_df,names_df,],axis=1,)
    gdf2["h3_index"] = gdf2.geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, h3_size))

    # 7. Group by H3, create categories primary set
    gdf3 = gdf2.dissolve(by="h3_index",as_index=False,aggfunc={"categories_primary": lambda x: list([y for y in set(x) if pd.notna(y)]),"sources": "count",},)
    gdf3.rename(columns={"sources": "cnt"}, inplace=True)
    # OUTPUT B: Return subset H3 Rollup
    return gdf3[["h3_index", "categories_primary", "cnt"]]