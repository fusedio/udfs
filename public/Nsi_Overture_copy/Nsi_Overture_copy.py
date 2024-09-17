@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    target_metric: str = "val_struct",
):
    from utils import load_nsi_gdf, load_overture_gdf

    # Load both datasets
    use_columns = [
        "id",
        "geometry",
        "version",
        "update_time",
        "sources",
        "names",
        "class",
    ]  # 'update_time', 'sources', 'names'
    gdf_overture = load_overture_gdf(
        bbox, overture_type="building", use_columns=use_columns
    )
    gdf_nsi = load_nsi_gdf(bbox)

    # Spatial join - keeps the polygon geometry
    out = gdf_overture.sjoin(gdf_nsi)

    # Drop cases where two points fall within one polygon
    out.drop_duplicates(subset="id", keep="first", inplace=True, ignore_index=True)

    # Reduce column output list
    out_columns = [
        "id",
        "geometry",
        "st_damcat",
        "occtype",
        "med_yr_blt",
        "val_struct",
        "val_cont",
        "val_vehic",
    ]
    out_columns.append(target_metric)
    out = out[list(set(out_columns))]
    out["stats"] = (out["val_struct"] / 10_000).astype(int)

    return out
