@fused.udf 
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-12-18.0",
    theme: str = None,
    overture_type: str = None, 
    use_columns: list = None,
    num_parts: int = 6,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    from utils import get_overture
    
    gdf = get_overture(
        bbox=bbox,
        release=release,
        theme=theme,
        overture_type=overture_type,
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )

    # Color by source
    gdf['_src'] = gdf['sources'].apply(lambda x: 'EASTASIA' if x is None else 'OVERTURE')
    
    # Filter out columns
    gdf = gdf[['_src', 'geometry', 'sources']]

    # Calculate area/perimeter ratio to filter out noise
    gdf["area_m2"] = gdf.to_crs(gdf.estimate_utm_crs()).area.round(2)
    gdf["perimeter_m"] = gdf.to_crs(gdf.estimate_utm_crs()).length.round(2)
    gdf["aop"] = gdf["area_m2"] / gdf["perimeter_m"]
    gdf["aop_positive"] = gdf["aop"].apply(lambda x: x>1)
    
    return gdf