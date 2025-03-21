@fused.udf 
def udf(
    bounds: fused.types.Tile = None,
    release: str = "2025-01-22-0",
    theme: str = None,
    overture_type: str = None, 
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    from utils import get_overture
    
    gdf = get_overture(
        bounds=bounds,
        release=release,
        theme=theme,
        overture_type=overture_type,
        use_columns=use_columns,
        num_parts=num_parts,
        min_zoom=min_zoom,
        polygon=polygon,
        point_convert=point_convert
    )
    return gdf
