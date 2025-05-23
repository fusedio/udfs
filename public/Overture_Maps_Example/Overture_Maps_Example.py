@fused.udf
def udf(
    bounds: fused.types.Bounds = [-0.113, 51.503, -0.099, 51.513],
    release: str = "2025-05-21-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    clip: bool = None,
    point_convert: str = None,
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
        clip=clip,
        point_convert=point_convert,
    )
    return gdf
