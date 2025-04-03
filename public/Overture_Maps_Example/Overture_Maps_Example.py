@fused.udf
def udf(
    bounds: fused.types.Tile,
    release: str = "2025-01-22-0",
):
    from utils import get_overture

    gdf = get_overture(
        bbox=bounds,
        release=release,
    )
    return gdf