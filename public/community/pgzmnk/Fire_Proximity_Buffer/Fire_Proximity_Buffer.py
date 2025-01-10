@fused.udf
def udf(
    bbox: fused.types.TileGDF=None,
    date_start: int = 2,
):

    from utils import load_fire_buffer_gdf
    gdf_fire = load_fire_buffer_gdf()
    return gdf_fire
