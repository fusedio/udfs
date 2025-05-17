@fused.udf
def udf(
    bounds: fused.types.Bounds=[-117.644,34.416,-117.639,34.421],
    date_start: int = 2,
):

    from utils import load_fire_buffer_gdf
    gdf_fire = load_fire_buffer_gdf()
    return gdf_fire
