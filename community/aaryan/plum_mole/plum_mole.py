# 'Shift + Enter' to execute this UDF as you pan the map or change the code directly
@fused.udf
def udf(bounds: fused.types.Bounds = None, buffer_multiple: float = 1):
    # Using common fused functions as helper
    common = fused.load("https://github.com/fusedio/udfs/tree/495e84e/public/common/").utils
    # This helper function turns our bounds into XYZ tiles
    tiles = common.get_tiles(bounds, target_num_tiles=16)
    # Buffering tiles internally
    tiles.geometry = tiles.buffer(buffer_multiple / (tiles.z.iloc[0]) ** 2)
    # Use print statements to debug
    print(f"{tiles.geometry.area.sample(3)=}")
    return tiles
