@fused.udf
def udf(path: str, *, bounds: fused.types.Bounds = [-180, -90, 180, 90], chip_len:int=512):
    common = fused.load("https://github.com/fusedio/udfs/tree/6dd2c4e/public/common/")
    arr = common.read_tiff_safe(bounds, path, chip_len, max_pixel=10**8, colormap=None)
    return arr
