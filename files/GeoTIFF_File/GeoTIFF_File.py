@fused.udf
def udf(bounds: fused.types.Bounds, path: str, *, chip_len=256):
    common = fused.load("https://github.com/fusedio/udfs/tree/8080427/public/common/")    
    arr = common.read_tiff_safe(bounds, path, chip_len, max_pixel=10**8, colormap='plasma')
    return arr
