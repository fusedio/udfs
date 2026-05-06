@fused.udf
def udf(
    bounds: fused.types.Bounds = [6.6272, 36.6197, 18.4802, 47.0921],
    chip_len: int = 512
):
    path: str = 's3://fused-asset/demos/seismic_data/v2023_1_pga_475_rock_3min.tif'
    common = fused.load('https://github.com/fusedio/udfs/tree/6750f1f/public/common/')
    arr = common.read_tiff_safe(bounds, path, chip_len, max_pixel=10 ** 8)

    print(arr)
    return arr
