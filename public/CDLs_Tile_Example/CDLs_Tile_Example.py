@fused.udf 
def udf(bbox, year='2022', crop_type='', chip_len=256):
    read_tiff = fused.core.import_from_github('https://github.com/fusedio/udfs/tree/a63664f4a4451d07efd003e318a1413c51a54889/public/common').utils.read_tiff
    from utils import filter_crops, crop_counts
    import numpy as np 
    input_tiff_path=f"s3://fused-asset/data/cdls/{year}_30m_cdls.tif"
    arr, color_map = read_tiff(bbox, input_tiff_path, output_shape=(chip_len,chip_len), return_colormap=True)
    if crop_type:
        arr = filter_crops(arr, crop_type, verbose=False)
    print(crop_counts(arr).head(20))
    colored_array = np.array([color_map[value] for value in arr.flat], dtype=np.uint8).reshape(arr.shape + (4,)).transpose(2,0,1)
    return colored_array
