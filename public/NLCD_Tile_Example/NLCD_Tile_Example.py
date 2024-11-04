@fused.udf
def udf(bbox: fused.types.TileGDF, year:int=1985, land_type:str='', chip_len:int=256, colored: bool = True):
    import numpy as np
    from utils import type_counts, filter_lands, rgb_to_hex
    path= f"https://s3-us-west-2.amazonaws.com/mrlc/Annual_NLCD_LndCov_{year}_CU_C1V0.tif"
    utils = fused.load('https://github.com/fusedio/udfs/tree/004b8d9/public/common/').utils
    arr_int, color_map = utils.read_tiff(bbox, path, output_shape=(chip_len, chip_len), return_colormap=True)
    if land_type:
        arr_int = filter_lands(arr_int, land_type, verbose=False)
    arr_colored = np.array([color_map[value] for value in arr_int.data.flat], dtype=np.uint8).reshape(arr_int.shape + (4,)).transpose(2, 0, 1)   
    df = type_counts(arr_int)
    df['color'] = df.index.map(lambda x: rgb_to_hex(color_map[x]) if x in color_map else "NaN")
    print(df[['land_type','color','n_pixel']])
    if colored:
        return arr_colored
    else:
        return arr_int