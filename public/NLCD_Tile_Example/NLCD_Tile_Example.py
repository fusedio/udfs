@fused.udf
def udf(bounds: fused.types.Bounds, year:int=1985, land_type:str='', chip_len:int=256, colored: bool = True):
    import numpy as np
    from utils import get_data, get_summary

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    
    arr_int, color_map = get_data(tile, year, land_type, chip_len)
    print(get_summary(arr_int, color_map))
    
    if colored:
        arr_flat = np.array([color_map[value] for value in arr_int.data.flat], dtype=np.uint8)
        return arr_flat.reshape(arr_int.shape + (4,)).transpose(2, 0, 1)
    else:
        return arr_int