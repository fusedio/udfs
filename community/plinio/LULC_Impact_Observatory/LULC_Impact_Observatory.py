@fused.udf
def udf(bounds: fused.types.Tile, lulc_type:str="wetland,water,crop", path: str = 's3://io-monitor-orders/0f2f3a0533/crop-mosaic_0f2f3a0533_fs_20240101_20241231_aoi.tif'):
    import numpy as np
    import numpy.ma as ma
    import geopandas as gpd

    # Read tiff
    arr_int, metadata= fused.utils.common.read_tiff(bounds, path, output_shape=(256,256), return_colormap=True)
    arr_int = np.array([value for value in np.array(arr_int).flat], dtype=np.uint8).reshape(arr_int.shape)

    # Filter by LULC type    
    if lulc_type:
        import numpy as np
        values_to_keep = lulc_to_int(lulc_type)
        print('values_to_keep: ', values_to_keep)
        if len(values_to_keep) > 0:
            mask = np.isin(arr_int, values_to_keep, invert=True)
            arr_int[mask] = 0
        else:
            print(f"{lulc_type=} was not found in the list of lulc types")

    return np.array([metadata["colormap"][value] for value in np.array(arr_int).flat], dtype=np.uint8).reshape(arr_int.shape + (4,)).transpose(2, 0, 1)


def lulc_to_int(lulc_type):
    import pandas as pd
    from utils import land_classes

    df_meta = pd.DataFrame(land_classes)
    out = []

    for term in map(str.strip, lulc_type.split(',')):
        mask = df_meta.name.str.lower().str.contains(term.lower())
        codes = df_meta[mask].code.values
        out.extend(codes)
    return out

    