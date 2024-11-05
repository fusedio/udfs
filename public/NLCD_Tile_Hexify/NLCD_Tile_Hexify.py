@fused.udf
def udf(bbox: fused.types.TileGDF, year:int=2010, land_type:str='', chip_len:int=256):
    import numpy as np        
    import pandas as pd
    from utils import df_to_hex, nlcd_category_dict, rgb_to_hex, filter_lands
    utils = fused.load('https://github.com/fusedio/udfs/tree/bafadc1/public/common/').utils
    path= f"https://s3-us-west-2.amazonaws.com/mrlc/Annual_NLCD_LndCov_{year}_CU_C1V0.tif"
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    res_offset = 1  # lower makes the hex finer
    res = max(min(int(3 + bbox.z[0] / 1.5), 12) - res_offset, 2)
    print(res)

    # read tiff file
    arr_int, color_map = utils.read_tiff(bbox, path, output_shape=(chip_len, chip_len), return_colormap=True)
    if land_type:
        arr_int = filter_lands(arr_int, land_type, verbose=False)

    # pointify
    df = utils.arr_to_latlng(arr_int, bbox.total_bounds)

    # hexify
    df = utils.df_to_hex(df, res=res, ordered=False)

    # hexify
    df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
    df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
    df[['r', 'g', 'b', 'a']] = df.most_freq.map(lambda x: pd.Series(color_map[x])).apply(pd.Series)
    df['land_type'] = df.most_freq.map(nlcd_category_dict)
    df['color'] = df.most_freq.map(lambda x: rgb_to_hex(color_map[x]) if x in color_map else "NaN")
    df=df[['hex', 'land_type', 'color', 'r', 'g', 'b', 'a', 'most_freq','n_pixel']]
    print(df.groupby(['color','land_type'])['n_pixel'].sum().sort_values(ascending=False))
    df=df[df['most_freq']>0]
    return df