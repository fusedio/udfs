@fused.udf
def udf(bounds: fused.types.Bounds=[-121.673,37.561,-120.778,38.314], year:int=1985, land_type:str='', chip_len:int=256):
    import numpy as np        
    import pandas as pd
    from utils import get_data, arr_to_h3, nlcd_category_dict, rgb_to_hex

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)

    #initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    res_offset = 1  # lower makes the hex finer
    res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
    print(res)
    
    # read tiff file
    arr_int, color_map = get_data(tile, year, land_type, chip_len)

    # hexify tiff array
    
    df = arr_to_h3(arr_int, bounds, res=res, ordered=False)

    # find most frequet land_type
    df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
    df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
    df=df[df['most_freq']>0]
    if len(df)==0: return 

    # get the color and land_type
    df[['r', 'g', 'b', 'a']] = df.most_freq.map(lambda x: pd.Series(color_map[x])).apply(pd.Series)
    df['land_type'] = df.most_freq.map(nlcd_category_dict)
    df['color'] = df.most_freq.map(lambda x: rgb_to_hex(color_map[x]) if x in color_map else "NaN")
    df=df[['hex', 'land_type', 'color', 'r', 'g', 'b', 'a', 'most_freq','n_pixel']]

    #print the stats for each tiles
    print(df.groupby(['color','land_type'])['n_pixel'].sum().sort_values(ascending=False))
    
    return df