@fused.udf
def udf(bounds: fused.types.TileGDF, year:int=1985, land_type:str='', chip_len:int=256):
    import numpy as np        
    import pandas as pd
    from utils import get_data, arr_to_h3, nlcd_category_dict, rgb_to_hex
    
    #initial parameters
    x, y, z = bounds.iloc[0][["x", "y", "z"]]
    res_offset = 1  # lower makes the hex finer
    res = max(min(int(3 + bounds.z[0] / 1.5), 12) - res_offset, 2)
    print(res)
    
    # read tiff file
    arr_int, color_map = get_data(bounds, year, land_type, chip_len)

    # hexify tiff array
    
    df = arr_to_h3(arr_int, bounds.total_bounds, res=res, ordered=False)

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