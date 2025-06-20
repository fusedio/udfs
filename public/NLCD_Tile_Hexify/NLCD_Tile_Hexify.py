nlcd_example_utils = fused.load('https://github.com/fusedio/udfs/tree/c0a9abf/public/NLCD_Tile_Example/').utils
common_utils = fused.load("https://github.com/fusedio/udfs/tree/36f4e97/public/common/").utils

@fused.udf
def udf(bounds: fused.types.Bounds=[-121.673,37.561,-120.778,38.314], year:int=1985, land_type:str='', chip_len:int=256):
    import numpy as np
    import pandas as pd

    # convert bounds to tile
    tile = common_utils.get_tiles(bounds, clip=True)

    #initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    res_offset = 1  # lower makes the hex finer
    res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
    print(res)

    # read tiff file
    data = get_data(tile, year, land_type, chip_len)
    if data is None:
        print(f"No data found for tile {tile}")
        return None
    arr_int, color_map = data

    # hexify tiff array
    df = common_utils.arr_to_h3(arr_int, bounds, res=res, ordered=False)

    # find most frequet land_type
    df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
    df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
    df=df[df['most_freq']>0]
    if len(df)==0:
        print(f"Empty dataframe for tile {tile}")
        return

    # get the color and land_type
    df[['r', 'g', 'b', 'a']] = df.most_freq.map(lambda x: pd.Series(color_map[x])).apply(pd.Series)
    df['land_type'] = df.most_freq.map(nlcd_example_utils.nlcd_category_dict)
    df['color'] = df.most_freq.map(lambda x: nlcd_example_utils.rgb_to_hex(color_map[x]) if x in color_map else "NaN")
    df=df[['hex', 'land_type', 'color', 'r', 'g', 'b', 'a', 'most_freq','n_pixel']]

    #print the stats for each tiles
    print(df.groupby(['color','land_type'])['n_pixel'].sum().sort_values(ascending=False))

    return df


def get_data(bounds, year, land_type, chip_len):
        path = f"https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_LndCov_{year}_CU_C1V0.tif"
        tiff = common_utils.read_tiff(bounds, path, output_shape=(chip_len, chip_len), return_colormap=True)
        if tiff is None:
            return None
        arr_int, metadata = tiff
        colormap = metadata['colormap']
        if land_type:
            arr_int = filter_lands(arr_int, land_type, verbose=False)    
        return arr_int, colormap