utils = fused.load('https://github.com/fusedio/udfs/tree/e1fefb7/public/common/').utils

def get_data(bbox, year, land_type, chip_len):
        path= f"https://s3-us-west-2.amazonaws.com/mrlc/Annual_NLCD_LndCov_{year}_CU_C1V0.tif"
        arr_int, metadata = utils.read_tiff(bbox, path, output_shape=(chip_len, chip_len), return_colormap=True)
        colormap = metadata['colormap']
        if land_type:
            arr_int = filter_lands(arr_int, land_type, verbose=False)    
        return arr_int, colormap

def get_summary(arr_int, colormap):
    df = type_counts(arr_int)
    df['color'] = df.index.map(lambda x: rgb_to_hex(colormap[x]) if x in colormap else "NaN")
    return df[['land_type', 'color', 'n_pixel']]

nlcd_category_dict = {
            11: "Open Water",
            12: "Perennial Ice/Snow",
            21: "Developed, Open Space",
            22: "Developed, Low Intensity",
            23: "Developed, Medium Intensity",
            24: "Developed, High Intensity",
            31: "Barren Land",
            41: "Deciduous Forest",
            42: "Evergreen Forest",
            43: "Mixed Forest",
            52: "Shrub/Scrub",
            71: "Herbaceous",
            81: "Hay/Pasture",
            82: "Cultivated Crops",
            90: "Woody Wetlands",
            95: "Emergent Herbaceous Wetlands"
            }
def filter_lands(arr, land_type, verbose=True):
    import numpy as np
    values_to_keep = [key for key, value in nlcd_category_dict.items() if land_type.lower() in value.lower()]
    if len(values_to_keep) > 0:
        mask = np.isin(arr, values_to_keep, invert=True)
        arr[mask] = 0
        return arr
    else:
        print(f"{land_type=} was not found in the list of landcovers")
        return arr

def type_counts(arr, nlcd_category_dict=nlcd_category_dict):
        import numpy as np
        import pandas as pd
        unique_values, counts = np.unique(arr, return_counts=True)
        df = pd.DataFrame(counts, index=unique_values, columns=["n_pixel"]).sort_values(
            by="n_pixel", ascending=False
        )
        df['land_type'] = df.index.map(nlcd_category_dict)
        return df[df.index!=0] #remove the zero pixels
def rgb_to_hex(rgb):
    return '#{:02x}{:02x}{:02x}'.format(rgb[0], rgb[1], rgb[2])