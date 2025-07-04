color_map = {
    0: [255, 255, 255, 255],  # #ffffff - No change
    1: [0, 0, 255, 255],      # #0000ff - Permanent
    2: [34, 177, 76, 255],    # #22b14c - New permanent
    3: [209, 16, 45, 255],    # #d1102d - Lost permanent
    4: [153, 217, 234, 255],  # #99d9ea - Seasonal
    5: [181, 230, 29, 255],   # #b5e61d - New seasonal
    6: [230, 161, 170, 255],  # #e6a1aa - Lost seasonal
    7: [255, 127, 39, 255],   # #ff7f27 - Seasonal to permanent
    8: [255, 201, 14, 255],   # #ffc90e - Permanent to seasonal
    9: [127, 127, 127, 255],  # #7f7f7f - Ephemeral permanent
    10: [195, 195, 195, 255]  # #c3c3c3 - (Unspecified)
}
description_mapping = {
    0: "No change",
    1: "Permanent",
    2: "New permanent",
    3: "Lost permanent",
    4: "Seasonal",
    5: "New seasonal",
    6: "Lost seasonal",
    7: "Seasonal to permanent",
    8: "Permanent to seasonal",
    9: "Ephemeral permanent",
    10: "Unspecified"
}



def ee_initialize(service_account_name="", key_path=""):
    import xee
    import ee
    
    # Authenticate GEE
    credentials = ee.ServiceAccountCredentials(service_account_name, key_path)

    ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com', credentials=credentials)


utils = fused.load('https://github.com/fusedio/udfs/tree/004b8d9/public/common/').utils

def get_data(bbox, year, land_type, chip_len):
        path= f"https://s3-us-west-2.amazonaws.com/mrlc/Annual_NLCD_LndCov_{year}_CU_C1V0.tif"
        arr_int, color_map = utils.read_tiff(bbox, path, output_shape=(chip_len, chip_len), return_colormap=True)
        if land_type:
            arr_int = filter_lands(arr_int, land_type, verbose=False)    
        return arr_int, color_map

def get_summary(arr_int, color_map):
    df = type_counts(arr_int)
    df['color'] = df.index.map(lambda x: rgb_to_hex(color_map[x]) if x in color_map else "NaN")
    return df[['land_type', 'color', 'n_pixel']]



def rgb_to_hex(rgb):
    return '#{:02x}{:02x}{:02x}'.format(rgb[0], rgb[1], rgb[2])