@fused.udf
def udf(
    bounds: fused.types.Bounds=None,
    var: str = 'transition', 
    # var: str = 'max_extent', # 
    return_object: str='nothex', # TODO: whether to return H3 or a polygon of the H3
    res_offset: int = -1  # lower makes the hex finer

): 
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    import h3
    
    common = fused.load("https://github.com/fusedio/udfs/tree/3991434/public/common/")

    if bounds is None:
        bounds = [-122.5, 37.7, -122.3, 37.8]

    bounds = common.bounds_to_gdf(bounds)
    z = common.estimate_zoom(bounds)
    bounds['z'] = z

    if 'z' not in bounds.columns:
        bounds['z'] = 10

    # JRC Global Surface Water COG URLs (no GEE needed)
    # Hosted on Google Cloud Storage as Cloud-Optimized GeoTIFFs
    gsw_layers = {
        'transition': 'https://storage.googleapis.com/global-surface-water/downloads2021/transitions/transitions_{lon}_{lat}.tif',
        'max_extent': 'https://storage.googleapis.com/global-surface-water/downloads2021/extent/extent_{lon}_{lat}.tif',
        'occurrence': 'https://storage.googleapis.com/global-surface-water/downloads2021/occurrence/occurrence_{lon}_{lat}.tif',
        'seasonality': 'https://storage.googleapis.com/global-surface-water/downloads2021/seasonality/seasonality_{lon}_{lat}.tif',
        'recurrence': 'https://storage.googleapis.com/global-surface-water/downloads2021/recurrence/recurrence_{lon}_{lat}.tif',
    }
    
    # Read from the global composite COG via Planetary Computer STAC
    @fused.cache
    def get_gsw_tiff(var, bounds_tuple, output_shape):
        import planetary_computer
        import pystac_client

        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
        items = catalog.search(
            collections=["jrc-gsw"],
            bbox=bounds_tuple,
        ).item_collection()

        if len(items) == 0:
            print("No JRC GSW items found for this bbox")
            return None

        # Get the COG URL for the requested variable
        item = items[0]
        # Map user-facing var names to STAC asset keys
        var_to_asset = {
            'transition': 'transitions',
            'max_extent': 'extent',
            'occurrence': 'occurrence',
            'seasonality': 'seasonality',
            'recurrence': 'recurrence',
            'change': 'change',
        }
        asset_key = var_to_asset.get(var, var)
        if asset_key not in item.assets:
            print(f"Variable '{var}' not found. Available: {list(item.assets.keys())}")
            return None
        
        href = planetary_computer.sign(item.assets[asset_key]).href
        return href

    bounds_tuple = tuple(bounds.total_bounds)
    href = get_gsw_tiff(var, bounds_tuple, (256, 256))
    
    if href is None:
        print(f"Could not find JRC GSW data for var='{var}'")
        return None
    
    print(f"Reading COG: {href[:80]}...")
    arr = common.read_tiff(bounds, href, output_shape=(256, 256))
    arr = arr.astype('uint8')
    print(f"Array shape: {arr.shape}, unique values: {np.unique(arr)[:20]}")
    
    # Hexify
    res = max(min(int(3 + z / 1.5), 12) - res_offset, 2)
    df = common.arr_to_h3(arr, bounds.total_bounds, res=res, ordered=False)

    # find most frequet land_type
    df['most_freq'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[0][np.argmax(np.unique(x, return_counts=True)[1])])
    df['n_pixel'] = df.agg_data.map(lambda x: np.unique(x, return_counts=True)[1].max())
    df=df[df['most_freq']>0]
    if len(df)==0: return 

    # get the color and land_type
    df[['r', 'g', 'b', 'a']] = df.most_freq.map(lambda x: pd.Series(color_map[x])).apply(pd.Series)
    df['transition_type'] = df.most_freq.map(description_mapping)
    df=df[['hex', 'transition_type', 'r', 'g', 'b', 'a', 'most_freq','n_pixel']]
    df['hex'] = df['hex'].apply(lambda x: h3.int_to_str(x))


    if return_object == 'hex':
        return df

    from shapely.geometry import Polygon
    import h3
    df['geometry'] = df['hex'].astype(str).apply(lambda x: Polygon([(lon, lat) for lat, lon in h3.cell_to_boundary(x)]))
    gdf = gpd.GeoDataFrame(df)
    return gdf

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



def get_data(bbox, year, land_type, chip_len):
        common = fused.load("https://github.com/fusedio/udfs/tree/3991434/public/common/")

        path= f"https://s3-us-west-2.amazonaws.com/mrlc/Annual_NLCD_LndCov_{year}_CU_C1V0.tif"
        arr_int, color_map = common.read_tiff(bbox, path, output_shape=(chip_len, chip_len), return_colormap=True)
        if land_type:
            arr_int = filter_lands(arr_int, land_type, verbose=False)    
        return arr_int, color_map

def get_summary(arr_int, color_map):
    df = type_counts(arr_int)
    df['color'] = df.index.map(lambda x: rgb_to_hex(color_map[x]) if x in color_map else "NaN")
    return df[['land_type', 'color', 'n_pixel']]



def rgb_to_hex(rgb):
    return '#{:02x}{:02x}{:02x}'.format(rgb[0], rgb[1], rgb[2])

    