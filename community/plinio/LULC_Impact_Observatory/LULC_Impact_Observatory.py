@fused.udf
def udf(bounds: fused.types.Bounds, lulc_type:str="wetland,water,crop", path: str = 's3://fused-asset/data/san-juan-del-pirari_20210415-20210714_data.tif'):
    import numpy as np
    import numpy.ma as ma
    import geopandas as gpd

    # Read tiff
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    arr_int, metadata= common.read_tiff(tile, path, output_shape=(256,256), return_colormap=True)
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

    df_meta = pd.DataFrame(land_classes)
    out = []

    for term in map(str.strip, lulc_type.split(',')):
        mask = df_meta.name.str.lower().str.contains(term.lower())
        codes = df_meta[mask].code.values
        out.extend(codes)
    return out


land_classes = [
    {"name": "Water Channel Extent", "code": 11, "color": "#dcedfa", "description": "Full extent of water under normal circumstances (maximum long-term footprint outside of flooding or other exceptional events). Covers areas like lake beds and arroyos where water might be present regularly, but only seasonally."},
    {"name": "Variable Water", "code": 12, "color": "#51a3e1", "description": "Intermittent water flow or standing water, representing seasonal fluctuation in cover, weather events, or human activities. At times this class may occur due to frequent turbidity, algal blooms, ice cover, pollution, or glare."},
    {"name": "Persistent Water", "code": 13, "color": "#204e90", "description": "Sustained water flow or standing water, representing permanent or sustained seasonal cover."},
    {"name": "Sparse Deciduous Trees", "code": 108, "color": "#adbca0", "description": "Primarily trees that seasonally shed their leaves, mixed with other vegetation."},
    {"name": "Dense Deciduous Trees", "code": 109, "color": "#515e44", "description": "Primarily trees that seasonally shed their leaves, densely packed."},
    {"name": "Sparse Evergreen Trees", "code": 111, "color": "#afe09f", "description": "Primarily trees that retain functional foliage throughout the year, mixed with other vegetation."},
    {"name": "Dense Evergreen Trees", "code": 112, "color": "#3d6e2d", "description": "Primarily trees that retain functional foliage throughout the year, densely packed."},
    {"name": "Sparse Rangeland", "code": 61, "color": "#ece4b4", "description": "Vegetation that is some mix of grasses and/or dispersed, short, woody scrub, with or without some bare ground cover. May contain small isolated trees. Defined as a rangeland prediction with a maximum NDVI value less than a biome-specific threshold during the given time period."},
    {"name": "Dense Rangeland", "code": 62, "color": "#938225", "description": "Healthy, closely packed vegetation that is predominantly dense, short (under 5m) woody shrubs with very little to no mixed grass or bare ground cover. May contain small isolated trees. Within and around areas classified as built, this class can also include highly manicured lawns or fields. Defined as a rangeland prediction with a maximum NDVI value greater than or equal to a biome-specific threshold during the given time period."},
    {"name": "Inactive Cropland", "code": 141, "color": "#f5b27b", "description": "Fallow or otherwise inactive fields, sometimes mixed with small infrastructure in close proximity of active crops."},
    {"name": "Active Cropland", "code": 142, "color": "#e8821c", "description": "Actively growing crops, irrigated pastures, and other vegetation actively managed by humans."},
    {"name": "Snow/Ice Variable", "code": 31, "color": "#A8EBFF", "description": "Large, homogenous areas of persistent snow or ice, typically only in mountain areas or high latitudes."},
    {"name": "Snow/Ice Perennial", "code": 33, "color": "#3db785", "description": "Snow or ice that persists year-round."},
    {"name": "Mangroves", "code": 81, "color": "#856fc3", "description": "Woody vegetation located in tropical and subtropical zones which thrive in coastal saline/brackish waters."},
    {"name": "Wetland Nonwoody", "code": 82, "color": "#c1aff5", "description": "Areas of standing water intermixed with non-dense submerged and/or emergent, vegetation; some sparse woody vegetation may occur."},
    {"name": "Wetland Mixed", "code": 83, "color": "#4a3b76", "description": "Areas of vegetation where either the extent of water or type of vegetation is unclear; here, standing water is obscured by thick canopy (dense marsh or swamp), is very shallow, or is instead present in waterlogged soil (peatland, bog, fen)."},
    {"name": "Bare Ground", "code": 40, "color": "#CCCCCC", "description": "Areas of rock or soil with very sparse to no vegetation for the entire year; large areas of sand with little to no vegetation; examples: exposed rock or soil, desert, dry salt flats, dry lake beds."},
    {"name": "Low Density Built", "code": 172, "color": "#fa8075", "description": "Artificial impervious surfaces, buildings, and structures mixed with vegetation that cannot be easily detangled at 10m-resolution. These areas typically represent low-density residential areas, suburban areas, large path networks or other dispersed human construction. Defined as built area with a maximum NDVI value greater than or equal to a biome-specific threshold."},
    {"name": "High Density Built", "code": 174, "color": "#9d1206", "description": "Artificial, impervious surfaces in the form of individual features, parts of features, or tight clusters of features with little to no mixed vegetation or bare ground. Isolated clusters of these detections represent small buildings or construction, while larger clusters are indicative of commercial, industrial, or high-density residential areas. Defined as built area with a maximum NDVI value less than a biome-specific threshold."},
    {"name": "Clouds", "code": 240, "color": "#444444", "description": "No land cover information due to clouds."}
]

    