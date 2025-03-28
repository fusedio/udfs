@fused.cache
def fetch_rgb_udf(
    bounds: fused.types.Bounds = None,
    neon_site: str = "SRER"
):
    import ee
    import numpy as np
    import xarray
    import xee

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    # Authenticate GEE
    key_path = "/mnt/cache/gp_creds.json"
    credentials = ee.ServiceAccountCredentials(
        "wgewneondataexplorer-7cd53ea0f@eminent-tesla-172116.iam.gserviceaccount.com", key_path
    )
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)

    # Create collection
    geom = ee.Geometry.Rectangle(*tile.total_bounds)
    scale = 1 / 2 ** max(0, zoom)

    # Get NEON RGB image
    ic = ee.ImageCollection("projects/neon-prod-earthengine/assets/RGB/001")\
        .filterDate("2017-01-01", "2018-01-01")\
        .filter(ee.Filter.eq("NEON_SITE", neon_site))

    # Open GEE object using xarray
    ds = xarray.open_dataset(ic, engine="ee", geometry=geom, scale=scale).isel(time=0)

    # Extract RGB bands
    R = ds["R"].values.squeeze().T.astype(float)
    G = ds["G"].values.squeeze().T.astype(float)
    B = ds["B"].values.squeeze().T.astype(float)

    # Replace invalid values with NaN
    R[R < 1] = np.nan
    G[G < 1] = np.nan
    B[B < 1] = np.nan

    # Stack RGB bands and clip values
    arr = np.stack([R, G, B], axis=0)
    arr_scaled = np.clip(arr, 1, 255).astype(np.uint8)

    return arr_scaled

def get_gee_credentials():
    import ee
    key_path = '/mnt/cache/gp_creds.json'
    credentials = ee.ServiceAccountCredentials("wgewneondataexplorer-7cd53ea0f@eminent-tesla-172116.iam.gserviceaccount.com", key_path)
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)
