# Note: Place your GEE credentials json in the `key_path` and set your `acct_serv`

@fused.udf
def udf(
    bounds: fused.types.Bounds=[-110.936,31.761,-110.793,31.890],
    acct_serv: str = "wgewneondataexplorer-7cd53ea0f@eminent-tesla-172116.iam.gserviceaccount.com"
):
    import ee
    import xarray
    import numpy as np
    import xee
    import json
     
    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)
    zoom = tile.iloc[0].z

    # Authenticate GEE
    key_path = '/mnt/cache/gp_creds.json'
    credentials = ee.ServiceAccountCredentials(acct_serv, key_path)
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)
  
    # Create collection
    geom = ee.Geometry.Rectangle(*bounds)
    scale = 1 / 2 ** max(0, zoom)
    
    def mask_img(img):
        msk = img.select(['R', 'G', 'B']).gt(0)
        return img.updateMask(msk) 
        
    # Get NEON RGB 
    ic = ee.ImageCollection("projects/neon-prod-earthengine/assets/RGB/001")\
        .filterDate('2017-01-01', '2018-01-01')\
        .filter(ee.Filter.eq('NEON_SITE', 'SRER'))\
        .map(mask_img)
 
    # # Open GEE object using xarray
    ds = xarray.open_dataset(ic, engine='ee', geometry=geom, scale=scale).isel(time=0)
    
    R = ds['R'].values.squeeze().T.astype(float)
    G = ds['G'].values.squeeze().T.astype(float)
    B = ds['B'].values.squeeze().T.astype(float)
    A = B.copy()
    
    R[R<1]=np.nan
    G[G<1]=np.nan
    B[B<1]=np.nan
    A[B==np.nan]=0
    # Get RGB values and stack them
    arr = np.stack([R,G,B,A], axis =0)

    arr_scaled = np.clip(arr, 0, 255).astype(np.uint8)

    return arr_scaled

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
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)

    # Authenticate GEE
    key_path = "/mnt/cache/gp_creds.json"
    credentials = ee.ServiceAccountCredentials(
        "wgewneondataexplorer-7cd53ea0f@eminent-tesla-172116.iam.gserviceaccount.com", key_path
    )
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)

    # Create collection
    geom = ee.Geometry.Rectangle(*bounds)
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

