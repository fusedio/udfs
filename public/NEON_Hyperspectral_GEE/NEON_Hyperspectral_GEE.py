
@fused.udf
def udf(bbox: fused.types.TileGDF=None, acct_serv: str = "wgewneondataexplorer-7cd53ea0f@eminent-tesla-172116.iam.gserviceaccount.com"):

    import ee
    import xarray
    import numpy as np
    import xee
    import json
    
     
    # Authenticate GEE
    key_path = '/mnt/cache/gp_creds.json'
    credentials = ee.ServiceAccountCredentials(acct_serv, key_path)
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials)
  
    # Create collection
    geom = ee.Geometry.Rectangle(*bbox.total_bounds)
    scale = 1 / 2 ** max(0, bbox.z[0])
    
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
    
