#To Get your username and password, Please visit https://urs.earthdata.nasa.gov 
@fused.udf
def udf(bbox,
        collection_id='HLSS30.v2.0', # Landsat:'HLSL30.v2.0' & Sentinel:'HLSS30.v2.0'
        band='B8A', # Landsat:'B05' & Sentinel:'B8A'
        date_range = '2023-11/2024-01',
        max_cloud_cover=25, 
        n_mosaic=5,
        min_max=(0,3000), 
        username='your_username', 
        password='your_password',
        env='earthdata'):
    z = bbox.z[0]
    if z>=9:
        from utils import mosaic_tiff, set_earth_session, arr_to_plasma
        import numpy as np
        from pystac_client import Client  
        STAC_URL = 'https://cmr.earthdata.nasa.gov/stac' 
        catalog = Client.open(f'{STAC_URL}/LPCLOUD/')
        search = catalog.search(
        collections=[collection_id],
        bbox=bbox.total_bounds,
        datetime=date_range, 
        limit=100  
        )
        item_collection = search.get_all_items()
        band_urls = []
        for i in item_collection:
            if (i.properties['eo:cloud_cover'] <= max_cloud_cover 
                and i.collection_id == collection_id
                and band in i.assets):
                url = i.assets[band].href
                url = url.replace('https://data.lpdaac.earthdatacloud.nasa.gov/', 's3://')
                band_urls.append(url)
        if len(band_urls)==0:
            print('No items were found. Please check your `collection_id` and `band` and widen your `date_range`.')
            return None
        try:
            set_earth_session(cred = {'env':env, 'username':username, 'password':password})
        except:
            print('Please set `username` and `password` arguments. Create credentials at: https://urs.earthdata.nasa.gov to register and manage your Earthdata Login account.')
            return None
        mosaic_reduce_function = lambda x:np.max(x, axis=0)
        arr = mosaic_tiff(bbox, band_urls[:n_mosaic], mosaic_reduce_function, overview_level=max(0,12-z))
        output = arr_to_plasma(arr, min_max)
        print(f'{i.assets.keys()=}')
        print(f'{len(band_urls)=}')
        print(f'{arr.min()=}')
        print(f'{arr.max()=}')
        
        return output
    elif z>=8:
        print('Almost there! Please zoom in a bit more. 😀')
    else:
        print('Please zoom in more.')