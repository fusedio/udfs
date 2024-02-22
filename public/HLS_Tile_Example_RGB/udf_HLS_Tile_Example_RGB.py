#To Get your username and password, Please visit https://urs.earthdata.nasa.gov 
@fused.udf
def udf(bbox,
        collection_id='HLSS30.v2.0', # Landsat:'HLSL30.v2.0' & Sentinel:'HLSS30.v2.0'
        bands=['B04', 'B03', 'B02'],
        date_range = '2023-11/2024-01',
        max_cloud_cover=25, 
        n_mosaic=5,
        username='your_username', 
        password='your_password'):
    z = bbox.z[0]
    if z>=9: 
        from utils import get_hls
        import numpy as np
        a=[]
        for band in bands: 
            a.append(get_hls(bbox, username, password, band, date_range, 
                                 collection_id, max_cloud_cover, n_mosaic))
        rgb= (a[0]/10, 
              a[1]/10, 
              a[2]/10)
        output = np.asarray(rgb).astype('uint8')
        print(output.shape)
        return output
    elif z>=8:
        print('Almost there! Please zoom in a bit more. 😀')
    else:
        print('Please zoom in more.')