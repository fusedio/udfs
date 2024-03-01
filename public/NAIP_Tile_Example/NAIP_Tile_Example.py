@fused.udf
def udf(bbox, var='NDVI', chip_len='256', buffer_degree=0.000,):
    read_tiff = fused.core.import_from_github('https://github.com/fusedio/udfs/tree/ccbab4334b0cfa989c0af7d2393fb3d607a04eef/public/common/').utils.read_tiff
    if bbox.z[0]>=15:
        from utils import read_tiff, bbox_stac_items, arr_to_png
        import numpy as np
        output_shape= (int(chip_len), int(chip_len))
        matching_items = bbox_stac_items(bbox.geometry[0], table='s3://fused-asset/imagery/naip/')
        print(f'{len(matching_items)=}')
        if len(matching_items) < 10:
            input_tiff_path = matching_items.iloc[0].assets['naip-analytic']['href']
            crs = matching_items.iloc[0]['proj:epsg']
            arr=read_tiff(bbox, input_tiff_path, crs, buffer_degree, output_shape)
            if var=='RGB':
                arr = arr[:3]
            elif var=='NDVI':
                ir=arr[3].astype(float)
                red=arr[0].astype(float)
                tresh=.1
                mask = ((ir-red)/(ir+red)) < tresh
                arr[3][mask] = 0
                arr=np.array([arr[3]*1, arr[3]*0, arr[3]*0])
            else:
                raise ValueError(f'{var=} does not exist. var options are "RGB" and "NDVI"')
            return arr_to_png(arr)
        else:
            print('Too many images. Please zoom in more.')
            return None
    else:
        print('Please zoom in more.')
        
    
