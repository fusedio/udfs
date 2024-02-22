@fused.udf
def udf(bbox, var='NDVI', chip_len='256', buffer_degree=0.000,):
    if bbox.z[0]>=15:
        from utils import bbox_stac_items, read_tiff, arr_to_png
        import numpy as np
        output_shape= (int(chip_len), int(chip_len))
        matching_items = bbox_stac_items(bbox.geometry[0], table='s3://fused-asset/imagery/naip/')
        print(f'{len(matching_items)=}')
        if len(matching_items) < 10:
            input_tiff_path = matching_items.iloc[0].assets['naip-analytic']['href']
            crs = matching_items.iloc[0]['proj:epsg']
            arr=read_tiff(bbox, input_tiff_path, crs, buffer_degree, output_shape, resample_order=0)
            if var=='RGB':
                arr = arr[:3]
            elif var=='NDVI':
                ir=arr[3].astype(float)
                red=arr[0].astype(float)
                tresh=.4
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