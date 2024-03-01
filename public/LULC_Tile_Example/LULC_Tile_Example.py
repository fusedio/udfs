@fused.udf
def udf(bbox, context,year='2022'):
    if bbox.z[0]>=5:
        from utils import mosaic_tiff, bbox_stac_items, s3_to_https, arr_to_color, LULC_IO_COLORS
        matching_items = bbox_stac_items(bbox.geometry[0], table='s3://fused-asset/lulc/io10m/')
        mask = matching_items['datetime'].map(lambda x:str(x)[:4]==year)
        tiff_list = matching_items[mask].assets.map(lambda x:s3_to_https(x['supercell']['href'][:-17]+'.tif')).values
        data = mosaic_tiff(bbox, tiff_list, output_shape=(256,256), overview_level = min(max(12-bbox.z[0],0),4))
        data = arr_to_color(data,color_map=  LULC_IO_COLORS)
        return data
    else:
        print('Please zoom in more.')

