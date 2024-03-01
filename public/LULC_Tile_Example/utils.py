import numpy as np
import fused
read_tiff = fused.core.import_from_github('https://github.com/fusedio/udfs/tree/ccbab4334b0cfa989c0af7d2393fb3d607a04eef/public/common/').utils.read_tiff
mosaic_tiff = fused.core.import_from_github('https://github.com/fusedio/udfs/tree/ccbab4334b0cfa989c0af7d2393fb3d607a04eef/public/common/').utils.mosaic_tiff
LULC_IO_COLORS = {1:( 65, 155, 223), #Water
                  2:( 57, 125, 73), #Trees
                  4:( 57, 125, 73), #Flooded vegetation
                  5:( 228, 150, 53), #Crops
                  7:(196, 40, 27), #Built area
                  8:(165, 155, 143), #Bare ground
                  9:(168, 235, 255), #Snow
                  10:(97, 97, 97), #Clouds
                  11:(227, 226, 195), #Rangeland
                  }

def s3_to_https(path):
    arr = path[5:].split('/')
    out = 'https://'+arr[0]+'.s3.amazonaws.com/'+'/'.join(arr[1:])
    return out

def arr_to_color(arr, color_map={1:( 65, 155, 223 ), 2:( 57, 125, 73 )}):
    import numpy as np
    def map_values(value, band=0):
        if value in color_map:
            return color_map[value][band]
        else:
            return 0
    mapped_arr = [np.vectorize(lambda x:map_values(x,i))(arr) for i in [0,1,2]]
    return np.asarray(mapped_arr).astype('uint8')

def bbox_stac_items(bbox, table):
    import fused
    import pandas as pd
    df = fused.get_chunks_metadata(table)
    df = df[df.intersects(bbox)]
    List = df[['file_id','chunk_id']].values
    rows_df = pd.concat([fused.get_chunk_from_table(table, fc[0], fc[1]) for fc in List])
    return rows_df[rows_df.intersects(bbox)]
