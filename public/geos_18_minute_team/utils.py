def file_of_day(i, bucket_name, product_name, band, start_datestr='2024-01-30', hour_offset='7'):
    if product_name in ('ABI-L1b-RadF', 'ABI-L2-CMIPF'):
        nfile=6
    elif product_name in ('ABI-L1b-RadC', 'ABI-L2-CMIPC'):
        nfile=12
    else:
        print(f'{product_name} not in ABI-L1b-RadF | ABI-L1b-RadC | ABI-L2-CMIPF')

    import datetime
    import numpy as np
    import s3fs
    start_time = int(datetime.datetime.strptime(start_datestr, '%Y-%m-%d').timestamp())
    hourly_timestamps = []
    hour = i//nfile
    hour_timestamp = start_time + hour * 3600  
    start_datetime = datetime.datetime.strptime(start_datestr, '%Y-%m-%d')
    hour_datetime = start_datetime + datetime.timedelta(hours=hour+int(hour_offset))
    day_of_year = hour_datetime.timetuple().tm_yday
    hourly_timestamps.append([hour_datetime.year, day_of_year, hour_datetime.hour])
    blob = f'{product_name}/{hour_datetime.year}/{day_of_year:03.0f}/{hour_datetime.hour:02.0f}'
    prefix = f'OR_{product_name}-M6C{band:02.0f}'
    s3 = s3fs.S3FileSystem(anon=True)
    flist = s3.ls(f'{bucket_name}/{blob}')
    flist = np.sort([i for i in flist if i.split('/')[-1].startswith(prefix)])
    return flist[i%nfile][len(product_name):]


def chunkify(chunk_size, xy_size, pad=10):
    """
    e.g. slice_list = chunkify(chunk_size=(500,500), xy_size=(1200,2000), pad=10)
    """
    x_len, y_len = xy_size
    # Calculate the number of chunks along each dimension
    n_chunks_x = x_len // (chunk_size[1] - pad) + 1
    n_chunks_y = y_len // (chunk_size[0] - pad) + 1
    xy_slices = []
    # Create chunks
    for i in range(n_chunks_y):
        for j in range(n_chunks_x):
            start_idx_x = j * (chunk_size[1] - pad)
            end_idx_x = min(start_idx_x + chunk_size[1], x_len)
            start_idx_y = i * (chunk_size[0] - pad)
            end_idx_y = min(start_idx_y + chunk_size[0],  y_len)
            xy_slices.append([slice(start_idx_x,end_idx_x), slice(start_idx_y,end_idx_y)])
    return xy_slices

def roi_ds_chunks(roi, xds, slice_list, valid_list=[], return_data=True):
    import numpy as np
    if len(valid_list)>0:
        slice_list=valid_list
    ds_list=[]
    for xy_slice in slice_list:
        try:
            ds = xds.isel(x=xy_slice[0], y=xy_slice[1])
            ds_list.append(ds.rio.reproject(roi.crs).rio.clip(roi.geometry))
            if xy_slice not in valid_list:
                valid_list.append(xy_slice)
            print(xy_slice)
        except Exception as e:
            print(xy_slice, e)
    if return_data:
        return ds_list
    else: 
        return valid_list

def combine_resample(ds_chunks,variable='CMI', xy_res=(2000,2000)):
    import numpy as np
    minx, miny = np.min([[i.x.min().values,i.y.min().values] for i in ds_chunks],axis=0)
    maxx, maxy = np.max([[i.x.max().values,i.y.max().values] for i in ds_chunks],axis=0)
    xrange = np.arange(minx, maxx, xy_res[0])
    # Nonte: y axis need to be top to bottom
    yrange = np.arange(maxy, miny, -xy_res[1])
    da_interp=[i.interp(x=xrange,y=yrange)[variable] for i in ds_chunks]
    #TODO: if in the jupyter-notebook change the nanmin to nanmax
    da_interp[0][:]=np.nanmin(np.stack(da_interp), axis=0)
    return da_interp[0]
        
plasma_colors = [[ 12,   7, 135],
       [ 16,   7, 136],
       [ 19,   6, 137],
       [ 22,   6, 138],
       [ 24,   6, 140],
       [ 27,   6, 141],
       [ 29,   6, 142],
       [ 31,   5, 143],
       [ 33,   5, 144],
       [ 35,   5, 145],
       [ 38,   5, 146],
       [ 40,   5, 146],
       [ 42,   5, 147],
       [ 43,   5, 148],
       [ 45,   4, 149],
       [ 47,   4, 150],
       [ 49,   4, 151],
       [ 51,   4, 151],
       [ 53,   4, 152],
       [ 54,   4, 153],
       [ 56,   4, 154],
       [ 58,   4, 154],
       [ 60,   3, 155],
       [ 61,   3, 156],
       [ 63,   3, 156],
       [ 65,   3, 157],
       [ 66,   3, 158],
       [ 68,   3, 158],
       [ 70,   3, 159],
       [ 71,   2, 160],
       [ 73,   2, 160],
       [ 75,   2, 161],
       [ 76,   2, 161],
       [ 78,   2, 162],
       [ 80,   2, 162],
       [ 81,   1, 163],
       [ 83,   1, 163],
       [ 84,   1, 164],
       [ 86,   1, 164],
       [ 88,   1, 165],
       [ 89,   1, 165],
       [ 91,   0, 165],
       [ 92,   0, 166],
       [ 94,   0, 166],
       [ 95,   0, 166],
       [ 97,   0, 167],
       [ 99,   0, 167],
       [100,   0, 167],
       [102,   0, 167],
       [103,   0, 168],
       [105,   0, 168],
       [106,   0, 168],
       [108,   0, 168],
       [110,   0, 168],
       [111,   0, 168],
       [113,   0, 168],
       [114,   0, 169],
       [116,   0, 169],
       [117,   0, 169],
       [119,   1, 168],
       [120,   1, 168],
       [122,   1, 168],
       [123,   2, 168],
       [125,   2, 168],
       [126,   3, 168],
       [128,   3, 168],
       [129,   4, 167],
       [131,   4, 167],
       [132,   5, 167],
       [134,   6, 167],
       [135,   7, 166],
       [136,   7, 166],
       [138,   8, 166],
       [139,   9, 165],
       [141,  11, 165],
       [142,  12, 164],
       [144,  13, 164],
       [145,  14, 163],
       [146,  15, 163],
       [148,  16, 162],
       [149,  17, 161],
       [150,  18, 161],
       [152,  19, 160],
       [153,  20, 160],
       [155,  21, 159],
       [156,  23, 158],
       [157,  24, 157],
       [158,  25, 157],
       [160,  26, 156],
       [161,  27, 155],
       [162,  28, 154],
       [164,  29, 154],
       [165,  30, 153],
       [166,  32, 152],
       [167,  33, 151],
       [169,  34, 150],
       [170,  35, 149],
       [171,  36, 149],
       [172,  37, 148],
       [173,  38, 147],
       [175,  40, 146],
       [176,  41, 145],
       [177,  42, 144],
       [178,  43, 143],
       [179,  44, 142],
       [180,  45, 141],
       [181,  46, 140],
       [183,  47, 139],
       [184,  49, 138],
       [185,  50, 137],
       [186,  51, 137],
       [187,  52, 136],
       [188,  53, 135],
       [189,  54, 134],
       [190,  55, 133],
       [191,  57, 132],
       [192,  58, 131],
       [193,  59, 130],
       [194,  60, 129],
       [195,  61, 128],
       [196,  62, 127],
       [197,  63, 126],
       [198,  64, 125],
       [199,  66, 124],
       [200,  67, 123],
       [201,  68, 122],
       [202,  69, 122],
       [203,  70, 121],
       [204,  71, 120],
       [205,  72, 119],
       [206,  73, 118],
       [207,  75, 117],
       [208,  76, 116],
       [208,  77, 115],
       [209,  78, 114],
       [210,  79, 113],
       [211,  80, 112],
       [212,  81, 112],
       [213,  83, 111],
       [214,  84, 110],
       [215,  85, 109],
       [215,  86, 108],
       [216,  87, 107],
       [217,  88, 106],
       [218,  89, 105],
       [219,  91, 105],
       [220,  92, 104],
       [220,  93, 103],
       [221,  94, 102],
       [222,  95, 101],
       [223,  96, 100],
       [224,  98,  99],
       [224,  99,  98],
       [225, 100,  98],
       [226, 101,  97],
       [227, 102,  96],
       [227, 104,  95],
       [228, 105,  94],
       [229, 106,  93],
       [230, 107,  92],
       [230, 108,  92],
       [231, 110,  91],
       [232, 111,  90],
       [232, 112,  89],
       [233, 113,  88],
       [234, 114,  87],
       [235, 116,  86],
       [235, 117,  86],
       [236, 118,  85],
       [237, 119,  84],
       [237, 121,  83],
       [238, 122,  82],
       [238, 123,  81],
       [239, 124,  80],
       [240, 126,  80],
       [240, 127,  79],
       [241, 128,  78],
       [241, 129,  77],
       [242, 131,  76],
       [242, 132,  75],
       [243, 133,  74],
       [244, 135,  73],
       [244, 136,  73],
       [245, 137,  72],
       [245, 139,  71],
       [246, 140,  70],
       [246, 141,  69],
       [247, 143,  68],
       [247, 144,  67],
       [247, 145,  67],
       [248, 147,  66],
       [248, 148,  65],
       [249, 149,  64],
       [249, 151,  63],
       [249, 152,  62],
       [250, 154,  61],
       [250, 155,  60],
       [251, 156,  60],
       [251, 158,  59],
       [251, 159,  58],
       [251, 161,  57],
       [252, 162,  56],
       [252, 164,  55],
       [252, 165,  54],
       [252, 166,  54],
       [253, 168,  53],
       [253, 169,  52],
       [253, 171,  51],
       [253, 172,  50],
       [253, 174,  49],
       [254, 175,  49],
       [254, 177,  48],
       [254, 178,  47],
       [254, 180,  46],
       [254, 181,  46],
       [254, 183,  45],
       [254, 185,  44],
       [254, 186,  43],
       [254, 188,  43],
       [254, 189,  42],
       [254, 191,  41],
       [254, 192,  41],
       [254, 194,  40],
       [254, 195,  40],
       [254, 197,  39],
       [254, 199,  39],
       [253, 200,  38],
       [253, 202,  38],
       [253, 203,  37],
       [253, 205,  37],
       [253, 207,  37],
       [252, 208,  36],
       [252, 210,  36],
       [252, 212,  36],
       [251, 213,  36],
       [251, 215,  36],
       [251, 217,  36],
       [250, 218,  36],
       [250, 220,  36],
       [249, 222,  36],
       [249, 223,  36],
       [248, 225,  37],
       [248, 227,  37],
       [247, 229,  37],
       [247, 230,  37],
       [246, 232,  38],
       [246, 234,  38],
       [245, 235,  38],
       [244, 237,  39],
       [244, 239,  39],
       [243, 241,  39],
       [242, 242,  38],
       [242, 244,  38],
       [241, 246,  37],
       [241, 247,  36],
       [240, 249,  33]]


#todo: use arr_to_color in arr_to_plasma
def arr_to_plasma(data, min_max=(0,255), colormap=plasma_colors): 
    import numpy as np 
    data = data.astype(float)
    if min_max:
        norm_data = (data - min_max[0])/(min_max[1]-min_max[0])
        norm_data = np.clip(norm_data,0,1)           
    else:
        print(f'min_max:({round(np.nanmin(data),3)},{round(np.nanmax(data),3)})')
        norm_data = (data - np.nanmin(data))/(np.nanmax(data)-np.nanmin(data))            
    norm_data255 = (norm_data*255).astype('uint8')
    if colormap:
        mapped_colors = np.array([colormap[val] for val in norm_data255.flat])
        return mapped_colors.reshape(data.shape+(3,)).astype('uint8').transpose(2,0,1)
    else:
        return norm_data255