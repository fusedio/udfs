# to use this utils run the following command in your udf:
#utils = fused.public.utils
import fused

@fused.cache 
def acs_5yr_bbox(bounds, census_variable='population', type='bbox', suffix='simplify'):
    import json
    import shapely  
    import geopandas as gpd 
    if type == 'bbox':
        bounds = '{'+f'"west": {bounds[0]}, "north": {bounds[3]}, "east": {bounds[2]}, "south": {bounds[1]}'+'}'
        # print(bounds)
    bounds=json.loads(bounds)
    box = shapely.box(bounds['west'], bounds['south'], bounds['east'], bounds['north'])
    
    # box = shapely.box(*[-122.41, 37.68, -122.40, 38.70])

    bbox = gpd.GeoDataFrame(geometry=[box], crs=4326)
    # print(bbox)
    table_to_tile = fused.public.utils.table_to_tile
    fused.public.utils.import_env()
    tid=search_title(census_variable)  
    df = acs_5yr_table(tid)
    df['GEOID'] = df.GEO_ID.map(lambda x:x.split('US')[-1])
    table_path='s3://fused-asset/infra/census_bg_us'
    if suffix:
        table_path+=f'_{suffix}'
    print(table_path)
    gdf = table_to_tile(bbox, table_path, use_columns=['GEOID','geometry'], min_zoom=12)
    gdf2 = gdf.merge(df)  
    return gdf2 
    
@fused.cache
def acs_5yr_meta(): 
    import pandas as pd
    #Filter only records with cencus block groups data
    tmp = pd.read_excel('https://www2.census.gov/programs-surveys/acs/summary_file/2021/sequence-based-SF/documentation/tech_docs/ACS_2021_SF_5YR_Appendices.xlsx')
    table_ids_cbgs = tmp[tmp['Geography Restrictions'].isna()]['Table Number']
    #Get the list of tables and filter by only totals (the first row of each table)
    df_tables = pd.read_csv('https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/ACS20225YR_Table_Shells.txt', delimiter='|')
    df_tables2 = df_tables.drop_duplicates('Table ID')
    df_tables2 = df_tables2[df_tables2['Table ID'].isin(table_ids_cbgs)]
    return df_tables2 
    
def search_title(title):
    df_meta=acs_5yr_meta() 
    #search for title in the list of tables 
    search_column = 'Title' #'Title' #'Topics'
    meta_dict = df_meta[['Table ID', search_column]].set_index(search_column).to_dict()['Table ID']
    List = [[meta_dict[i], i] for i in meta_dict.keys() if title.lower() in i.lower()]
    print(f'Chosen: {List[0]}\nfrom: {List[:20]}')
    return List[0][0]

@fused.cache
def acs_5yr_table(tid, year=2022):
    import pandas as pd
    url=f'https://www2.census.gov/programs-surveys/acs/summary_file/{year}/table-based-SF/data/5YRData/acsdt5y{year}-{tid.lower()}.dat'
    return pd.read_csv(url, delimiter='|')


def url_to_arr(url, return_colormap=False):
        import requests
        import rasterio
        from rasterio.plot import show
        from io import BytesIO
        response = requests.get(url)
        print(response.status_code)
        with rasterio.open(BytesIO(response.content)) as dataset:
            if return_colormap:
                colormap=dataset.colormap
                return dataset.read(), dataset.colormap(1)
            else:
                return dataset.read()
            
def read_shape_zip(url, file_index=0, name_prefix=''):
    """This function opens any zipped shapefile"""
    import zipfile
    import geopandas as gpd
    path = fused.utils.download(url,name_prefix+url.split('/')[-1])
    fnames = [i.filename for i in zipfile.ZipFile(path).filelist if i.filename[-4:]=='.shp']
    df = gpd.read_file(f'{path}!{fnames[file_index]}')
    return df


@fused.cache
def get_collection_bbox(collection):
    import geopandas as gpd
    import pystac_client
    import planetary_computer
    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1",modifier=planetary_computer.sign_inplace,)
    asset = catalog.get_collection(collection).assets["geoparquet-items"]
    df = gpd.read_parquet(asset.href, storage_options=asset.extra_fields["table:storage_options"])
    return df[['assets','datetime','geometry']]

@fused.cache
def get_pc_token(url):
    import requests
    from urllib.parse import urlparse
    parsed_url = urlparse(url.rstrip("/"))
    account_name = parsed_url.netloc.split(".")[0]
    path_blob = parsed_url.path.lstrip("/").split("/", 1)
    container_name = path_blob[-2]
    url = f'https://planetarycomputer.microsoft.com/api/sas/v1/token/{account_name}/{container_name}'
    response = requests.get(url)
    return response.json()

@fused.cache
def read_tiff_pc(bbox, tiff_url, cache_id=2): 
    tiff_url=f"{tiff_url}?{get_pc_token(tiff_url,_cache_id=cache_id)['token']}"
    print(tiff_url) 
    arr = fused.public.utils.read_tiff(bbox,tiff_url)
    return arr,tiff_url


@fused.cache(path="table_to_tile")
def table_to_tile(bbox, table="s3://fused-asset/imagery/naip/", min_zoom=12, centorid_zoom_offset=0, use_columns=['geometry'], clip=False):
    version="0.2.3"
    import fused    
    import pandas as pd
    try:
        x,y,z = bbox[['x','y','z']].iloc[0]
        print(x,y,z)
    except:
        z=min_zoom
    df = fused.utils.get_chunks_metadata(table)
    df = df[df.intersects(bbox.geometry[0])]
    if z>=min_zoom:#z>=12:
        List = df[['file_id','chunk_id']].values
        if use_columns:
            if 'geometry' not in use_columns: use_columns+=['geometry']
            rows_df = pd.concat([fused.utils.get_chunk_from_table(table, fc[0], fc[1], columns=use_columns) for fc in List])
        else:
            rows_df = pd.concat([fused.utils.get_chunk_from_table(table, fc[0], fc[1]) for fc in List])
            print('avaiable columns:', list(rows_df.columns))
        df = rows_df[rows_df.intersects(bbox.geometry[0])]
        df.crs=bbox.crs 
        if z<min_zoom+centorid_zoom_offset:# switch to centroid for the last one zoom level before showing metadata
            df.geometry=df.geometry.centroid
        if clip: return df.clip(bbox).explode()
        else: return df
    else:
        if clip: return df.clip(bbox).explode()
        else: return df

@fused.cache(path="geom_stats")
def geom_stats(gdf, arr,  chip_len=255):
    df_3857 = gdf.to_crs(3857)
    df_tile = df_3857.dissolve()
    minx, miny, maxx, maxy = df_tile.total_bounds
    d = (maxx-minx)/chip_len
    transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
    from fused.utils import rasterize_geometry
    geom_masks = [fused.utils.rasterize_geometry(geom, arr.shape[-2:], transform) 
                    for geom in df_3857.geometry]
    gdf['stats']=[arr[geom_mask].mean() for geom_mask in geom_masks]
    gdf['count']=[geom_mask.sum() for geom_mask in geom_masks]
    return gdf

def earth_session(cred):
    from rasterio.session import AWSSession
    from job2.credentials import get_session
    aws_session = get_session(cred['env'],
          earthdatalogin_username=cred['username'],
          earthdatalogin_password=cred['password'])
    return AWSSession(aws_session, requester_pays=False)

@fused.cache(path="read_tiff2")
def read_tiff(bbox, input_tiff_path, filter_list=None, output_shape=(256,256), overview_level=None, return_colormap=False, return_transform=False, cred=None):
    version="0.2.0"
    import os
    import rasterio
    import numpy as np
    from scipy.ndimage import zoom
    from contextlib import ExitStack
    if not cred:
        context = rasterio.Env()
    else:
        aws_session = earth_session(cred = cred)
        context = rasterio.Env(aws_session,
                      GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',
                      GDAL_HTTP_COOKIEFILE=os.path.expanduser('/tmp/cookies.txt'),
                      GDAL_HTTP_COOKIEJAR=os.path.expanduser('/tmp/cookies.txt'))  
    with ExitStack() as stack:
        stack.enter_context(context)
        with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:
        # with rasterio.Env():
            from rasterio.warp import reproject, Resampling
            bbox = bbox.to_crs(3857)
            # transform_bounds = rasterio.warp.transform_bounds(3857, src.crs, *bbox["geometry"].bounds.iloc[0])            
            window = src.window(*bbox.to_crs(src.crs).total_bounds)
            original_window = src.window(*bbox.to_crs(src.crs).total_bounds)
            gridded_window = rasterio.windows.round_window_to_full_blocks(original_window, [(1, 1)])
            window=gridded_window # Expand window to nearest full pixels
            source_data =src.read(window=window, boundless=True, masked=True)
            nodata_value = src.nodatavals[0]
            if filter_list:
                mask = np.isin(source_data, filter_list, invert=True)
                source_data[mask] = 0
            src_transform=src.window_transform(window)
            src_crs = src.crs
            minx, miny, maxx, maxy = bbox.total_bounds
            d = (maxx-minx)/output_shape[-1]
            dst_transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
            dst_shape = output_shape
            dst_crs = bbox.crs

            
            destination_data = np.zeros(dst_shape, src.dtypes[0])
            if return_colormap:
                colormap = src.colormap(1)
            reproject(
                source_data,
                destination_data,
                src_transform=src_transform,
                src_crs=src_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                #TODO: rather than nearest, get all the values and then get pct
                resampling=Resampling.nearest)
            destination_data = np.ma.masked_array(destination_data, destination_data==nodata_value)
    if return_colormap:
        #todo: only set transparency to zero
        colormap[0]=[0,0,0,0]
        return destination_data, colormap
    elif return_transform: #Note: usually you do not need this since it can be calculated using crs=4326 and bounds 
        return destination_data, dst_transform
    else:
        return destination_data

@fused.cache(path="mosaic_tiff")
def mosaic_tiff(bbox, tiff_list, reduce_function=None, filter_list=None, output_shape=(256,256), overview_level=None, cred=None):
    import numpy as np
    if not reduce_function:
        reduce_function = lambda x:np.max(x, axis=0)
    a=[]
    for input_tiff_path in tiff_list:
        if not input_tiff_path: continue 
        a.append(read_tiff(bbox=bbox, input_tiff_path=input_tiff_path, filter_list=filter_list, output_shape=output_shape, overview_level=overview_level, cred=cred))
    if len(a)==1:
        data=a[0]
    else:
        data = reduce_function(a)
    return data#.squeeze()[:-2,:-2]


def arr_resample(arr, dst_shape=(512,512), order=0):
        from scipy.ndimage import zoom
        import numpy as np
        zoom_factors= (np.array(dst_shape) / np.array(arr.shape[-2:]))
        if len(arr.shape)==2:
            return zoom(arr, zoom_factors, order=order)
        elif len(arr.shape)==3:
            return np.asanyarray([zoom(i, zoom_factors, order=order) for i in arr])
            
def arr_to_color(arr, colormap, out_dtype='uint8'):
    import numpy as np
    mapped_colors = np.array([colormap[val] for val in arr.flat])
    return mapped_colors.reshape(arr.shape[-2:]+(len(colormap[0]),)).astype(out_dtype).transpose(2,0,1)


def arr_to_plasma(data, min_max=(0,255), colormap='plasma', include_opacity=False, reverse=True): 
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
        #ref: https://matplotlib.org/stable/users/explain/colors/colormaps.html
        from matplotlib import colormaps
        if include_opacity:
            colormap=[(np.array([colormaps[colormap](i)[0],colormaps[colormap](i)[1],colormaps[colormap](i)[2],i])*255).astype('uint8') for i in range(257)]
            if reverse:
                colormap=colormap[::-1]
            mapped_colors = np.array([colormap[val] for val in norm_data255.flat])
            return mapped_colors.reshape(data.shape+(4,)).astype('uint8').transpose(2,0,1)
        else:
            colormap=[(np.array(colormaps[colormap](i)[:3])*255).astype('uint8') for i in range(256)]
            if reverse:
                colormap=colormap[::-1]
            mapped_colors = np.array([colormap[val] for val in norm_data255.flat])
            return mapped_colors.reshape(data.shape+(3,)).astype('uint8').transpose(2,0,1)
    else:
        return norm_data255

# #todo: use arr_to_color in arr_to_plasma
# def arr_to_plasma(data, min_max=(0,255)): 
#     import numpy as np 
#     data = data.astype(float)
#     if min_max:
#         norm_data = (data - min_max[0])/(min_max[1]-min_max[0])
#         norm_data = np.clip(norm_data,0,1)           
#     else:
#         print(f'min_max:({round(np.nanmin(data),3)},{round(np.nanmax(data),3)})')
#         norm_data = (data - np.nanmin(data))/(np.nanmax(data)-np.nanmin(data))            
#     norm_data255 = (norm_data*255).astype('uint8')
#     mapped_colors = np.array([plasma_colors[val] for val in norm_data255.flat])
#     return mapped_colors.reshape(data.shape+(3,)).astype('uint8').transpose(2,0,1)

# plasma_colors = [[ 12,   7, 135],
#        [ 16,   7, 136],
#        [ 19,   6, 137],
#        [ 22,   6, 138],
#        [ 24,   6, 140],
#        [ 27,   6, 141],
#        [ 29,   6, 142],
#        [ 31,   5, 143],
#        [ 33,   5, 144],
#        [ 35,   5, 145],
#        [ 38,   5, 146],
#        [ 40,   5, 146],
#        [ 42,   5, 147],
#        [ 43,   5, 148],
#        [ 45,   4, 149],
#        [ 47,   4, 150],
#        [ 49,   4, 151],
#        [ 51,   4, 151],
#        [ 53,   4, 152],
#        [ 54,   4, 153],
#        [ 56,   4, 154],
#        [ 58,   4, 154],
#        [ 60,   3, 155],
#        [ 61,   3, 156],
#        [ 63,   3, 156],
#        [ 65,   3, 157],
#        [ 66,   3, 158],
#        [ 68,   3, 158],
#        [ 70,   3, 159],
#        [ 71,   2, 160],
#        [ 73,   2, 160],
#        [ 75,   2, 161],
#        [ 76,   2, 161],
#        [ 78,   2, 162],
#        [ 80,   2, 162],
#        [ 81,   1, 163],
#        [ 83,   1, 163],
#        [ 84,   1, 164],
#        [ 86,   1, 164],
#        [ 88,   1, 165],
#        [ 89,   1, 165],
#        [ 91,   0, 165],
#        [ 92,   0, 166],
#        [ 94,   0, 166],
#        [ 95,   0, 166],
#        [ 97,   0, 167],
#        [ 99,   0, 167],
#        [100,   0, 167],
#        [102,   0, 167],
#        [103,   0, 168],
#        [105,   0, 168],
#        [106,   0, 168],
#        [108,   0, 168],
#        [110,   0, 168],
#        [111,   0, 168],
#        [113,   0, 168],
#        [114,   0, 169],
#        [116,   0, 169],
#        [117,   0, 169],
#        [119,   1, 168],
#        [120,   1, 168],
#        [122,   1, 168],
#        [123,   2, 168],
#        [125,   2, 168],
#        [126,   3, 168],
#        [128,   3, 168],
#        [129,   4, 167],
#        [131,   4, 167],
#        [132,   5, 167],
#        [134,   6, 167],
#        [135,   7, 166],
#        [136,   7, 166],
#        [138,   8, 166],
#        [139,   9, 165],
#        [141,  11, 165],
#        [142,  12, 164],
#        [144,  13, 164],
#        [145,  14, 163],
#        [146,  15, 163],
#        [148,  16, 162],
#        [149,  17, 161],
#        [150,  18, 161],
#        [152,  19, 160],
#        [153,  20, 160],
#        [155,  21, 159],
#        [156,  23, 158],
#        [157,  24, 157],
#        [158,  25, 157],
#        [160,  26, 156],
#        [161,  27, 155],
#        [162,  28, 154],
#        [164,  29, 154],
#        [165,  30, 153],
#        [166,  32, 152],
#        [167,  33, 151],
#        [169,  34, 150],
#        [170,  35, 149],
#        [171,  36, 149],
#        [172,  37, 148],
#        [173,  38, 147],
#        [175,  40, 146],
#        [176,  41, 145],
#        [177,  42, 144],
#        [178,  43, 143],
#        [179,  44, 142],
#        [180,  45, 141],
#        [181,  46, 140],
#        [183,  47, 139],
#        [184,  49, 138],
#        [185,  50, 137],
#        [186,  51, 137],
#        [187,  52, 136],
#        [188,  53, 135],
#        [189,  54, 134],
#        [190,  55, 133],
#        [191,  57, 132],
#        [192,  58, 131],
#        [193,  59, 130],
#        [194,  60, 129],
#        [195,  61, 128],
#        [196,  62, 127],
#        [197,  63, 126],
#        [198,  64, 125],
#        [199,  66, 124],
#        [200,  67, 123],
#        [201,  68, 122],
#        [202,  69, 122],
#        [203,  70, 121],
#        [204,  71, 120],
#        [205,  72, 119],
#        [206,  73, 118],
#        [207,  75, 117],
#        [208,  76, 116],
#        [208,  77, 115],
#        [209,  78, 114],
#        [210,  79, 113],
#        [211,  80, 112],
#        [212,  81, 112],
#        [213,  83, 111],
#        [214,  84, 110],
#        [215,  85, 109],
#        [215,  86, 108],
#        [216,  87, 107],
#        [217,  88, 106],
#        [218,  89, 105],
#        [219,  91, 105],
#        [220,  92, 104],
#        [220,  93, 103],
#        [221,  94, 102],
#        [222,  95, 101],
#        [223,  96, 100],
#        [224,  98,  99],
#        [224,  99,  98],
#        [225, 100,  98],
#        [226, 101,  97],
#        [227, 102,  96],
#        [227, 104,  95],
#        [228, 105,  94],
#        [229, 106,  93],
#        [230, 107,  92],
#        [230, 108,  92],
#        [231, 110,  91],
#        [232, 111,  90],
#        [232, 112,  89],
#        [233, 113,  88],
#        [234, 114,  87],
#        [235, 116,  86],
#        [235, 117,  86],
#        [236, 118,  85],
#        [237, 119,  84],
#        [237, 121,  83],
#        [238, 122,  82],
#        [238, 123,  81],
#        [239, 124,  80],
#        [240, 126,  80],
#        [240, 127,  79],
#        [241, 128,  78],
#        [241, 129,  77],
#        [242, 131,  76],
#        [242, 132,  75],
#        [243, 133,  74],
#        [244, 135,  73],
#        [244, 136,  73],
#        [245, 137,  72],
#        [245, 139,  71],
#        [246, 140,  70],
#        [246, 141,  69],
#        [247, 143,  68],
#        [247, 144,  67],
#        [247, 145,  67],
#        [248, 147,  66],
#        [248, 148,  65],
#        [249, 149,  64],
#        [249, 151,  63],
#        [249, 152,  62],
#        [250, 154,  61],
#        [250, 155,  60],
#        [251, 156,  60],
#        [251, 158,  59],
#        [251, 159,  58],
#        [251, 161,  57],
#        [252, 162,  56],
#        [252, 164,  55],
#        [252, 165,  54],
#        [252, 166,  54],
#        [253, 168,  53],
#        [253, 169,  52],
#        [253, 171,  51],
#        [253, 172,  50],
#        [253, 174,  49],
#        [254, 175,  49],
#        [254, 177,  48],
#        [254, 178,  47],
#        [254, 180,  46],
#        [254, 181,  46],
#        [254, 183,  45],
#        [254, 185,  44],
#        [254, 186,  43],
#        [254, 188,  43],
#        [254, 189,  42],
#        [254, 191,  41],
#        [254, 192,  41],
#        [254, 194,  40],
#        [254, 195,  40],
#        [254, 197,  39],
#        [254, 199,  39],
#        [253, 200,  38],
#        [253, 202,  38],
#        [253, 203,  37],
#        [253, 205,  37],
#        [253, 207,  37],
#        [252, 208,  36],
#        [252, 210,  36],
#        [252, 212,  36],
#        [251, 213,  36],
#        [251, 215,  36],
#        [251, 217,  36],
#        [250, 218,  36],
#        [250, 220,  36],
#        [249, 222,  36],
#        [249, 223,  36],
#        [248, 225,  37],
#        [248, 227,  37],
#        [247, 229,  37],
#        [247, 230,  37],
#        [246, 232,  38],
#        [246, 234,  38],
#        [245, 235,  38],
#        [244, 237,  39],
#        [244, 239,  39],
#        [243, 241,  39],
#        [242, 242,  38],
#        [242, 244,  38],
#        [241, 246,  37],
#        [241, 247,  36],
#        [240, 249,  33]]


def run_cmd(cmd, cwd='.', shell=False, communicate=False):
    import subprocess
    import shlex 
    if type(cmd)==str: cmd = shlex.split(cmd)
    proc=subprocess.Popen(cmd, shell=shell, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if communicate: 
        return proc.communicate()
    else: 
        return proc


# def run_cmd(cmd, cwd='.', shell=False):
#     import subprocess
#     import shlex
#     if type(cmd)==str: cmd = shlex.split(cmd)
#     return subprocess.Popen(cmd, shell=shell, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)



def import_env(env='testxenv', mnt_path='/mnt/cache/envs/', packages_path='/lib/python3.11/site-packages'):
    import sys
    sys.path.append(f'{mnt_path}{env}{packages_path}')

def install_module(name, env='testxenv', mnt_path='/mnt/cache/envs/', packages_path='/lib/python3.11/site-packages'):
    import_env(env, mnt_path, packages_path)
    import sys
    import os
    path=f'{mnt_path}{env}{packages_path}'
    sys.path.append(path)
    if not os.path.exists(path):
        run_cmd(f'python -m venv  {mnt_path}{env}', communicate=True) 
    return run_cmd(f'{mnt_path}{env}/bin/python -m pip install {name}', communicate=True)


def read_module(url, remove_strings=[]):
    import requests
    content_string = requests.get(url).text
    if len(remove_strings)>0:
        for i in remove_strings:
            content_string = content_string.replace(i,'')
    module = {}
    exec(content_string, module)
    return module
# url='https://raw.githubusercontent.com/stac-utils/stac-geoparquet/main/stac_geoparquet/stac_geoparquet.py'
# remove_strings=['from stac_geoparquet.utils import fix_empty_multipolygon']
# to_geodataframe = read_module(url,remove_strings)['to_geodataframe']
    
def download_file(url, destination):
    import requests
    try:
        response = requests.get(url)
        with open(destination, 'wb') as file:
            file.write(response.content)
        return f"File downloaded to '{destination}'."
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"

def import_utils(handle, udf_name, globals_var, module_list=None, verbose=False):
    import fused
    udf=fused.utils.import_udf(handle, udf_name)
    udf=fused.utils.import_udf('sina@fused.io','my_raster')
    List = udf.utils.keys()
    # List = list([*udf.utils])
    if module_list:
        List=module_list 
    if verbose: print('import:', List)
    for i in List:
        globals_var[i] = getattr(udf.utils, i)

def fs_list_hls(path='lp-prod-protected/HLSL30.020/HLS.L30.T10SEG.2023086T184554.v2.0/', 
                env="earthdata", earthdatalogin_username="", earthdatalogin_password=""):
        from job2.credentials import get_credentials
        import s3fs
        aws_session = get_credentials(env,
                              earthdatalogin_username=earthdatalogin_username,
                              earthdatalogin_password=earthdatalogin_password)
        fs = s3fs.S3FileSystem(key=aws_session['aws_access_key_id'], secret=aws_session['aws_secret_access_key'], token=aws_session['aws_session_token'])
        return fs.ls(path)

def cache_data(func, *args_hash, verbose=False, reset_cache=False, cache_path='tmp', 
               cache_function=True, base_path='/mnt/cache/sina/', retry=True, **kwargs_hash, ):
    class HashableLambda: 
        def __init__(self, lambda_function):
            self.lambda_function = lambda_function
    
        def __call__(self, *args, **kwargs):
            return self.lambda_function(*args, **kwargs)
    
        def __hash__(self):
            # Generate a hash based on the lambda's code
            return hash(self.lambda_function.__code__)
    from pathlib import Path
    cache_path = Path(base_path+cache_path)
    cache_path.mkdir(parents=True, exist_ok=True)
    cache_path = str(cache_path)
    try:
        import pickle
        import os
        import fused
        def strip_string(s, illegal_chars = " []%@()<>:/\\|?*"): 
                return ''.join(c for c in s if c not in illegal_chars)
        def hashify(x):
            import hashlib
            hash_object = hashlib.sha256()
            if callable(x):
                import base64
                func_bytes = base64.b64encode(func.__code__.co_code)
                hash_object.update(func_bytes)
                return hash_object.hexdigest()
            else:
                hash_object.update(str(x).encode())
                return hash_object.hexdigest()
        if cache_function:
            id=hashify(func) 
        else:
            id=''
        for v in args_hash:
            id+='_'+hashify(v)
        for k in kwargs_hash:
            id+=k+hashify(kwargs_hash[k])
        # id=strip_string(id)
        id=hashify(id)
        if verbose: print(id)
        # path = fused.utils.fused_path(f'cache{id}.pickle')
        # path=path.replace('tmp',cache_path)
        path = cache_path+'/data'+id
        if not os.path.exists(path) or reset_cache==True:            
            if verbose: print('first time loading...')
            with open(path, 'wb') as f:
                data=func()
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                return data
        else:
            if verbose: print(f'load from cache: {path}')
            try:
                with open(path, 'rb') as f:
                    data = pickle.load(f)
                    return data 
            except:    
                if retry:
                    # Try again
                    with open(path, 'wb') as f:
                        data=func()
                        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                        return data
                else:
                    return None
    except Exception as e:
        print(f'Error Caching {e}')
        return None

def run_async(fn, arr_args, delay=0, max_workers=32):
    import asyncio
    import nest_asyncio
    nest_asyncio.apply()
    import numpy as np
    import concurrent.futures
    loop = asyncio.get_event_loop()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    async def fn_async(pool, fn, *args):
        try:
            
                result = await loop.run_in_executor(pool, fn, *args)
                return result
        except OSError as error:
            print(f"Error: {error}")
            return None
   
    async def fn_async_exec(fn, arr, delay):
        tasks = []
        await asyncio.sleep(delay*np.random.random())
        if type(arr[0])==list or type(arr[0])==tuple:
            pass
        else:
            arr = [[i] for i in arr]
        for i in arr:
            tasks.append(fn_async(pool,fn,*i))
        return await asyncio.gather(*tasks)

    return loop.run_until_complete(fn_async_exec(fn, arr_args, delay))


def run_file(context, name, param_string=''):
    #todo: need to add option for png
    import requests
    from io import BytesIO
    import geopandas as gpd
    access_token=context.auth_token
    # access_token = fused.credentials.access_token()
    url = f'https://app-staging.fused.io/server/v1/realtime/fused-staging/api/v1/run/udf/saved/sina@fused.io/{name}?{param_string}dtype_out_vector=parquet&dtype_out_raster=png'
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    bytes_io = BytesIO(response.content)
    return gpd.read_parquet(bytes_io)
    
# def cache_data(func, *args_hash,  verbose=False, reset_cache=False, **kwargs_hash):
#     # todo: optioni: id based: give it id and things that you know it changes
#     # todo: option1: have a decorator on top of the function similar to streamlit
#     # todo: option2: pass it like async: pass funcion and kwargs 
#     # todo: option3: just directly put cache on the udf. you can not pass the parameters since it is lambda and it can not access to the global
#     import pickle
#     import os
#     import fused
#     import hashlib
#     import dis
#     # def strip_string(s, illegal_chars = " []%@()<>:/\\|?*"): 
#     #         return ''.join(c for c in s if c not in illegal_chars)

#     # class HashableLambda:
#     #     def __init__(self, lambda_function):
#     #         self.lambda_function = lambda_function
    
#     #     def __call__(self, *args, **kwargs):
#     #         return self.lambda_function(*args, **kwargs)
    
#     #     def __hash__(self):
#     #         # Generate a hash based on the lambda's code
#     #         return hash(self.lambda_function.__code__)
#     # hash(HashableLambda(lambda x:x))
# 
#     params = ''
#     for v in args_hash:
#         params+='_'+str(v)
#     for k in kwargs_hash:
#         params+=k+str(kwargs_hash[k])
#     # source_code = strip_string(inspect.getsource(func)+params)
#     # source_code = dis.Bytecode(func).dis()+params
#     source_code=''
#     source_code=f'co_code_{str(func.__code__.co_code)}' 
#     source_code+='co_names'+'_'.join(func.__code__.co_names)  
#     source_code+='co_freevars'+'_'.join(func.__code__.co_freevars)  
#     source_code+='co_varnames'+'_'.join(func.__code__.co_varnames)  
#     source_code+='co_cellvars'+'_'.join(func.__code__.co_cellvars)
#     source_code+='co_consts'+'_'.join([str(i) for i in func.__code__.co_consts]) 
#     # source_code+='co_qualname'+'_'.join([str(i) for i in func.__code__.co_qualname])     
#     # source_code+='co_names2_'.join([f'{i}_{globals_var[i]}' for i in func.__code__.co_freevars])
#     # source_code+='co_names_'.join([f'{i}_{func.__globals__[i]}' for i in func.__code__.co_names
#     #                               if (i in func.__globals__) & (type(func.__globals__[i])!=type(lambda:1))] )
#     # source_code+='co_freevars'.join([f'{i}_{func.__globals__[i]}' for i in func.__code__.co_freevars
#     #                               if (i in func.__globals__) & (type(func.__globals__[i])!=type(lambda:1))] )
#     print(source_code)
#     print(globals().keys())
#     hashed_value = hashlib.sha256(source_code.encode()).hexdigest()
#     if verbose: print('cached')
#     path = fused.utils.fused_path(f'{hashed_value}.pickle')
#     path=path.replace('tmp','mnt/cache/sina')
#     if not os.path.exists(path) and not reset_cache:
#         data=func()
#         if verbose: print('first time loading...')
#         with open(path, 'wb') as f:
#             pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
#     else:
#         if verbose: print(f'load from cache: {path}')
    
#         with open(path, 'rb') as f:
#                 data = pickle.load(f)
#     return data
    
# def xyz_tile_to_bbox(x, y, zoom):
#         import math
#         n = 2.0 ** zoom
#         lon1 = x / n * 360.0 - 180.0
#         lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
#         lat1 = math.degrees(lat_rad)
#         lon2 = (x + 1) / n * 360.0 - 180.0
#         lat2_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
#         lat2 = math.degrees(lat2_rad)
#         return (lon1, lat1, lon2, lat2)


# def read_tiff(bbox, input_tiff_path, output_shape=(256,256), overview_level=None, buffer_degree=0.000, resample_order=0):
#     import rasterio
#     import numpy as np
#     if buffer_degree != 0:
#         bbox.geometry=bbox.geometry.buffer(buffer_degree)
#     import rasterio
#     with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:            
#         bbox = bbox.to_crs(3857)
#         transform_bounds = rasterio.warp.transform_bounds(3857, src.crs, *bbox["geometry"].bounds.iloc[0])            
#         from rasterio.warp import reproject, Resampling
#         bbox_buffered = bbox.to_crs(src.crs)
#         buffer=0.25*(bbox_buffered.total_bounds[2] - bbox_buffered.total_bounds[0])
#         bbox_buffered = bbox_buffered.geometry.buffer(buffer)
#         window = src.window(*bbox_buffered.total_bounds)
#         source_data =src.read(window=window, boundless=True)
#         src_transform=src.window_transform(window)
#         minx, miny, maxx, maxy = bbox.total_bounds
#         d = (maxx-minx)/output_shape[-1]
#         dst_transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
#         with rasterio.Env():
#             src_crs = src.crs
#             dst_shape = output_shape
#             dst_crs = bbox.crs
#             destination_data = np.zeros(dst_shape, src.dtypes[0])
#             reproject(
#                 source_data,
#                 destination_data,
#                 src_transform=src_transform,
#                 src_crs=src_crs,
#                 dst_transform=dst_transform,
#                 dst_crs=dst_crs,
#                 resampling=Resampling.nearest)
#     arr = destination_data
#     return arr

# def mosaic_tiff(bbox, tiff_list, reduce_function=lambda x:np.max(x, axis=0), output_shape=(256,256), overview_level=None, buffer_degree=0.000, resample_order=0):
#     import numpy as np
#     a=[]
#     for input_tiff_path in tiff_list:
#         if not input_tiff_path: continue 
#         a.append(read_tiff(bbox, input_tiff_path, output_shape, overview_level, buffer_degree, resample_order))
#     if len(a)==1:
#         data=a[0]
#     else:
#         data = reduce_function(a)
#     return data
 


# #wrong one
# def read_tiff(bbox, input_tiff_path, output_shape=(255,255), overview_level=None):
#     import numpy as np
#     import rasterio
#     with rasterio.Env():
#         with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:            
#             bbox = bbox.to_crs(3857)
#             transform_bounds = rasterio.warp.transform_bounds(3857, src.crs, *bbox["geometry"].bounds.iloc[0])            
#             from rasterio.warp import reproject, Resampling
#             window = src.window(*bbox.to_crs(src.crs).total_bounds)
#             source_data =src.read(window=window, boundless=True)
#             src_transform=src.window_transform(window)
#             minx, miny, maxx, maxy = bbox.total_bounds
#             d = (maxx-minx)/output_shape[-1]
#             dst_transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
#             src_crs = src.crs
#             dst_shape = output_shape
#             dst_crs = bbox.crs
#             destination_data = np.zeros(dst_shape, src.dtypes[0])
#             reproject(
#                 source_data,
#                 destination_data,
#                 src_transform=src_transform,
#                 src_crs=src_crs,
#                 dst_transform=dst_transform,
#                 dst_crs=dst_crs,
#                 resampling=Resampling.nearest)
#     rgb = destination_data
#     return rgb

 
# import fused
# @fused.cache
# def mosaic_tiff(bbox, tiff_list, reduce_function=None, output_shape=(256,256), overview_level=None):
#     import numpy as np
#     if not reduce_function:
#         reduce_function = lambda x:np.mean(x, axis=0)
#     a=[]
#     for input_tiff_path in tiff_list:
#         if not input_tiff_path: continue 
#         a.append(read_tiff(bbox, input_tiff_path, output_shape, overview_level))
#     if len(a)==1:
#         data=a[0]
#     else:
#         data = reduce_function(a)
#     return data#.squeeze()[:-2,:-2]
    
# def read_tiff(bbox, input_tiff_path, output_shape=(255,255), overview_level=None):
#     import numpy as np
#     import rasterio
#     with rasterio.Env():
#         with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:  
#             from rasterio.warp import reproject, Resampling
#             bbox = bbox.to_crs(3857)
#             # transform_bounds = rasterio.warp.transform_bounds(3857, src.crs, *bbox["geometry"].bounds.iloc[0]) 
#             window = src.window(*bbox.to_crs(src.crs).total_bounds)
#             source_data =src.read(window=window, boundless=True)
#             src_transform=src.window_transform(window)
#             minx, miny, maxx, maxy = bbox.total_bounds
#             d = (maxx-minx)/output_shape[-1]
#             dst_transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
#             src_crs = src.crs
#             dst_shape = output_shape
#             dst_crs = bbox.crs
#             destination_data = np.zeros(dst_shape, src.dtypes[0])
#             reproject(
#                 source_data,
#                 destination_data,
#                 src_transform=src_transform,
#                 src_crs=src_crs,
#                 dst_transform=dst_transform,
#                 dst_crs=dst_crs,
#                 resampling=Resampling.nearest)
#     rgb = destination_data
#     return rgb


# def table_to_tile(bbox, table="s3://fused-asset/imagery/naip/", min_zoom=12, use_columns=['geometry']):
#     import fused    
#     import pandas as pd 
#     z = bbox['z'].values[0]
#     df = fused.utils.get_chunks_metadata(table)
#     df = df[df.intersects(bbox.geometry[0])]
#     if not min_zoom: 
#         min_zoom=z+1
#     if z>=min_zoom:#z>=12:
#         List = df[['file_id','chunk_id']].values
#         rows_df = pd.concat([fused.utils.get_chunk_from_table(table, fc[0], fc[1]) for fc in List])
#         print('Avaiable columns are:', list(rows_df.columns))
#         if use_columns:
#             if 'geometry' not in use_columns: use_columns+=['geometry']
#             return rows_df[rows_df.intersects(bbox.geometry[0])][use_columns]
#         else:
#             rows_df=rows_df.sample(1000)
#             return rows_df[rows_df.intersects(bbox.geometry[0])]
#     else: 
#         print(f'Please zoom more. cuurent zoom ({z=}) is less than {min_zoom=}')
#         return df     


##I tried this but then I got a very twisted resutls
# print(src_crs)
# dst_crs = 3857
# src_height, src_width = source_data.shape[-2:]
# dst_transform, dst_width, dst_height = rasterio.warp.calculate_default_transform(
#             src_crs, dst_crs, src_width, src_height,*bbox.to_crs(src.crs).total_bounds)
# dst_shape=(dst_height, dst_width)
# print(source_data.shape[-2:])
# print(dst_shape)

# print(dst_transform)


# def run_cmd(cmd, cwd='.', shell=False, communicate=False):
#     import subprocess
#     import shlex 
#     if type(cmd)==str: cmd = shlex.split(cmd)
#     proc=subprocess.Popen(cmd, shell=shell, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     if communicate: 
#         return proc.communicate()
#     else: 
#         return proc

# def install_module(name, env='testxenv', mnt_path='/mnt/cache/envs/', packages_path='/lib/python3.11/site-packages'):
#     import sys
#     import os
#     path=f'{mnt_path}{env}{packages_path}'
#     sys.path.append(path)
#     if not os.path.exists(path):
#         run_cmd(f'python -m venv  {mnt_path}{env}', communicate=True) 
#     return run_cmd(f'{mnt_path}{env}/bin/python -m pip install {name}', communicate=True)

