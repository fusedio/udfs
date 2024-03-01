# to use this, run the following command in your udf:
#common = fused.public.common
import fused
@fused.cache 
def acs_5yr_bbox(bounds, census_variable='population', type='bbox', suffix='simplify'):
    import json
    import shapely  
    import geopandas as gpd 
    if type == 'bbox':
        bounds = '{'+f'"west": {bounds[0]}, "north": {bounds[3]}, "east": {bounds[2]}, "south": {bounds[1]}'+'}'
    bounds=json.loads(bounds)
    box = shapely.box(bounds['west'], bounds['south'], bounds['east'], bounds['north'])  
    bbox = gpd.GeoDataFrame(geometry=[box], crs=4326)
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
    path = fused.core.download(url,name_prefix+url.split('/')[-1])
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
    arr = read_tiff(bbox,tiff_url)
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
    df = fused.get_chunks_metadata(table)
    df = df[df.intersects(bbox.geometry[0])]
    if z>=min_zoom:#z>=12:
        List = df[['file_id','chunk_id']].values
        if use_columns:
            if 'geometry' not in use_columns: use_columns+=['geometry']
            rows_df = pd.concat([fused.get_chunk_from_table(table, fc[0], fc[1], columns=use_columns) for fc in List])
        else:
            rows_df = pd.concat([fused.get_chunk_from_table(table, fc[0], fc[1]) for fc in List])
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


from typing import Dict, Tuple
from affine import Affine
from numpy.typing import NDArray
import numpy as np
def rasterize_geometry(
    geom: Dict, shape: Tuple[int, int], affine: Affine, all_touched: bool = False
) -> NDArray[np.uint8]:
    """Return an image array with input geometries burned in.

    Args:
        geom: GeoJSON geometry
        shape: desired 2-d shape
        affine: transform for the image
        all_touched: rasterization strategy. Defaults to False.

    Returns:
        numpy array with input geometries burned in.
    """
    from rasterio import features

    geoms = [(geom, 1)]
    rv_array = features.rasterize(
        geoms,
        out_shape=shape,
        transform=affine,
        fill=0,
        dtype="uint8",
        all_touched=all_touched,
    )

    return rv_array.astype(bool)


@fused.cache(path="geom_stats")
def geom_stats(gdf, arr,  chip_len=255):
    df_3857 = gdf.to_crs(3857)
    df_tile = df_3857.dissolve()
    minx, miny, maxx, maxy = df_tile.total_bounds
    d = (maxx-minx)/chip_len
    transform=[d, 0.0, minx, 0., -d, maxy, 0., 0., 1.]
    geom_masks = [rasterize_geometry(geom, arr.shape[-2:], transform) 
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

def run_cmd(cmd, cwd='.', shell=False, communicate=False):
    import subprocess
    import shlex 
    if type(cmd)==str: cmd = shlex.split(cmd)
    proc=subprocess.Popen(cmd, shell=shell, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if communicate: 
        return proc.communicate()
    else: 
        return proc

    
def download_file(url, destination):
    import requests
    try:
        response = requests.get(url)
        with open(destination, 'wb') as file:
            file.write(response.content)
        return f"File downloaded to '{destination}'."
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"


def fs_list_hls(path='lp-prod-protected/HLSL30.020/HLS.L30.T10SEG.2023086T184554.v2.0/', 
                env="earthdata", earthdatalogin_username="", earthdatalogin_password=""):
        from job2.credentials import get_credentials
        import s3fs
        aws_session = get_credentials(env,
                              earthdatalogin_username=earthdatalogin_username,
                              earthdatalogin_password=earthdatalogin_password)
        fs = s3fs.S3FileSystem(key=aws_session['aws_access_key_id'], secret=aws_session['aws_secret_access_key'], token=aws_session['aws_session_token'])
        return fs.ls(path)

#TODO combine run_udf_png & run_udf and use **kwarg
def run_udf(name, x, y ,z, context, dtype_out='geojson'):
        import requests
        import geopandas as gpd
        access_token=context.auth_token
        url = f"https://du0iv5kype.execute-api.us-west-2.amazonaws.com/api/v1/run/udf/saved/{name}/tiles/{z}/{x}/{y}?dtype_out={dtype_out}"
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(url, headers=headers)
        gdf = gpd.GeoDataFrame.from_features(response.json())
        return gdf

def run_udf_png(bbox, context, input_tiff_path, chip_len='256', requester_pays=0):
        x,y,z=bbox[['x','y','z']].iloc[0]
        import requests
        import rasterio
        from io import BytesIO
        url = f"https://du0iv5kype.execute-api.us-west-2.amazonaws.com/api/v1/run/udf/saved/sina@fused.io/tiff_tiler/tiles/{z}/{x}/{y}?dtype_out=png&chip_len={chip_len}&input_tiff_path={input_tiff_path}&requester_pays={requester_pays}"
        headers = {"Authorization": f"Bearer {context.auth_token}"}
        response = requests.get(url, headers=headers)
        with rasterio.open(BytesIO(response.content)) as dataset:
                data = dataset.read()
        return data


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

