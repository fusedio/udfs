# To use these functions, add the following command in your UDF:
# `common = fused.utils.common`
from __future__ import annotations
import fused
import pandas as pd
import numpy as np
import json
import os
from numpy.typing import NDArray
from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union, Any
from loguru import logger
from fused.api.api import AnyBaseUdf

import contextlib
@contextlib.contextmanager
def mutex(filename, wait=1, verbose=False):
    """Create a mutex lock using a file on disk.
    
    Args:
        filename: File path to use as mutex
        wait: Seconds to wait between lock attempts
        verbose: Whether to print status messages
    """
    import time
    import os
    while True:
        try:
            f = open(filename, 'x')
            break
        except OSError as e:
            if verbose:
                print(f"waiting {filename=} {e=}")
            time.sleep(wait)
    
    if verbose:
        print(f"acquired lock {filename=}")
    
    try:
        yield
    finally:
        if verbose:
            print(f"exited lock {filename=}")
        f.close()
        os.unlink(filename)


def jam_lock(lock_second=1, verbose=False):
    import time    
    current_second = int(time.time()) // (lock_second)
    lock_file = f'/tmp/fused_lock_{current_second}'
    with mutex(lock_file + '_lock', wait=0.5, verbose=verbose):
        if os.path.exists(lock_file):
            time.sleep(lock_second)
        else:
            open(lock_file, 'x').close()  

def get_catalog_url(udf):
    return f"http://{fused.options.base_url.split('/')[2]}/workbench/catalog/{udf.entrypoint}-{udf.metadata['fused:id']}"
    
def get_jobs_status(jobs, wait=True, sleep_seconds=3):
    from datetime import datetime
    s=datetime.now()
    import pandas as pd
    df = pd.DataFrame([i.job_id for i in jobs], columns=['job_id'])
    def get_status(job_id):
        return fused.api.job_get_status(job_id).status
    df['status'] = df.job_id.map(get_status)
    not_done=wait
    while not_done:
        import time
        time.sleep(sleep_seconds)
        df['status'] = df.apply(lambda row: 'terminated' if row.status in ['shutting-down','terminated'] else get_status(row.job_id) ,1)
        not_done=(df.status=='terminated').mean()<1
        print(f'\r{df.status.value_counts().to_dict()} | elapsed time: {datetime.now() - s}', end='', flush=True)
    return df
def get_s3_size(file_path):
    return sum([i.size for i in fused.api.list(file_path, details=1) if i.size])/10**9

def s3_tmp_path(path, folder="tmp/new", user_env="fused"):
    import re

    base_tmp_path = f"s3://fused-users/{user_env}/fused-tmp"
    fname = path.split("/")[-1]
    if '.' in fname:
        path = "/".join(path.split("/")[:-1])
    else:
        fname=''
    cleaned = re.sub(r"[^a-zA-Z0-9/]", "", path)  # remove non-alphanumeric except /
    cleaned = cleaned.replace("/", "")  # flatten path
    folder = folder.strip("/")
    parts = [base_tmp_path, folder]
    print(cleaned)
    if cleaned:
        parts.append(cleaned)
    s3_path = "/".join(parts) + "/"
    return s3_path + fname

def file_exists(path, verbose=True):
    """
    Check if a file exists based on the path type:
    - Local file system
    - S3 path (starting with 's3:')
    - HTTP/HTTPS URL
    
    Args:
        path: Path to check
        verbose: Whether to print status messages
    
    Returns:
        bool: True if file exists, False otherwise
    """
    import os
    
    # Handle S3 paths
    if path.startswith('s3:'):
        try:
            import s3fs
            fs = s3fs.S3FileSystem()
            exists = fs.exists(path)
            if verbose and exists:
                print(f'{path=} exists on S3.')
            return exists
        except ImportError:
            if verbose:
                print("s3fs package not installed. Please install with 'pip install s3fs'")
            return False
    
    # Handle HTTP/HTTPS URLs
    elif path.startswith(('http://', 'https://')):
        try:
            import requests
            response = requests.head(path, allow_redirects=True)
            exists = response.status_code == 200
            if verbose and exists:
                print(f'{path=} exists with status code 200.')
            return exists
        except Exception as e:
            if verbose:
                print(f"Error checking URL {path}: {str(e)}")
            return False
    
    # Handle local files
    else:
        exists = os.path.exists(path)
        if verbose and exists:
            print(f'{path=} exists locally.')
        return exists

def encode_metadata_fused(fused_metadata: pd.DataFrame) -> bytes:
    import base64
    from io import BytesIO
    with BytesIO() as bio:
        fused_metadata.to_parquet(bio, index=False)
        fused_metadata_bytes = bio.getvalue()

    return base64.b64encode(fused_metadata_bytes)

def create_sample_file(df_meta, file_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    fused_metadata_b64 = encode_metadata_fused(df_meta)

    table = pa.table({})
    meta = {}
    meta[b"fused:format_version"] = b"5"
    meta[b"fused:_metadata"] = fused_metadata_b64
    pq.write_table(
        table.replace_schema_metadata(meta),
        file_path,
    )
    return f"{file_path} written!"

def unzip_file(zip_path_str):
    import zipfile
    from pathlib import Path
    zip_path = Path(zip_path_str)
    extract_dir = zip_path.parent
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        extracted_files = zip_ref.namelist()

    print(f"Extracted files: {extract_dir}")
    for f in extracted_files:
        full_path = extract_dir / f
        if full_path.exists():
            stat = full_path.stat()
            size_kb = stat.st_size / 1024
            print(f"{full_path.name:50} {size_kb:10.1f} KB")
        else:
            print(f"{full_path.name:50} <not found>")


@fused.cache
def get_crs(path):
    import rasterio
    with rasterio.open(path) as src:
        return src.crs

@fused.cache(cache_max_age='30d')
def bounds_to_file_chunk(bounds:list=[-180, -90, 180, 90], target_num_files: int = 64, target_num_file_chunks: int = 64):
    import pandas as pd

    df = get_tiles(bounds, target_num_tiles=target_num_files)
    all_tiles = []
    for idx, row in df.iterrows():
        sub_tiles = get_tiles(row["geometry"].bounds, target_num_tiles=target_num_file_chunks)
        sub_tiles["file_id"] = idx
        sub_tiles["chunk_id"] = range(len(sub_tiles))
        sub_tiles["bbox_minx"] = sub_tiles["geometry"].bounds.minx
        sub_tiles["bbox_miny"] = sub_tiles["geometry"].bounds.miny
        sub_tiles["bbox_maxx"] = sub_tiles["geometry"].bounds.maxx
        sub_tiles["bbox_maxy"] = sub_tiles["geometry"].bounds.maxy
        all_tiles.append(sub_tiles)
    df = pd.concat(all_tiles)
    # print(df.chunk_id.value_counts())
    return df

def bounds_to_hex(bounds: list = [-180, -90, 180, 90], res: int = 3, hex_col: str = "hex"):
    bbox = get_tiles(bounds, 4)
    bbox.geometry=bbox.buffer((bounds[2]-bounds[0])/20)
    df = bbox.to_wkt()
    qr = f""" with t as (
        SELECT unnest(h3_polygon_wkt_to_cells_experimental(geometry, 'center' , {res})) AS {hex_col}
        FROM df)
        select *,  from t
        where h3_cell_to_lng({hex_col}) between {bounds[0]} and {bounds[2]}
        and h3_cell_to_lat({hex_col}) between {bounds[1]} and {bounds[3]}
        group by 1
        """
    con = duckdb_connect()
    df = con.sql(qr).df()
    return df

def gdf_to_hex(gdf, res=11, add_latlng_cols=['lat','lng']):
    import pandas as pd
    con = duckdb_connect()
    df_wkt = gdf.to_wkt()
    if add_latlng_cols:
        df_wkt[add_latlng_cols[0]] = gdf.centroid.y
        df_wkt[add_latlng_cols[1]] = gdf.centroid.x
    df_wkt['fused_index']=range(len(df_wkt))
    df_hex1 = con.sql(f'''
        with t as(
            SELECT 
                * exclude(geometry), 
                h3_polygon_wkt_to_cells_experimental(geometry, 'center', {res}) AS hex 
            FROM df_wkt)
        SELECT 
            * exclude(hex), 
            unnest(hex) AS hex, 
            len(hex) AS cell_count 
        FROM t
        '''
    ).df()
    
    df_wkt2 = df_wkt[~df_wkt.fused_index.isin(df_hex1.fused_index.unique())]
    df_hex2 = con.sql(f'''
            SELECT * exclude(geometry), 
            h3_latlng_to_cell(ST_Y(ST_Centroid(ST_GeomFromText(geometry))), ST_X(ST_Centroid(ST_GeomFromText(geometry))), {res}) AS hex,
            1 AS cell_count
            from df_wkt2
            '''
        ).df()
    df_hex=pd.concat([df_hex1,df_hex2])
    df_hex = df_hex.sort_values('fused_index').drop('fused_index', axis=1).reset_index(drop=True)
    return df_hex

def filter_hex_bounds(df_hex, bounds=[-180, -90, 180, 90], col_hex='hex'):
    con = duckdb_connect()
    df = con.sql(f'''
        SELECT 
            * 
        FROM df_hex
            where h3_cell_to_lat({col_hex}) between {bounds[1]} and {bounds[3]}
            and h3_cell_to_lng({col_hex}) between {bounds[0]} and {bounds[2]}
        '''
    ).df()
    return df

def to_pickle(obj):
    """Encode an object to a pickle byte stream and store in DataFrame."""
    import pickle
    import pandas as pd
    return pd.DataFrame(
        {"data_type": [type(obj).__name__], "data_content": [pickle.dumps(obj)]}
    )

def from_pickle(df):
    """Decode an object from a DataFrame containing pickle byte stream."""
    import pickle
    return pickle.loads(df["data_content"].iloc[0])

def df_summary(df, description="", n_head=5, n_tail=5, n_sample=5, n_unique=100, add_details=True):
    val = description+"\n\n"
    val += "These are stats for df (pd.DataFrame):\n"
    val += f"{list(df.columns)=} \n\n"
    val += f"{df.isnull().sum()=} \n\n"
    val += f"{df.describe().to_json()=} \n\n"
    val += f"{df.head(n_head).to_json()=} \n\n"
    val += f"{df.tail(n_tail).to_json()=} \n\n"
    if len(df) > n_sample:
        val += f"{df.sample(n_sample).to_json()=} \n\n"
    if add_details:
        if len(df) <= n_unique:
            val += f"{df.to_json()} \n\n"
        else:
            for c in df.columns:
                value_counts = df[c].value_counts()
                df[c].value_counts().head()
                val += f"df[{c}].value_counts()\n{value_counts} \n\n"
                val += f"{df[c].unique()[:n_unique]} \n\n"
    return val

def get_diff_text(text1: str, text2: str, as_html: bool=True, only_diff: bool=False) -> str:
    import difflib
    import html
    
    diff = difflib.ndiff(text1.splitlines(keepends=True), text2.splitlines(keepends=True))
    processed_diff = []
    
    if not as_html:
        for line in diff:
            if line.startswith("+"):
                processed_diff.append(f"ADD: {line}")  # Additions
            elif line.startswith("-"):
                processed_diff.append(f"DEL: {line}")  # Deletions
            else:
                if not only_diff:
                    processed_diff.append(f"  {line}")  # Unchanged lines
        return "\n".join(processed_diff)            
    
    for line in diff:
        escaped_line = html.escape(line)  # Escape HTML to preserve special characters
        
        if line.startswith("+"):
            processed_diff.append(f"<span style='color:green; line-height:normal;'> {escaped_line} </span><br>")  # Green for additions
        elif line.startswith("-"):
            processed_diff.append(f"<span style='color:red; line-height:normal;'> {escaped_line} </span><br>")  # Red for deletions
        else:
            if not only_diff:
                processed_diff.append(f"<span style='color:gray; line-height:normal;'> {escaped_line} </span><br>")  # Gray for unchanged lines
    
    # HTML structure with a dropdown for selecting background color
    html_output = """
    <div>
        <label for="backgroundColor" style="color:gray;">Choose Background Color: </label>
        <select id="backgroundColor" onchange="document.getElementById('diff-container').style.backgroundColor = this.value;">
            <option value="#111111">Dark Gray</option>
            <option value="#f0f0f0">Light Gray</option>
            <option value="#ffffff">White</option>
            <option value="#e0f7fa">Cyan</option>
            <option value="#ffebee">Pink</option>
            <option value="#c8e6c9">Green</option>
        </select>
    </div>
    <div id="diff-container" style="background-color:#111111; padding:10px; font-family:monospace; white-space:pre; line-height:normal;">
        {}</div>
    """.format("".join(processed_diff))
    return html_output


def json_path_from_secret(var='gcs_fused'):
    import json
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp_file:
        json.dump(json.loads(fused.secrets[var]), tmp_file)
        tmp_file_path = tmp_file.name
    return tmp_file_path
    
def gcs_credentials_from_secret(var='gcs_fused'):
    """
    example: 
    """
    import gcsfs
    import json
    from google.oauth2 import service_account
    
    gcs_secret = json.loads(fused.secrets[var])
    credentials = service_account.Credentials.from_service_account_info(gcs_secret)
    return credentials

@fused.cache
def simplify_gdf(gdf, pct=1, args='-o force -clean'):
    #ref https://github.com/mbloch/mapshaper/blob/master/REFERENCE.md
    import geopandas as gpd
    import uuid
    import json
    file_path = f'/mount/cache_data/temp/{uuid.uuid4()}.json'
    print(file_path)
    with open(file_path, "w") as f:
        json.dump(gdf.__geo_interface__, f)
    # print(fused.utils.common.run_cmd('npm install -g mapshaper',communicate=True)[1].decode())    
    print(run_cmd(f'/mount/npm/bin/mapshaper {file_path} -simplify {pct}% {args} -o {file_path.replace(".json","2.json")}',communicate=True))
    return gpd.read_file(file_path.replace(".json","2.json"))

def html_to_obj(html_str):
    from fastapi import Response
    return Response(html_str.encode('utf-8'), media_type="text/html")

def pydeck_to_obj(map, as_string=False):    
        html_str = map.to_html(as_string=True)
        if as_string:
            return html_str
        else: 
            return html_to_obj(html_str)

def altair_to_obj(chart, as_string=False):    
    import io
    html_buffer = io.StringIO()
    chart.save(html_buffer, format="html")
    html_str = html_buffer.getvalue()
    html_buffer.close()
    if as_string:
        return html_str
    else: 
        return html_to_obj(html_str)

def html_params(html_template, params={}, **kw):
    '''Exampl: html_params('<div>{{v1}}{{v2}}</div>',{'v1':'hello '}, v2='world!')'''
    from jinja2 import Template
    template = Template(html_template)
    return template.render(params, **kw)


def url_redirect(url):
    from fastapi import Response
    return Response(f'<meta http-equiv="refresh" content="0; url={url}">'.encode('utf-8'), media_type="text/html")
    
@fused.cache
def read_shapefile(url):
    import geopandas as gpd
    try:
        return gpd.read_file(url)
    except:
        path = fused.download(url, url)
        name=str(path).split('/')[-1].split('.')[0]
        print(name)
        try:
            return gpd.read_file(f"zip://{path}!{name}/{name}.shp")
        except:
            return gpd.read_file(f"zip://{path}!{name}.shp")

def point_to_line(start_lon=-122.4194, start_lat=37.7749, end_lon=-122.2712, end_lat=37.8044, value=0):
    import geopandas as gpd
    import shapely
    from shapely.geometry import LineString    
    line = LineString([(start_lon, start_lat), (end_lon, end_lat)])    
    return gpd.GeoDataFrame({"value": [value]}, geometry=[line], crs=4326)

def interpolate_points(line_shape, point_distance):
    points = []
    num_points = int(line_shape.length // point_distance)
    for i in range(num_points + 1):
        point = line_shape.interpolate(i * point_distance)
        points.append(point)
    return points

def pointify(lines, point_distance, segment_col='segment_id'):
    import geopandas as gpd
    crs_orig = lines.crs
    crs_utm=lines.estimate_utm_crs()
    lines = lines.to_crs(crs_utm)
    from shapely.geometry import Point
    import pandas as pd
    points_list = []
    for _, row in lines.iterrows():
        line = row['geometry']
        points = interpolate_points(line, point_distance)
        for point in points:
            points_list.append({segment_col: row[segment_col], 'geometry': point})
    print('number of points:', len(points_list))
    points_gdf = gpd.GeoDataFrame(points_list, crs=lines.crs)
    return points_gdf.to_crs(crs_orig)

def chunkify(lst, chunk_size=None, n_chunks=None):
    if (chunk_size is not None) and (n_chunks is not None):
        raise ValueError("Specify only one of chunk_size or n_chunks, not both.")
    if chunk_size is not None:
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    if n_chunks is not None:
        k, m = divmod(len(lst), n_chunks)
        return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n_chunks)]
    raise ValueError("You must provide either chunk_size or n_chunks.")


@fused.cache
def get_meta_chunk_datestr(base_path, total_row_groups=52, start_year=2020, end_year=2024, n_chunks_row_group=2, n_chunks_datestr=90,):
    import pandas as pd
    date_list = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year+1}-01-01').strftime('%Y-%m-%d').tolist()[:-1]
    df = pd.DataFrame([[i[0],i[-1]] for i in chunkify(date_list,n_chunks_datestr)], columns=['start_datestr','end_datestr'])
    df['row_group_ids']=[chunkify(list(range(total_row_groups)),n_chunks_row_group)]*len(df)
    df = df.explode('row_group_ids').reset_index(drop=True)
    df['path'] = df.apply(lambda row:f"{base_path.strip('/')}/file_{row.start_datestr.replace('-','')}_{row.end_datestr.replace('-','')}_{row.row_group_ids[0]}_{row.row_group_ids[-1]}.parquet", axis=1)
    df['idx'] = df.index
    df['row_group'] = df['row_group_ids']
    df = df.explode('row_group').drop_duplicates('idx').sort_values('row_group')
    del df['row_group']
    return df

def write_log(msg="Your message.", name='//default', log_type='info', rotation="10 MB"):
    from loguru import logger
    path = fused.file_path('logs/' + name + '.log')
    logger.add(path, rotation=rotation)
    if log_type=='warning':
        logger.warning(msg)
    else:
        logger.info(msg)  # Write the log message
    logger.remove()  # Remove the log handler
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} | {path} |msg: {msg}" )

def read_log(n=None, name='default', return_log=False):
    path = fused.file_path('logs/' + name + '.log')
    try:
        with open(path, 'r') as file:
            log_content = file.readlines()
            if n:
                log_content = ''.join(log_content[-n:])  # Return last 'tail_lines' entries
            else:
                log_content = ''.join(log_content)
            if return_log:
                return log_content
            else:
                print(log_content)
    except FileNotFoundError:
        if return_log:
            return "Log file not found."
        else:
            print("Log file not found.")

def dilate_bbox(bounds, chip_len, border_pixel):
    bbox_crs = bounds.crs
    clipped_chip=chip_len-(border_pixel*2)
    bounds = bounds.to_crs(bounds.estimate_utm_crs())
    length = bounds.area[0]**0.5
    buffer_ratio = (chip_len-clipped_chip)/clipped_chip
    buffer_distance=length*buffer_ratio/2
    bounds.geometry = bounds.buffer(buffer_distance)
    bounds = bounds.to_crs(bbox_crs)
    return bounds

def read_gdf_file(path):
    import geopandas as gpd

    extension = path.rsplit(".", maxsplit=1)[-1].lower()
    if extension in ["gpkg", "shp", "geojson"]:
        driver = (
            "GPKG"
            if extension == "gpkg"
            else ("ESRI Shapefile" if extension == "shp" else "GeoJSON")
        )
        return gpd.read_file(path, driver=driver)
    elif extension == "zip":
        return gpd.read_file(f"zip+{path}")
    elif extension in ["parquet", "pq"]:
        return gpd.read_parquet(path)


def url_to_arr(url, return_colormap=False):
    from io import BytesIO

    import rasterio
    import requests
    from rasterio.plot import show

    response = requests.get(url)
    print(response.status_code)
    with rasterio.open(BytesIO(response.content)) as dataset:
        if return_colormap:
            colormap = dataset.colormap
            return dataset.read(), dataset.colormap(1)
        else:
            return dataset.read()


def read_shape_zip(url, file_index=0, name_prefix=""):
    """This function opens any zipped shapefile"""
    import zipfile

    import geopandas as gpd

    path = fused.core.download(url, name_prefix + url.split("/")[-1])
    fnames = [
        i.filename for i in zipfile.ZipFile(path).filelist if i.filename[-4:] == ".shp"
    ]
    df = gpd.read_file(f"{path}!{fnames[file_index]}")
    return df

@fused.cache
def stac_to_gdf(bounds, datetime='2024', collections=["sentinel-2-l2a"], columns=['id', 'geometry', 'bounds', 'assets', 'datetime', 'eo:cloud_cover'], query={"eo:cloud_cover": {"lt": 20}}, catalog='mspc', explode_assets=False, version=0):
    import pystac_client
    import stac_geoparquet
    if catalog.lower()=='aws':
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif catalog.lower()=='mspc':
        import planetary_computer
        # catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else: 
        catalog = pystac_client.Client.open(catalog)
    items = catalog.search(
        collections=collections,
        bbox=bounds.total_bounds,
        datetime=datetime,
        query=query,
        ).item_collection()
    gdf=stac_geoparquet.to_geodataframe([item.to_dict() for item in items])
    if explode_assets:
        gdf['assets'] = gdf.assets.map(lambda x: [{k:x[k]['href']} for k in x]) 
        gdf = gdf.explode('assets')
        gdf['band'] = gdf.assets.map(lambda x: list(x.keys())[0]) 
        gdf['url'] = gdf.assets.map(lambda x: list(x.values())[0]) 
        del gdf['assets']
        if columns:
            columns+=['band','url']
    if columns==None:
        print(gdf.columns)
        return gdf
    else:
        gdf=gdf[list(set(columns).intersection(set(gdf.columns)))]
        print(gdf.columns)
        return gdf

@fused.cache
def stac_to_gdf_maxar(event_name, max_items=1000):
    import geopandas as gpd
    import requests
    from shapely.geometry import shape, box
    
    base_url = "https://maxar-opendata.s3.amazonaws.com/events/"
    
    def resolve_url(base_url, relative_url):
        if relative_url.startswith("../"):
            base_dir = "/".join(base_url.split("/")[:-1])
            parts = relative_url.split("/")
            up_levels = len([p for p in parts if p == ".."])
            base_parts = base_dir.split("/")
            base_path = "/".join(base_parts[:-up_levels])
            item_path = "/".join(parts[up_levels:])
            return f"{base_path}/{item_path}"
        elif relative_url.startswith("./"):
            base_dir = "/".join(base_url.split("/")[:-1])
            return f"{base_dir}/{relative_url[2:]}"
        return relative_url
    
    event_collection_url = f"{base_url}{event_name}/collection.json"
    response = requests.get(event_collection_url)
    if response.status_code != 200:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    
    event_collection = response.json()
    acq_collection_links = []
    for link in event_collection.get("links", []):
        if link.get("rel") == "child" and "acquisition_collections" in link.get("href", ""):
            href = link.get("href")
            if href.startswith("./"):
                href = f"{base_url}{event_name}/{href[2:]}"
            acq_collection_links.append(href)
    
    all_items = []
    item_count = 0
    
    for acq_url in acq_collection_links:
        if item_count >= max_items:
            break
        
        acq_response = requests.get(acq_url)
        if acq_response.status_code != 200:
            continue
        
        acq_collection = acq_response.json()
        item_links = [link.get("href") for link in acq_collection.get("links", []) 
                     if link.get("rel") == "item"]
        
        for item_link in item_links:
            if item_count >= max_items:
                break
            
            item_url = resolve_url(acq_url, item_link)
            
            try:
                item_response = requests.get(item_url)
                if item_response.status_code != 200:
                    continue
                
                item = item_response.json()
                
                geom = None
                if "geometry" in item and item["geometry"]:
                    geom = shape(item["geometry"])
                elif "bbox" in item and item["bbox"]:
                    geom = box(*item["bbox"])
                
                if geom:
                    props = item.get("properties", {})
                    item_data = {
                        "product_name": item.get("id", "Unknown"),
                        "collection": event_name,
                        "datetime": props.get("datetime", "Unknown"),
                        "platform": props.get("platform", "Unknown"),
                        "gsd": props.get("gsd", "Unknown"),
                        "catalog_id": props.get("catalog_id", "Unknown"),
                        "off_nadir": props.get("view:off_nadir", "Unknown"),
                        "azimuth": props.get("view:azimuth", "Unknown"),
                        "incidence_angle": props.get("view:incidence_angle", "Unknown"),
                        "sun_azimuth": props.get("view:sun_azimuth", "Unknown"),
                        "sun_elevation": props.get("view:sun_elevation", "Unknown"),
                        "utm_zone": props.get("utm_zone", "Unknown"),
                        "epsg": props.get("proj:epsg", "Unknown"),
                        "quadkey": props.get("quadkey", "Unknown"),
                        "cloud_percent": props.get("tile:clouds_percent", 0),
                        "data_area": props.get("tile:data_area", 0),
                        "item_url": item_url
                    }
                    
                    # Process all available assets
                    assets = item.get("assets", {})
                    asset_types = list(assets.keys())
                    item_data["asset_types"] = ",".join(asset_types)
                    
                    # Store URLs for all assets
                    for asset_key, asset_info in assets.items():
                        asset_href = asset_info.get("href", "")
                        if asset_href:
                            resolved_url = resolve_url(item_url, asset_href)
                            item_data[f"{asset_key}_url"] = resolved_url
                    
                    all_items.append((geom, item_data))
                    item_count += 1
            except Exception:
                continue
    
    if all_items:
        geometries = [item[0] for item in all_items]
        properties = [item[1] for item in all_items]
        gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs="EPSG:4326")        
        return gdf
    else:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

@fused.cache
def get_url_aws_stac(bounds, collections=["cop-dem-glo-30"]):
    import pystac_client
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    items = catalog.search(
        collections=collections,
        bbox=bounds.total_bounds,
    ).item_collection()
    url_list=[i['assets']['data']['href'] for i in items.to_dict()['features']]
    return url_list
        
def get_collection_bbox(collection):
    import geopandas as gpd
    import planetary_computer
    import pystac_client

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    asset = catalog.get_collection(collection).assets["geoparquet-items"]
    df = gpd.read_parquet(
        asset.href, storage_options=asset.extra_fields["table:storage_options"]
    )
    return df[["assets", "datetime", "geometry"]]


def get_pc_token(url):
    from urllib.parse import urlparse

    import requests

    parsed_url = urlparse(url.rstrip("/"))
    account_name = parsed_url.netloc.split(".")[0]
    path_blob = parsed_url.path.lstrip("/").split("/", 1)
    container_name = path_blob[-2]
    url = f"https://planetarycomputer.microsoft.com/api/sas/v1/token/{account_name}/{container_name}"
    response = requests.get(url)
    return response.json()

@fused.cache(path="table_to_tile")
def table_to_tile(
    bounds: fused.types.Bounds,
    table="s3://fused-asset/imagery/naip/",
    min_zoom=12,
    centorid_zoom_offset=0,
    use_columns=["geometry"],
    clip=False,
    print_xyz=False,
):
    import fused
    import geopandas as gpd
    import pandas as pd

    version = "0.2.3"
    tile = get_tiles(bounds)

    try:
        x, y, z = tile[["x", "y", "z"]].iloc[0]
        if print_xyz:
            print(x, y, z)
    except:
        z = min_zoom
    df = fused.get_chunks_metadata(table)
    if isinstance(tile, (list, tuple, np.ndarray)):
        tile=to_gdf(tile)
    elif len(tile) > 1:
        tile = tile.dissolve().reset_index(drop=True)
    else:
        tile = tile.reset_index(drop=True)
    df = df[df.intersects(tile.geometry[0])]
    if z >= min_zoom:
        List = df[["file_id", "chunk_id"]].values
        if not len(List):
            # No result at this area
            return gpd.GeoDataFrame(geometry=[])
        if use_columns:
            if "geometry" not in use_columns:
                use_columns += ["geometry"]
            rows_df = pd.concat(
                [
                    fused.get_chunk_from_table(table, fc[0], fc[1], columns=use_columns)
                    for fc in List
                ]
            )
        else:
            rows_df = pd.concat(
                [fused.get_chunk_from_table(table, fc[0], fc[1]) for fc in List]
            )
            print("available columns:", list(rows_df.columns))
        try:
            df = rows_df[rows_df.intersects(tile.geometry[0])]
        except:
            df = rows_df
        df.crs = tile.crs
        if (
            z < min_zoom + centorid_zoom_offset
        ):  # switch to centroid for the last one zoom level before showing metadata
            df.geometry = df.geometry.centroid
        if clip:
            return df.clip(tile).explode()
        else:
            return df
    else:
        df.crs = tile.crs
        if clip:
            return df.clip(tile).explode()
        else:
            return df


def rasterize_geometry(
    geom: Dict, shape: Tuple[int, int], affine, all_touched: bool = False
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
def geom_stats(gdf, arr, output_shape=(255, 255)):
    import numpy as np

    df_3857 = gdf.to_crs(3857)
    df_tile = df_3857.dissolve()
    minx, miny, maxx, maxy = df_tile.total_bounds
    dx = (maxx - minx) / output_shape[-1]
    dy = (maxy - miny) / output_shape[-2]
    transform = [dx, 0.0, minx, 0.0, -dy, maxy, 0.0, 0.0, 1.0]
    geom_masks = [
        rasterize_geometry(geom, arr.shape[-2:], transform) for geom in df_3857.geometry
    ]
    if isinstance(arr, np.ma.MaskedArray):
        arr = arr.data
    gdf["stats"] = [np.nanmean(arr[geom_mask]) for geom_mask in geom_masks]
    gdf["count"] = [geom_mask.sum() for geom_mask in geom_masks]
    return gdf


def earth_session(cred):
    from job2.credentials import get_session
    from rasterio.session import AWSSession

    aws_session = get_session(
        cred["env"],
        earthdatalogin_username=cred["username"],
        earthdatalogin_password=cred["password"],
    )
    return AWSSession(aws_session, requester_pays=False)


@fused.cache(path="read_tiff")
def read_tiff(
    bounds,
    input_tiff_path,
    filter_list=None,
    output_shape=(256, 256),
    overview_level=None,
    return_colormap=False,
    return_transform=False,
    return_crs=False,
    return_bounds=False,
    return_meta=False,
    cred=None,
    resampling = 'nearest'
):
    import os
    from contextlib import ExitStack

    import numpy as np
    import rasterio
    from rasterio.coords import BoundingBox, disjoint_bounds
    from rasterio.warp import Resampling, reproject
    from scipy.ndimage import zoom
        
    version = "0.2.0"

    
    if isinstance(resampling, str):
        resampling = getattr(Resampling, resampling.lower())

    if not cred:
        context = rasterio.Env()
    else:
        aws_session = earth_session(cred=cred)
        context = rasterio.Env(
            aws_session,
            GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
            GDAL_HTTP_COOKIEFILE=os.path.expanduser("/tmp/cookies.txt"),
            GDAL_HTTP_COOKIEJAR=os.path.expanduser("/tmp/cookies.txt"),
        )
    with ExitStack() as stack:
        stack.enter_context(context)
        try:
            with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:
                # with rasterio.Env():
                if src.crs:
                    src_crs = src.crs
                else:
                    src_crs=4326
                src_bbox = bounds.to_crs(src_crs)

                if disjoint_bounds(src.bounds, BoundingBox(*src_bbox.total_bounds)):
                    return None

                window = src.window(*src_bbox.total_bounds)

                factor = 1
                if output_shape is not None:
                    # determine a factor to downsample based on the overviews of the first band
                    for f in src.overviews(1):
                        if (window.height / f) < output_shape[0]:
                            break
                        else:
                            factor = f

                    n_pixels_window = window.height * window.width / (factor ** 2)
                    if n_pixels_window > (output_shape[-2] * output_shape[-1] * 4):
                        # if we would still be reading too much data with the current
                        # window and factor (cutoff at 4x the desired output size)
                        # -> increase factor to avoid memory blowup
                        new_factor =  min(
                            window.height / (output_shape[-2]*2),
                            window.width / (output_shape[-1]*2)
                        )
                        factor = int(new_factor // factor) * factor

                # # transform_bounds = rasterio.warp.transform_bounds(3857, src_crs, *bounds["geometry"].bounds.iloc[0])
                # window = src.window(*bounds.to_crs(src_crs).total_bounds)
                # original_window = src.window(*bounds.to_crs(src_crs).total_bounds)
                # gridded_window = rasterio.windows.round_window_to_full_blocks(
                #     original_window, [(1, 1)]
                # )
                # window = gridded_window  # Expand window to nearest full pixels
                source_data = src.read(
                    window=window,
                    out_shape=(src.count, int(window.height / factor), int(window.width / factor)),
                    resampling=resampling,
                    boundless=True,
                    masked=True,
                )

                window_transform = src.window_transform(window)
                src_transform = window_transform * window_transform.scale(
                    (window.width / source_data.shape[-1]),
                    (window.height / source_data.shape[-2])
                )
                src_dtype = src.dtypes[0]
                src_meta = src.meta
                nodata_value = src.nodatavals[0]

                if filter_list:
                    mask = np.isin(source_data, filter_list, invert=True)
                    source_data[mask] = 0
                if return_colormap:
                    colormap = src.colormap(1)
        except rasterio.RasterioIOError as err:
            print(f"Caught RasterioIOError {err=}, {type(err)=}")
            return  # Return without data
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise
        if output_shape:
            # reproject
            bbox_web = bounds.to_crs("EPSG:3857")
            minx, miny, maxx, maxy = bbox_web.total_bounds
            dx = (maxx - minx) / output_shape[-1]
            dy = (maxy - miny) / output_shape[-2]
            dst_transform = [dx, 0.0, minx, 0.0, -dy, maxy, 0.0, 0.0, 1.0]
            if len(source_data.shape) == 3 and source_data.shape[0] > 1:
                dst_shape = (source_data.shape[0], output_shape[-2], output_shape[-1])
            else:
                dst_shape = output_shape
            dst_crs = bbox_web.crs

            destination_data = np.zeros(dst_shape, src_dtype)
            reproject(
                source_data,
                destination_data,
                src_transform=src_transform,
                src_crs=src_crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                # TODO: rather than nearest, get all the values and then get pct
                resampling=resampling,
            )

            destination_mask = np.zeros(dst_shape, dtype="int8")
            # reproject(
            #     source_data.mask.astype("uint8"),
            #     destination_mask,
            #     src_transform=src_transform,
            #     src_crs=src_crs,
            #     dst_transform=dst_transform,
            #     dst_crs=dst_crs,
            #     resampling=Resampling.nearest,
            # )
            destination_data = np.ma.masked_array(
                destination_data, destination_mask == nodata_value
            )
        else:
            dst_transform = src_transform
            dst_crs = src_crs
            destination_data = source_data
            destination_data = np.ma.masked_array(
                source_data, source_data == nodata_value
            )
    if return_colormap or return_transform or return_crs or return_bounds or return_meta:
        ### TODO: NOT backward comp -- fix colormap / transform 
        metadata={}
        if return_colormap:
            # todo: only set transparency to zero
            colormap[0] = [0, 0, 0, 0]
            metadata['colormap']=colormap
        if return_crs:  
            metadata['crs']=dst_crs
        if return_transform:  # Note: usually you do not need this since it can be calculated using crs=4326 and bounds
            metadata['transform']=dst_transform
        if return_bounds:
            metadata['bounds']=rasterio.transform.array_bounds(destination_data.shape[-2] , destination_data.shape[-1], dst_transform)
        if return_meta:
            metadata['meta']=src_meta
        return destination_data, metadata
        
    else:
        return destination_data


def get_bounds_tiff(tiff_path):
    import rasterio
    with rasterio.open(tiff_path) as src:
        bounds = src.bounds
        import shapely 
        import geopandas as gpd
        bounds = gpd.GeoDataFrame({}, geometry=[shapely.box(*bounds)],crs=src.crs)
        bounds = bounds.to_crs(4326)
        return bounds

def gdf_to_mask_arr(gdf, shape, first_n=None):
    from rasterio.features import geometry_mask

    xmin, ymin, xmax, ymax = gdf.total_bounds
    w = (xmax - xmin) / shape[-1]
    h = (ymax - ymin) / shape[-2]
    if first_n:
        geom = gdf.geometry.iloc[:first_n]
    else:
        geom = gdf.geometry
    return ~geometry_mask(
        geom,
        transform=(w, 0, xmin, 0, -h, ymax, 0, 0, 0),
        invert=True,
        out_shape=shape[-2:],
    )


def mosaic_tiff(
    bounds,
    tiff_list,
    reduce_function=None,
    filter_list=None,
    output_shape=(256, 256),
    overview_level=None,
    cred=None,
):
    import numpy as np

    if not reduce_function:
        reduce_function = lambda x: np.max(x, axis=0)
    a = []
    for input_tiff_path in tiff_list:
        if not input_tiff_path:
            continue
        new_tiff = read_tiff(
            bounds=bounds,
            input_tiff_path=input_tiff_path,
            filter_list=filter_list,
            output_shape=output_shape,
            overview_level=overview_level,
            cred=cred,
        )
        if new_tiff is not None:
            a.append(new_tiff)

    if len(a) == 0:
        return
    elif len(a) == 1:
        data = a[0]
    else:
        data = reduce_function(a)
    return data  # .squeeze()[:-2,:-2]


def arr_resample(arr, dst_shape=(512, 512), order=0):
    import numpy as np
    from scipy.ndimage import zoom

    zoom_factors = np.array(dst_shape) / np.array(arr.shape[-2:])
    if len(arr.shape) == 2:
        return zoom(arr, zoom_factors, order=order)
    elif len(arr.shape) == 3:
        return np.asanyarray([zoom(i, zoom_factors, order=order) for i in arr])


def arr_to_cog(
    arr,
    bounds=(-180, -90, 180, 90),
    crs=4326,
    output_path="output_cog.tif",
    blockxsize=256,
    blockysize=256,
    overviews=[2, 4, 8, 16],
):
    import numpy as np
    import rasterio
    from rasterio.crs import CRS
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds

    data = arr.squeeze()
    # Define the CRS (Coordinate Reference System)
    crs = CRS.from_epsg(crs)

    # Calculate transform
    transform = from_bounds(*bounds, data.shape[-1], data.shape[-2])
    if len(data.shape) == 2:
        data = np.stack([data])
        count = 1
    elif len(data.shape) == 3:
        if data.shape[0] == 3:
            count = 3
        elif data.shape[0] == 4:
            count = 4
        else:
            print(data.shape)
            return f"Wrong number of bands {data.shape[0]}. The options are: 1(gray) | 3 (RGB) | 4 (RGBA)"
    else:
        return f"wrong shape {data.shape}. Data shape options are: (ny,nx) | (1,ny,nx) | (3,ny,nx) | (4,ny,nx)"
    # Write the numpy array to a Cloud-Optimized GeoTIFF file
    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=data.shape[-2],
        width=data.shape[-1],
        count=count,
        dtype=data.dtype,
        crs=crs,
        transform=transform,
        tiled=True,  # Enable tiling
        blockxsize=blockxsize,  # Set block size
        blockysize=blockysize,  # Set block size
        compress="deflate",  # Use compression
        interleave="band",  # Interleave bands
    ) as dst:
        dst.write(data)
        # Build overviews (pyramid layers)
        dst.build_overviews(overviews, Resampling.nearest)
        # Update tags to comply with COG standards
        dst.update_tags(ns="rio_overview", resampling="nearest")
    return output_path


def arr_to_color(arr, colormap, out_dtype="uint8"):
    import numpy as np

    mapped_colors = np.array([colormap[val] for val in arr.flat])
    return (
        mapped_colors.reshape(arr.shape[-2:] + (len(colormap[0]),))
        .astype(out_dtype)
        .transpose(2, 0, 1)
    )


def arr_to_plasma(
    data, min_max=(0, 255), colormap="plasma", include_opacity=False, reverse=True
):
    import numpy as np

    data = data.astype(float)
    if min_max:
        norm_data = (data - min_max[0]) / (min_max[1] - min_max[0])
        norm_data = np.clip(norm_data, 0, 1)
    else:
        print(f"min_max:({round(np.nanmin(data),3)},{round(np.nanmax(data),3)})")
        norm_data = (data - np.nanmin(data)) / (np.nanmax(data) - np.nanmin(data))
    norm_data255 = (norm_data * 255).astype("uint8")
    if colormap:
        # ref: https://matplotlib.org/stable/users/explain/colors/colormaps.html
        from matplotlib import colormaps

        if include_opacity:
            colormap = [
                (
                    np.array(
                        [
                            colormaps[colormap](i)[0],
                            colormaps[colormap](i)[1],
                            colormaps[colormap](i)[2],
                            i,
                        ]
                    )
                    * 255
                ).astype("uint8")
                for i in range(257)
            ]
            if reverse:
                colormap = colormap[::-1]
            mapped_colors = np.array([colormap[val] for val in norm_data255.flat])
            return (
                mapped_colors.reshape(data.shape + (4,))
                .astype("uint8")
                .transpose(2, 0, 1)
            )
        else:
            colormap = [
                (np.array(colormaps[colormap](i)[:3]) * 255).astype("uint8")
                for i in range(256)
            ]
            if reverse:
                colormap = colormap[::-1]
            mapped_colors = np.array([colormap[val] for val in norm_data255.flat])
            return (
                mapped_colors.reshape(data.shape + (3,))
                .astype("uint8")
                .transpose(2, 0, 1)
            )
    else:
        return norm_data255

def run_cmd(cmd, cwd=".", shell=False, communicate=False):
    import shlex
    import subprocess

    if type(cmd) == str:
        cmd = shlex.split(cmd)
    proc = subprocess.Popen(
        cmd, shell=shell, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if communicate:
        return proc.communicate()
    else:
        return proc


def download_file(url, destination):
    import requests

    try:
        response = requests.get(url)
        with open(destination, "wb") as file:
            file.write(response.content)
        return f"File downloaded to '{destination}'."
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"


def fs_list_hls(
    path="lp-prod-protected/HLSL30.020/HLS.L30.T10SEG.2023086T184554.v2.0/",
    env="earthdata",
    earthdatalogin_username="",
    earthdatalogin_password="",
):
    import s3fs
    from job2.credentials import get_credentials

    aws_session = get_credentials(
        env,
        earthdatalogin_username=earthdatalogin_username,
        earthdatalogin_password=earthdatalogin_password,
    )
    fs = s3fs.S3FileSystem(
        key=aws_session["aws_access_key_id"],
        secret=aws_session["aws_secret_access_key"],
        token=aws_session["aws_session_token"],
    )
    return fs.ls(path)


def get_s3_list(path, suffix=None):
    import s3fs

    fs = s3fs.S3FileSystem()
    if suffix:
        return ["s3://" + i for i in fs.ls(path) if i[-len(suffix) :] == suffix]
    else:
        return ["s3://" + i for i in fs.ls(path)]


@fused.cache
def get_s3_list_walk(path):
    #version 2 (recursive)
    import s3fs
    s3 = s3fs.S3FileSystem()
    # Recursively list all files in the specified S3 path
    flist=[]
    for dirpath, dirnames, filenames in s3.walk(path):
        for filename in filenames:
            flist.append(f"s3://{dirpath}/{filename}")
    return flist


def run_async(fn, arr_args, delay=0, max_workers=32):
    import asyncio
    import concurrent.futures

    import nest_asyncio
    import numpy as np

    nest_asyncio.apply()

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
        await asyncio.sleep(delay * np.random.random())
        if type(arr[0]) == list or type(arr[0]) == tuple:
            pass
        else:
            arr = [[i] for i in arr]
        for i in arr:
            tasks.append(fn_async(pool, fn, *i))
        return await asyncio.gather(*tasks)

    return loop.run_until_complete(fn_async_exec(fn, arr_args, delay))


def run_pool(fn, arg_list, max_workers=36):
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(fn, arg_list))


def import_env(
    env="testxenv",
    mnt_path="/mnt/cache/envs/",
    packages_path="/lib/python3.11/site-packages",
):
    import sys

    sys.path.append(f"{mnt_path}{env}{packages_path}")


def install_module(
    name,
    env="testxenv",
    mnt_path="/mnt/cache/envs/",
    packages_path="/lib/python3.11/site-packages",
):
    import_env(env, mnt_path, packages_path)
    import os
    import sys

    path = f"{mnt_path}{env}{packages_path}"
    sys.path.append(path)
    if not os.path.exists(path):
        run_cmd(f"python -m venv  {mnt_path}{env}", communicate=True)
    return run_cmd(
        f"{mnt_path}{env}/bin/python -m pip install {name}", communicate=True
    )


def read_module(url, remove_strings=[]):
    import requests

    content_string = requests.get(url).text
    if len(remove_strings) > 0:
        for i in remove_strings:
            content_string = content_string.replace(i, "")
    module = {}
    exec(content_string, module)
    return module


def get_geo_cols(data) -> List[str]:
    """Get the names of the geometry columns.

    The first item in the result is the name of the primary geometry column. Following
    items are other columns with a type of GeoSeries.
    """
    import geopandas as gpd
    main_col = data.geometry.name
    cols = [
        i for i in data.columns if (type(data[i]) == gpd.GeoSeries) & (i != main_col)
    ]
    return [main_col] + cols


def crs_display(crs):
    """Convert a CRS object into its human-readable EPSG code if possible.

    If the CRS object does not have a corresponding EPSG code, the CRS object itself is
    returned.
    """
    try:
        epsg_code = crs.to_epsg()
        if epsg_code is not None:
            return epsg_code
        else:
            return crs
    except Exception:
        return crs


def resolve_crs(gdf,
                crs,
                verbose= False
):
    """Reproject a GeoDataFrame to the given CRS

    Args:
        gdf: The GeoDataFrame to reproject.
        crs: The CRS to use as destination CRS.
        verbose: Whether to print log statements while running. Defaults to False.

    Returns:
        _description_
    """
    if str(crs).lower() == "utm":
        if gdf.crs is None:
            gdf = gdf.set_crs(4326)
            if verbose:
                logger.debug("No crs exists on `gdf`. Assuming it's WGS84 (epsg:4326).")

        utm_crs = gdf.estimate_utm_crs()
        if gdf.crs == utm_crs:
            if verbose:
                logger.debug(f"CRS is already {crs_display(utm_crs)}.")
            return gdf

        else:
            if verbose:
                logger.debug(
                    f"Converting from {crs_display(gdf.crs)} to {crs_display(utm_crs)}."
                )
            return gdf.to_crs(utm_crs)

    elif (gdf.crs is not None) & (gdf.crs != crs):
        old_crs = gdf.crs
        if verbose:
            logger.debug(
                f"Converting from {crs_display(old_crs)} to {crs_display(crs)}."
            )
        return gdf.to_crs(crs)
    elif gdf.crs is None:
        raise ValueError("gdf.crs is None and reprojection could not be performed.")
    else:
        if verbose:
            logger.debug(f"crs is already {crs_display(crs)}.")

        return gdf


def infer_lonlat(columns: Sequence[str]) -> Optional[Tuple[str, str]]:
    """Infer longitude and latitude columns from the column names of the DataFrame

    Args:
        columns: the column names in the DataFrame

    Returns:
        The pair of (longitude, latitude) column names, if found. Otherwise None.
    """
    columns_set = set(columns)
    allowed_column_pairs = [
        ("longitude", "latitude"),
        ("lon", "lat"),
        ("lng", "lat"),
        ("fused_centroid_x", "fused_centroid_y"),
        ("fused_centroid_x_left", "fused_centroid_y_left"),
        ("fused_centroid_x_right", "fused_centroid_x_right"),
    ]
    for allowed_column_pair in allowed_column_pairs:
        if (
            allowed_column_pair[0] in columns_set
            and allowed_column_pair[1] in columns_set
        ):
            return allowed_column_pair
    return None


def df_to_gdf(df, cols_lonlat=None, verbose=False):
    import json

    import pyarrow as pa
    import shapely
    from geopandas.io.arrow import _arrow_to_geopandas

    geo_metadata = {
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "crs": 4326}},
        "version": "1.0.0-beta.1",
    }
    arrow_geo_metadata = {b"geo": json.dumps(geo_metadata).encode()}
    if not cols_lonlat:
        cols_lonlat = infer_lonlat(list(df.columns))
        if not cols_lonlat:
            raise ValueError("no latitude and longitude columns were found.")

        assert (
            cols_lonlat[0] in df.columns
        ), f"column name {cols_lonlat[0]} was not found."
        assert (
            cols_lonlat[1] in df.columns
        ), f"column name {cols_lonlat[1]} was not found."

        if verbose:
            logger.debug(
                f"Converting {cols_lonlat} to points({cols_lonlat[0]},{cols_lonlat[0]})."
            )
    geoms = shapely.points(df[cols_lonlat[0]], df[cols_lonlat[1]])
    table = pa.Table.from_pandas(df)
    table = table.append_column("geometry", pa.array(shapely.to_wkb(geoms)))
    table = table.replace_schema_metadata(arrow_geo_metadata)
    try:
        df = _arrow_to_geopandas(table)
    except:
        df = _arrow_to_geopandas(table.drop(["__index_level_0__"]))
    return df


def to_gdf(
    data,
    crs=None,
    cols_lonlat=None,
    col_geom="geometry",
    verbose: bool = False,
):
    """Convert input data into a GeoPandas GeoDataFrame."""
    import geopandas as gpd
    import shapely
    import pandas as pd
    import mercantile
    
    # Convert xyz dict to xyz array
    if isinstance(data, dict) and set(data.keys()) == {'x', 'y', 'z'}:
        try:
            data = [int(data['x']), int(data['y']), int(data['z'])]
        except (ValueError, TypeError):
            pass     
            
    
    if data is None or (isinstance(data, (list, tuple, np.ndarray))):
        
        data = [327, 791, 11] if data is None else data #if no data, get a tile in SF
        
        if len(data) == 3: # Handle xyz tile coordinates
            x, y, z = data
            tile = mercantile.Tile(x, y, z)
            bounds = mercantile.bounds(tile)
            gdf = gpd.GeoDataFrame(
                {"x": [x], "y": [y], "z": [z]},
                geometry=[shapely.box(bounds.west, bounds.south, bounds.east, bounds.north)],
                crs=4326
            )
            return gdf[['x', 'y', 'z', 'geometry']]
         
        elif len(data) == 4: # Handle the bounds case specifically        
            return gpd.GeoDataFrame({}, geometry=[shapely.box(*data)], crs=crs or 4326)        
        
    if cols_lonlat:
        if isinstance(data, pd.Series):
            raise ValueError(
                "Cannot pass a pandas Series or a geopandas GeoSeries in conjunction "
                "with cols_lonlat."
            )
        gdf = df_to_gdf(data, cols_lonlat, verbose=verbose)
        if verbose:
            logger.debug(
                "cols_lonlat was passed so original CRS was assumed to be EPSG:4326."
            )
        if crs:
            gdf = resolve_crs(gdf, crs, verbose=verbose)
        return gdf
    if isinstance(data, gpd.GeoDataFrame):
        gdf = data
        if crs:
            gdf = resolve_crs(gdf, crs, verbose=verbose)
        elif gdf.crs is None:
            raise ValueError("Please provide crs. usually crs=4326.")
        return gdf
    elif isinstance(data, gpd.GeoSeries):
        gdf = gpd.GeoDataFrame(data=data)
        if crs:
            gdf = resolve_crs(gdf, crs, verbose=verbose)
        elif gdf.crs is None:
            raise ValueError("Please provide crs. usually crs=4326.")
        return gdf
    elif type(data) in (pd.DataFrame, pd.Series):
        if type(data) is pd.Series:
            data = pd.DataFrame(data)
            if col_geom in data.index:
                data = data.T
        if (col_geom in data.columns) and (not cols_lonlat):
            if type(data[col_geom][0]) == str:
                gdf = gpd.GeoDataFrame(
                    data.drop(columns=[col_geom]),
                    geometry=shapely.from_wkt(data[col_geom]),
                )
            else:
                gdf = gpd.GeoDataFrame(data)
            if gdf.crs is None:
                if crs:
                    gdf = gdf.set_crs(crs)
                else:
                    raise ValueError("Please provide crs. usually crs=4326.")
            elif crs:
                gdf = resolve_crs(gdf, crs, verbose=verbose)
        elif not cols_lonlat:
            cols_lonlat = infer_lonlat(data.columns)
            if not cols_lonlat:
                raise ValueError("no latitude and longitude columns were found.")
            if crs:
                if verbose:
                    logger.debug(
                        f"cols_lonlat was passed so crs was set to wgs84(4326) and {crs=} was ignored."
                    )
            # This is needed for Python 3.8 specifically, because otherwise creating the GeoDataFrame modifies the input DataFrame
            data = data.copy()
            gdf = df_to_gdf(data, cols_lonlat, verbose=verbose)
        return gdf
    elif (
        isinstance(data, shapely.geometry.base.BaseGeometry)
        or isinstance(data, shapely.geometry.base.BaseMultipartGeometry)
        or isinstance(data, shapely.geometry.base.EmptyGeometry)
    ):
        if not crs:
            raise ValueError("Please provide crs. usually crs=4326.")
        return gpd.GeoDataFrame(geometry=[data], crs=crs)
    else:
        raise ValueError(
            f"Cannot convert data of type {type(data)} to GeoDataFrame. Please pass a GeoDataFrame, GeoSeries, DataFrame, Series, or shapely geometry."
        )

def geo_buffer(
    data,
    buffer_distance=1000,
    utm_crs="utm",
    dst_crs="original",
    col_geom_buff="geom_buff",
    verbose: bool=False,
):
    """Buffer the geometry column in a GeoDataFrame in UTM projection.

    Args:
        data: The GeoDataFrame to use as input.
        buffer_distance: The distance in meters to use for buffering geometries. Defaults to 1000.
        utm_crs: The CRS to use for the buffering operation. Geometries will be reprojected to this CRS before the buffering operation is applied. Defaults to "utm", which finds the most appropriate UTM zone based on the data.
        dst_crs: The CRS to use for the output geometries. Defaults to "original", which matches the CRS defined in the input data.
        col_geom_buff: The name of the column that should store the buffered geometries. Defaults to "geom_buff".
        verbose: Whether to print logging output. Defaults to False.

    Returns:
        A new GeoDataFrame with a new column with buffered geometries.
    """
    data = data.copy()
    assert data.crs not in (
        None,
        "",
    ), "no crs was not found. use to_gdf to add crs"
    if str(dst_crs).lower().replace("_", "").replace(" ", "").replace("-", "") in [
        "original",
        "originalcrs",
        "origcrs",
        "orig",
        "source",
        "sourcecrs",
        "srccrs",
        "src",
    ]:
        dst_crs = data.crs
    if utm_crs:
        data[col_geom_buff] = resolve_crs(data, utm_crs, verbose=verbose).buffer(
            buffer_distance
        )
        data = data.set_geometry(col_geom_buff)
    else:
        data[col_geom_buff] = data.buffer(buffer_distance)
        data = data.set_geometry(col_geom_buff)
    if dst_crs:
        return resolve_crs(data, dst_crs, verbose=verbose)
    else:
        return data


def geo_bbox(
    data,
    dst_crs=None,
    verbose: bool = False,
):
    """Generate a GeoDataFrame that has the bounds of the current data frame.

    Args:
        data: the GeoDataFrame to use as input.
        dst_crs: Destination CRS. Defaults to None.
        verbose: Provide extra logging output. Defaults to False.

    Returns:
        A GeoDataFrame with one row, containing a geometry that has the bounds of this
    """
    import geopandas as gpd
    import shapely
    import pyproj
    src_crs = data.crs
    if not dst_crs:
        return to_gdf(
            shapely.geometry.box(*data.total_bounds), crs=src_crs, verbose=verbose
        )
    elif str(dst_crs).lower() == "utm":
        dst_crs = data.estimate_utm_crs()
        logger.debug(f"estimated dst_crs={crs_display(dst_crs)}")
    transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    dst_bounds = transformer.transform_bounds(*data.total_bounds)
    return to_gdf(
        shapely.geometry.box(*dst_bounds, ccw=True), crs=dst_crs, verbose=verbose
    )


def clip_bbox_gdfs(
    left,
    right,
    buffer_distance: Union[int, float] = 1000,
    join_type: Literal["left", "right"] = "left",
    verbose: bool = True,
):
    """Clip a DataFrame by a bounding box and then join to another DataFrame

    Args:
        left: The left GeoDataFrame to use for the join.
        right: The right GeoDataFrame to use for the join.
        buffer_distance: The distance in meters to use for buffering before joining geometries. Defaults to 1000.
        join_type: _description_. Defaults to "left".
        verbose: Provide extra logging output. Defaults to False.
    """

    def fn(df1, df2, buffer_distance=buffer_distance):
        if buffer_distance:
            utm_crs = df1.estimate_utm_crs()
            # transform bounds to utm & buffer & then to df2_crs
            bbox_utm = geo_bbox(df1, dst_crs=utm_crs, verbose=verbose)
            bbox_utm_buff = geo_buffer(
                bbox_utm, buffer_distance, utm_crs=None, dst_crs=None, verbose=verbose
            )
            bbox_utm_buff_df2_crs = geo_bbox(
                bbox_utm_buff, dst_crs=df2.crs, verbose=verbose
            )
            return df1, df2.sjoin(bbox_utm_buff_df2_crs).drop(columns="index_right")
        else:
            return df1, df2.sjoin(geo_bbox(df1, dst_crs=df2.crs, verbose=verbose)).drop(
                columns="index_right"
            )

    if join_type.lower() == "left":
        left, right = fn(left, right)
    elif join_type.lower() == "right":
        right, left = fn(right, left)
    else:
        assert False, "join_type should be left or right"

    return left, right


def geo_join(
    left,
    right,
    buffer_distance: Union[int, float, None] = None,
    utm_crs="utm",
    clip_bbox="left",
    drop_extra_geoms: bool = True,
    verbose: bool = False,
):
    """Join two GeoDataFrames

    Args:
        left: The left GeoDataFrame to use for the join.
        right: The right GeoDataFrame to use for the join.
        buffer_distance: The distance in meters to use for buffering before joining geometries. Defaults to None.
        utm_crs: The CRS used for UTM computations. Defaults to "utm", which infers a suitable UTM zone.
        clip_bbox: A bounding box used for clipping in the join step. Defaults to "left".
        drop_extra_geoms: Keep only the first geometry column. Defaults to True.
        verbose: Provide extra logging output. Defaults to False.

    Returns:
        Joined GeoDataFrame.
    """
    import geopandas as gpd
    import shapely
    if type(left) != gpd.GeoDataFrame:
        left = to_gdf(left, verbose=verbose)
    if type(right) != gpd.GeoDataFrame:
        right = to_gdf(right, verbose=verbose)
    left_geom_cols = get_geo_cols(left)
    right_geom_cols = get_geo_cols(right)
    if verbose:
        logger.debug(
            f"primary geometry columns -- input left: {left_geom_cols[0]} | input right: {right_geom_cols[0]}"
        )
    if clip_bbox:
        assert clip_bbox in (
            "left",
            "right",
        ), f'{clip_bbox} not in ("left", "right", "None").'
        left, right = clip_bbox_gdfs(
            left,
            right,
            buffer_distance=buffer_distance,
            join_type=clip_bbox,
            verbose=verbose,
        )
    if drop_extra_geoms:
        left = left.drop(columns=left_geom_cols[1:])
        right = right.drop(columns=right_geom_cols[1:])
    conflict_list = [
        col
        for col in right.columns
        if (col in left.columns) & (col not in (right_geom_cols[0]))
    ]
    for col in conflict_list:
        right[f"{col}_right"] = right[col]
    right = right.drop(columns=conflict_list)
    if not drop_extra_geoms:
        right[right_geom_cols[0] + "_right"] = right[right_geom_cols[0]]
    if buffer_distance:
        if not utm_crs:
            utm_crs = left.crs
            if verbose:
                logger.debug(
                    f"No crs transform before applying buffer (left crs:{crs_display(utm_crs)})."
                )
        df_joined = geo_buffer(
            left,
            buffer_distance,
            utm_crs=utm_crs,
            dst_crs=right.crs,
            col_geom_buff="_fused_geom_buff_",
            verbose=verbose,
        ).sjoin(right)
        df_joined = df_joined.set_geometry(left_geom_cols[0]).drop(
            columns=["_fused_geom_buff_"]
        )
        if left.crs == right.crs:
            if verbose:
                logger.debug(
                    f"primary geometry columns -- output: {df_joined.geometry.name}"
                )
            return df_joined
        else:
            df_joined = df_joined.to_crs(left.crs)
            if verbose:
                logger.debug(
                    f"primary geometry columns -- output: {df_joined.geometry.name}"
                )
            return df_joined
    else:
        if left.crs != right.crs:
            df_joined = left.to_crs(right.crs).sjoin(right).to_crs(left.crs)
            if verbose:
                logger.debug(
                    f"primary geometry columns -- output: {df_joined.geometry.name}"
                )
            return df_joined
        else:
            df_joined = left.sjoin(right)
            if verbose:
                logger.debug(
                    f"primary geometry columns -- output: {df_joined.geometry.name}"
                )
            return df_joined


def geo_distance(
    left,
    right,
    search_distance: Union[int, float] = 1000,
    utm_crs="utm",
    clip_bbox="left",
    col_distance: str = "distance",
    k_nearest: int = 1,
    cols_agg: Sequence[str] = ("fused_index",),
    cols_right: Sequence[str] = (),
    drop_extra_geoms: bool = True,
    verbose: bool = False,
):
    """Compute the distance from rows in one dataframe to another.

    First this performs an geo_join, and then finds the nearest rows in right to left.

    Args:
        left: left GeoDataFrame
        right: right GeoDataFrame
        search_distance: Distance in meters used for buffering in the geo_join step. Defaults to 1000.
        utm_crs: The CRS used for UTM computations. Defaults to "utm", which infers a suitable UTM zone.
        clip_bbox: A bounding box used for clipping in the join step. Defaults to "left".
        col_distance: The column named for saving the output of the distance step. Defaults to "distance".
        k_nearest: The number of nearest values to keep.. Defaults to 1.
        cols_agg: Columns used for the aggregation before the join. Defaults to ("fused_index",).
        cols_right: Columns from the right dataframe to keep. Defaults to ().
        drop_extra_geoms: Keep only the first geometry column. Defaults to True.
        verbose: Provide extra logging output. Defaults to False.

    Returns:
        _description_
    """
    import geopandas as gpd
    import shapely
    if type(left) != gpd.GeoDataFrame:
        left = to_gdf(left, verbose=verbose)
    if type(right) != gpd.GeoDataFrame:
        right = to_gdf(right, verbose=verbose)
    left_geom_cols = get_geo_cols(left)
    right_geom_cols = get_geo_cols(right)
    cols_right = list(cols_right)
    if drop_extra_geoms:
        left = left.drop(columns=left_geom_cols[1:])
        right = right.drop(columns=right_geom_cols[1:])
        cols_right = [i for i in cols_right if i in right.columns]
    if right_geom_cols[0] not in cols_right:
        cols_right += [right_geom_cols[0]]
    if cols_agg:
        cols_agg = list(cols_agg)
        all_cols = set(list(left.columns) + list(cols_right))
        assert (
            len(set(cols_agg) - all_cols) == 0
        ), f"{cols_agg=} not in the table. Please pass valid list or cols_agg=None if you want distance over entire datafame"
    dfj = geo_join(
        left,
        right[cols_right],
        buffer_distance=search_distance,
        utm_crs=utm_crs,
        clip_bbox=clip_bbox,
        drop_extra_geoms=False,
        verbose=verbose,
    )
    if len(dfj) > 0:
        utm_crs = dfj[left_geom_cols[0]].estimate_utm_crs()
        dfj[col_distance] = (
            dfj[[left_geom_cols[0]]]
            .to_crs(utm_crs)
            .distance(dfj[f"{right_geom_cols[0]}_right"].to_crs(utm_crs))
        )
    else:
        dfj[col_distance] = 0
        if verbose:
            print("geo_join returned empty dataframe.")

    if drop_extra_geoms:
        dfj = dfj.drop(columns=get_geo_cols(dfj)[1:])
    if cols_agg:
        dfj = dfj.sort_values(col_distance).groupby(cols_agg).head(k_nearest)
    else:
        dfj = dfj.sort_values(col_distance).head(k_nearest)
    return dfj


def geo_samples(
    n_samples: int, min_x: float, max_x: float, min_y: float, max_y: float
):
    """
    Generate sample points in a bounding box, uniformly.

    Args:
        n_samples: Number of sample points to generate
        min_x: Minimum x coordinate
        max_x: Maximum x coordinate
        min_y: Minimum y coordinate
        max_y: Maximum y coordinate

    Returns:
        A GeoDataFrame with point geometry.
    """
    import geopandas as gpd
    import shapely
    import random
    points = [
        (random.uniform(min_x, max_x), random.uniform(min_y, max_y))
        for _ in range(n_samples)
    ]
    return to_gdf(pd.DataFrame(points, columns=["lng", "lat"]))[["geometry"]]


def bbox_stac_items(bounds, table):
    import fused
    import geopandas as gpd
    import pandas as pd
    import pyarrow.parquet as pq
    import shapely

    df = fused.get_chunks_metadata(table)
    df = df[df.intersects(bounds)]
    if len(df) > 10 or len(df) == 0:
        return None  # fault
    matching_images = []
    for idx, row in df.iterrows():
        file_url = table + row["file_id"] + ".parquet"
        chunk_table = pq.ParquetFile(file_url).read_row_group(row["chunk_id"])
        chunk_gdf = gpd.GeoDataFrame(chunk_table.to_pandas())
        if "geometry" in chunk_gdf:
            chunk_gdf.geometry = shapely.from_wkb(chunk_gdf["geometry"])
        matching_images.append(chunk_gdf)

    ret_gdf = pd.concat(matching_images)
    ret_gdf = ret_gdf[ret_gdf.intersects(bounds)]
    return ret_gdf


# todo: switch to read_tiff with requester_pays option
def read_tiff_naip(
    bounds, input_tiff_path, crs, buffer_degree, output_shape, resample_order=0
):
    from io import BytesIO

    import rasterio
    from rasterio.session import AWSSession
    from scipy.ndimage import zoom

    out_buf = BytesIO()
    with rasterio.Env(AWSSession(requester_pays=True)):
        with rasterio.open(input_tiff_path) as src:
            if buffer_degree != 0:
                bounds.geometry = bounds.geometry.buffer(buffer_degree)
            bbox_projected = bounds.to_crs(crs)
            window = src.window(*bbox_projected.total_bounds)
            data = src.read(window=window, boundless=True)
            zoom_factors = np.array(output_shape) / np.array(data[0].shape)
            rgb = np.array(
                [zoom(arr, zoom_factors, order=resample_order) for arr in data]
            )
        return rgb


def image_server_bbox(
    image_url,
    bounds=None,
    time=None,
    size=512,
    bbox_crs=4326,
    image_crs=3857,
    image_format="tiff",
    return_colormap=False,
):
    if bbox_crs and bbox_crs != image_crs:
        import geopandas as gpd
        import shapely

        gdf = gpd.GeoDataFrame(geometry=[shapely.box(*bounds)], crs=bbox_crs).to_crs(
            image_crs
        )
        print(gdf)
        minx, miny, maxx, maxy = gdf.total_bounds
    else:
        minx, miny, maxx, maxy = list(bounds)
    image_url = image_url.strip("/")
    url_template = f"{image_url}?f=image"
    url_template += f"&bounds={minx},{miny},{maxx},{maxy}"
    if time:
        url_template += f"&time={time}"
    if image_crs:
        url_template += f"&imageSR={image_crs}&bboxSR={image_crs}"
    if size:
        url_template += f"&size={size},{size*(miny-maxy)/(minx-maxx)}"
    url_template += f"&format={image_format}"
    return url_to_arr(url_template, return_colormap=return_colormap)


def arr_to_stats(arr, gdf, type="nominal"):
    import numpy as np

    minx, miny, maxx, maxy = gdf.total_bounds
    dx = (maxx - minx) / arr.shape[-1]
    dy = (maxy - miny) / arr.shape[-2]
    transform = [dx, 0.0, minx, 0.0, -dy, maxy, 0.0, 0.0, 1.0]
    geom_masks = [
        rasterize_geometry(geom, arr.shape[-2:], transform) for geom in gdf.geometry
    ]
    if type == "nominal":
        counts_per_mask = []
        for geom_mask in geom_masks:
            masked_arr = arr[geom_mask]
            unique_values, counts = np.unique(masked_arr, return_counts=True)
            counts_dict = {value: count for value, count in zip(unique_values, counts)}
            counts_per_mask.append(counts_dict)
        gdf["stats"] = counts_per_mask
        return gdf
    elif type == "numerical":
        stats_per_mask = []
        for geom_mask in geom_masks:
            masked_arr = arr[geom_mask]
            stats_per_mask.append(
                {
                    "min": np.nanmin(masked_arr),
                    "max": np.nanmax(masked_arr),
                    "mean": np.nanmean(masked_arr),
                    "median": np.nanmedian(masked_arr),
                    "std": np.nanstd(masked_arr),
                }
            )
        gdf["stats"] = stats_per_mask
        return gdf
    else:
        raise ValueError(
            f'{type} is not supported. Type options are "nominal" and "numerical"'
        )


def ask_openai(prompt, openai_api_key, role="user", model="gpt-4-turbo-preview"):
    # ref: https://github.com/openai/openai-python
    # ref: https://platform.openai.com/docs/models/gpt-4-and-gpt-4-turbo
    from openai import OpenAI

    client = OpenAI(
        api_key=openai_api_key,
    )
    messages = [
        {
            "role": role,
            "content": prompt,
        }
    ]
    chat_completion = client.chat.completions.create(
        messages=messages,
        model=model,
    )
    return [i.message.content for i in chat_completion.choices]


def ee_initialize(service_account_name="", key_path=""):
    """
    Authenticate with Google Earth Engine using service account credentials.

    This function initializes the Earth Engine API by authenticating with the
    provided service account credentials. The authenticated session allows for
    accessing and manipulating data within Google Earth Engine.

    Args:
        service_account_name (str): The email address of the service account.
        key_path (str): The path to the private key file for the service account.

    See documentation: https://docs.fused.io/basics/in/gee/

    Example:
        ee_initialize('your-service-account@your-project.iam.gserviceaccount.com', 'path/to/your-private-key.json')
    """
    import ee

    credentials = ee.ServiceAccountCredentials(service_account_name, key_path)
    ee.Initialize(
        opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials
    )


@fused.cache
def run_pool_tiffs(bounds, df_tiffs, output_shape):
    import numpy as np

    columns = df_tiffs.columns

    @fused.cache
    def fn_read_tiff(tiff_url, bounds=bounds, output_shape=output_shape):
        read_tiff = fused.load(
            "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
        ).utils.read_tiff
        return read_tiff(bounds, tiff_url, output_shape=output_shape)

    tiff_list = []
    for band in columns:
        for i in range(len(df_tiffs)):
            tiff_list.append(df_tiffs[band].iloc[i])

    arrs_tmp = run_pool(fn_read_tiff, tiff_list)
    arrs_out = np.stack(arrs_tmp)
    arrs_out = arrs_out.reshape(
        len(columns), len(df_tiffs), output_shape[0], output_shape[1]
    )
    return arrs_out


def search_pc_catalog(
    bounds,
    time_of_interest,
    query={"eo:cloud_cover": {"lt": 5}},
    collection="sentinel-2-l2a",
):
    import planetary_computer
    import pystac_client

    # Instantiate PC client
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    # Search catalog
    items = catalog.search(
        collections=[collection],
        bbox=bounds.total_bounds,
        datetime=time_of_interest,
        query=query,
    ).item_collection()

    if len(items) == 0:
        print(f"empty for {time_of_interest}")
        return

    return items


def create_tiffs_catalog(stac_items, band_list):
    import pandas as pd

    input_paths = []
    for selected_item in stac_items:
        max_key_length = len(max(selected_item.assets, key=len))
        input_paths.append([selected_item.assets[band].href for band in band_list])
    return pd.DataFrame(input_paths, columns=band_list)

def create_chunk_metadata(df, chunk_size=10_000, cols_lonlat=['lng', 'lat']):
    total_rows = len(df)
    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    meta = []
    for idx in range(num_chunks):
        chunk = df.iloc[idx * chunk_size:min((idx + 1) * chunk_size, total_rows)]
        meta.append({
            "bbox_minx": chunk[cols_lonlat[0]].min(),
            "bbox_maxx": chunk[cols_lonlat[0]].max(),
            "bbox_miny": chunk[cols_lonlat[1]].min(),
            "bbox_maxy": chunk[cols_lonlat[1]].max(),
            "chunk_id": idx,
            "num_rows": len(chunk)  # Additional stat showing number of rows in chunk
        })
    import pandas as pd
    return pd.DataFrame.from_records(meta)

def chunked_tiff_to_points(tiff_path, i: int = 0, x_chunks: int = 2, y_chunks: int = 2):
    import numpy as np
    import pandas as pd
    import rasterio

    with rasterio.open(tiff_path) as src:
        x_list, y_list = shape_transform_to_xycoor(src.shape[-2:], src.transform)
        x_slice, y_slice = get_chunk_slices_from_shape(
            src.shape[-2:], x_chunks, y_chunks, i
        )
        x_list = x_list[x_slice]
        y_list = y_list[y_slice]
        X, Y = np.meshgrid(x_list, y_list)
        arr = src.read(window=(y_slice, x_slice)).flatten()
        df = pd.DataFrame(X.flatten(), columns=["lng"])
        df["lat"] = Y.flatten()
        df["data"] = arr
    return df


def shape_transform_to_xycoor(shape, transform):
    import numpy as np

    n_y = shape[-2]
    n_x = shape[-1]
    w, _, x, _, h, y, _, _, _ = transform
    x_list = np.arange(x + w / 2, x + n_x * w + w / 2, w)[:n_x]
    y_list = np.arange(y + h / 2, y + n_y * h + h / 2, h)[:n_y]
    return x_list, y_list


def get_chunk_slices_from_shape(array_shape, x_chunks, y_chunks, i):
    # Unpack the dimensions of the array shape
    rows, cols = array_shape

    # Calculate the size of each chunk
    chunk_height = rows // y_chunks
    chunk_width = cols // x_chunks

    # Calculate row and column index for the i-th chunk
    row_start = (i // x_chunks) * chunk_height
    col_start = (i % x_chunks) * chunk_width

    # Create slice objects for the i-th chunk
    y_slice = slice(row_start, row_start + chunk_height)
    x_slice = slice(col_start, col_start + chunk_width)

    return x_slice, y_slice

def arr_to_latlng(arr, bounds):
    import numpy as np
    import pandas as pd
    from rasterio.transform import from_bounds
    transform = from_bounds(*bounds, arr.shape[-1], arr.shape[-2])
    x_list, y_list = shape_transform_to_xycoor(arr.shape[-2:], transform)
    X, Y = np.meshgrid(x_list, y_list)
    df = pd.DataFrame(X.flatten(), columns=["lng"])
    df["lat"] = Y.flatten()
    df["data"] = arr.flatten()
    return df

# @fused.cache
def df_to_h3(df, res, latlng_cols=("lat", "lng"), ordered=False):
    qr = f"""
            SELECT h3_latlng_to_cell({latlng_cols[0]}, {latlng_cols[1]}, {res}) AS hex, ARRAY_AGG(data) as agg_data
            FROM df
            group by 1
        """
    if ordered:
        qr+="  order by 1"
    con = duckdb_connect()
    return con.query(qr).df()

def arr_to_h3(arr, bounds, res, ordered=False):
    return df_to_h3(arr_to_latlng(arr, bounds), res=res, ordered=ordered)

def duckdb_connect(verbose=False, home_directory='/tmp/duckdb/'):
    import os
    os.makedirs(home_directory, exist_ok=True)
    import duckdb 
    @fused.cache(storage='local')
    def install(home_directory):
        con = duckdb.connect()
        con.sql(
        f"""SET home_directory='{home_directory}';
        INSTALL h3 FROM community;
        INSTALL 'httpfs';
        
        INSTALL spatial;
        """)
    install(home_directory)
    con = duckdb.connect()
    con.sql(
    f"""SET home_directory='{home_directory}';
    LOAD h3;
    LOAD 'httpfs';
    LOAD spatial;
    """)
    if verbose:
        print(f"duckdb version: {duckdb.__version__} | {home_directory=}")
    return con


# @fused.cache
def run_query(query, return_arrow=False):
    con = duckdb_connect()
    if return_arrow:
        return con.sql(query).fetch_arrow_table()
    else:
        return con.sql(query).df()


def ds_to_tile(ds, variable, bounds, na_values=0, cols_lonlat=('x', 'y')):
    da = ds[variable]
    x_slice, y_slice = bbox_to_xy_slice(
        bounds.total_bounds, ds.rio.shape, ds.rio.transform()
    )
    window = bbox_to_window(bounds.total_bounds, ds.rio.shape, ds.rio.transform())
    py0 = py1 = px0 = px1 = 0
    if window.col_off < 0:
        px0 = -window.col_off
    if window.col_off + window.width > da.shape[-2]:
        px1 = window.col_off + window.width - da.shape[-2]
    if window.row_off < 0:
        py0 = -window.row_off
    if window.row_off + window.height > da.shape[-1]:
        py1 = window.row_off + window.height - da.shape[-1]
    # data = da.isel(x=x_slice, y=y_slice, time=0).fillna(0)
    data = da.isel({cols_lonlat[0]:x_slice, cols_lonlat[1]:y_slice}).fillna(0)
    data = data.pad({cols_lonlat[0]:(px0, px1), cols_lonlat[1]:(py0, py1)},
                     mode="constant", constant_values=na_values
    )
    return data

# @fused.cache(cache_max_age='24h')
def tiff_to_xyz(input_tiff, output_xyz, xoff, x_block_size, yoff, y_block_size):
    cmd = f"gdal_translate -srcwin {xoff*x_block_size} {yoff*y_block_size} {x_block_size} {y_block_size} -of XYZ {input_tiff} {output_xyz}"
    r = run_cmd(cmd, communicate=True)
    # assert r[1] == b""   # cehck if there is an error (r[1] != b"")
    return r

def xy_transform(df, src_crs="EPSG:5070", dst_crs="EPSG:4326", 
                         cols_src_xy=['x','y'], cols_dst_xy=['lng', 'lat']):
    from pyproj import Proj, transform

    # Define the source (original CRS) and target (EPSG:4326) CRSs
    source_proj = Proj(init=src_crs)  # Replace with original CRS
    target_proj = Proj(init=dst_crs)  # EPSG:4326 for WGS84

    # Transform the X, Y coordinates to EPSG:4326 (WGS84)
    x_coords = df[cols_src_xy[0]].values
    y_coords = df[cols_src_xy[1]].values
    lon, lat = transform(source_proj, target_proj, x_coords, y_coords)
    df[cols_dst_xy[0]] = lon
    df[cols_dst_xy[1]] = lat
    return df

def bbox_to_xy_slice(bounds, shape, transform):
    import rasterio
    from affine import Affine

    if transform[4] < 0:  # if pixel_height is negative
        original_window = rasterio.windows.from_bounds(*bounds, transform=transform)
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        y_slice, x_slice = gridded_window.toslices()
        return x_slice, y_slice
    else:  # if pixel_height is not negative
        original_window = rasterio.windows.from_bounds(
            *bounds,
            transform=Affine(
                transform[0],
                transform[1],
                transform[2],
                transform[3],
                -transform[4],
                -transform[5],
            ),
        )
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        y_slice, x_slice = gridded_window.toslices()
        y_slice = slice(shape[0] - y_slice.stop, shape[0] - y_slice.start + 0)
        return x_slice, y_slice


def bbox_to_window(bounds, shape, transform):
    import rasterio
    from affine import Affine

    if transform[4] < 0:  # if pixel_height is negative
        original_window = rasterio.windows.from_bounds(*bounds, transform=transform)
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        return gridded_window
    else:  # if pixel_height is not negative
        original_window = rasterio.windows.from_bounds(
            *bounds,
            transform=Affine(
                transform[0],
                transform[1],
                transform[2],
                transform[3],
                -transform[4],
                -transform[5],
            ),
        )
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        return gridded_window


def bounds_to_gdf(bounds_list, crs=4326):
    import geopandas as gpd
    import shapely

    box = shapely.box(*bounds_list)
    return gpd.GeoDataFrame(geometry=[box], crs=crs)


def mercantile_polyfill(geom, zooms=[15], compact=True, k=None):
    import geopandas as gpd
    import mercantile
    import shapely

    gdf = to_gdf(geom , crs = 4326)
    geometry = gdf.geometry[0]

    tile_list = list(mercantile.tiles(*geometry.bounds, zooms=zooms))
    gdf_tiles = gpd.GeoDataFrame(
        tile_list,
        geometry=[shapely.box(*mercantile.bounds(i)) for i in tile_list],
        crs=4326,
    )
    gdf_tiles_intersecting = gdf_tiles[gdf_tiles.intersects(geometry)]

    if k:
        temp_list = gdf_tiles_intersecting.apply(
            lambda row: mercantile.Tile(row.x, row.y, row.z), 1
        )
        clip_list = mercantile_kring_list(temp_list, k)
        if not compact:
            gdf = gpd.GeoDataFrame(
                clip_list,
                geometry=[shapely.box(*mercantile.bounds(i)) for i in clip_list],
                crs=4326,
            )
            return gdf
    else:
        if not compact:
            return gdf_tiles_intersecting
        clip_list = gdf_tiles_intersecting.apply(
            lambda row: mercantile.Tile(row.x, row.y, row.z), 1
        )
    simple_list = mercantile.simplify(clip_list)
    gdf = gpd.GeoDataFrame(
        simple_list,
        geometry=[shapely.box(*mercantile.bounds(i)) for i in simple_list],
        crs=4326,
    )
    return gdf  # .reset_index(drop=True)


def mercantile_kring(tile, k):
    # ToDo: Remove invalid tiles in the globe boundries (e.g. negative values)
    import mercantile

    result = []
    for x in range(tile.x - k, tile.x + k + 1):
        for y in range(tile.y - k, tile.y + k + 1):
            result.append(mercantile.Tile(x, y, tile.z))
    return result


def mercantile_kring_list(tiles, k):
    a = []
    for tile in tiles:
        a.extend(mercantile_kring(tile, k))
    return list(set(a))


def make_tiles_gdf(bounds, zoom=14, k=0, compact=0):
    import shapely

    df_tiles = mercantile_polyfill(
        shapely.box(*bounds), zooms=[zoom], compact=compact, k=k
    )
    df_tiles["bounds"] = df_tiles["geometry"].apply(lambda x: x.bounds, 1)
    return df_tiles


def get_masked_array(gdf_aoi, arr_aoi):
    import numpy as np
    from rasterio.features import geometry_mask
    from rasterio.transform import from_bounds

    transform = from_bounds(*gdf_aoi.total_bounds, arr_aoi.shape[-1], arr_aoi.shape[-2])
    geom_mask = geometry_mask(
        gdf_aoi.geometry,
        transform=transform,
        invert=True,
        out_shape=arr_aoi.shape[-2:],
    )
    masked_value = np.ma.MaskedArray(data=arr_aoi, mask=[~geom_mask])
    return masked_value


def get_da(path, coarsen_factor=1, variable_index=0, xy_cols=["longitude", "latitude"]):
    # Load data
    import xarray

    path = fused.download(path, path.split('/')[-1]) 
    ds = xarray.open_dataset(path, engine='h5netcdf')
    try:
        var = list(ds.data_vars)[variable_index]
        print(var)
        if coarsen_factor > 1:
            da = ds.coarsen(
                {xy_cols[0]: coarsen_factor, xy_cols[1]: coarsen_factor},
                boundary="trim",
            ).max()[var]
        else:
            da = ds[var]
        print("done")
        return da
    except Exception as e:
        print(e)
        ValueError()


def get_da_bounds(da, xy_cols=("longitude", "latitude"), pixel_position='center'):
    x_list = da[xy_cols[0]].values
    y_list = da[xy_cols[1]].values
    if pixel_position=='center':
        pixel_width = x_list[1] - x_list[0]
        pixel_height = y_list[1] - y_list[0]
        minx = x_list[0] - pixel_width / 2
        miny = y_list[-1] + pixel_height / 2
        maxx = x_list[-1] + pixel_width / 2
        maxy = y_list[0] - pixel_height / 2
        return (minx, miny, maxx, maxy)
    else:
        return (x_list[0], y_list[-1], x_list[-1], y_list[0])
        
        
def da_fit_to_resolution(da, target_shape):
    import numpy as np
    dims = da.dims
    new_coords = {
        dim: np.linspace(da[dim].min(), da[dim].max(), target_shape[i])
        for i, dim in enumerate(dims)
    }
    return da.interp(new_coords)    


def clip_arr(arr, bounds_aoi, bounds_total=(-180, -90, 180, 90)):
    # ToDo: fix antimeridian issue by spliting and merging arr around lng=360
    from rasterio.transform import from_bounds

    transform = from_bounds(*bounds_total, arr.shape[-1], arr.shape[-2])
    if bounds_total[2] > 180 and bounds_total[0] >= 0:
        print("Normalized longitude for bounds_aoi to (0,360) to match bounds_total")
        bounds_aoi = (
            (bounds_aoi[0] + 360) % 360,
            bounds_aoi[1],
            (bounds_aoi[2] + 360) % 360,
            bounds_aoi[3],
        )
    x_slice, y_slice = bbox_to_xy_slice(bounds_aoi, arr.shape, transform)
    arr_aoi = arr[y_slice, x_slice]
    if bounds_total[1] > bounds_total[3]:
        if len(arr_aoi.shape) == 3:
            arr_aoi = arr_aoi[:, ::-1]
        else:
            arr_aoi = arr_aoi[::-1]
    return arr_aoi


def visualize(
    data,
    mask: float | np.ndarray = None,
    min: float = 0,
    max: float = 1,
    opacity: float = 1,
    colormap = None,
):
    """Convert objects into visualization tiles."""
    import xarray as xr
    import palettable
    from matplotlib.colors import LinearSegmentedColormap
    from matplotlib.colors import Normalize   
    
    if data is None:
        return
    
    if colormap is None:
        # Set a default colormap
        colormap = palettable.colorbrewer.sequential.Greys_9_r
        cm = colormap.mpl_colormap
    elif isinstance(colormap, palettable.palette.Palette):
        cm = colormap.mpl_colormap
    elif isinstance(colormap, (list, tuple)):
        cm = LinearSegmentedColormap.from_list('custom', colormap)
    else:
        print('visualize: no type match for colormap')

    if isinstance(data, xr.DataArray):
        # Convert from an Xarray DataArray to a Numpy ND Array
        data = data.values

    if isinstance(data, np.ndarray):

        if isinstance(data, np.ma.MaskedArray):
            boolean_mask = data.mask
            if mask is None:
                mask = boolean_mask
            else:
                # Combine the two masks.
                mask = mask * boolean_mask
        
        norm_data = Normalize(vmin=min, vmax=max, clip=False)(data)
        mapped_colors = cm(norm_data)
        if isinstance(mask, (float, np.ndarray)):
            mapped_colors[:,:,3] = mask * opacity
        else:
            mapped_colors[:,:,3] = opacity
        
        # Convert to unsigned 8-bit ints for visualization.
        vis_dtype = np.uint8
        max_color_value = np.iinfo(vis_dtype).max
        norm_data255 = (mapped_colors * max_color_value).astype(vis_dtype)
        
        # Reshape array to 4 x nRow x nCol.
        shaped = norm_data255.transpose(2,0,1)
    
        return shaped
    else:
        print('visualize: data instance type not recognized')

    
class AsyncRunner:
    '''
    ## Usage example:
    async def fn(n): return n**2
    runner = AsyncRunner(fn, range(10))
    runner.get_result_now()
    '''
    def __init__(self, func, args_list, delay_second=0, verbose=True):
        import asyncio
        if isinstance(args_list, pd.DataFrame):
            self.args_list=args_list.T.to_dict().values()
        elif isinstance(args_list, list) or isinstance(args_list, range):     
            self.args_list=args_list
        else:
            raise ValueError('args_list need to be list, pd.DataFrame, or range')
        self.func = func
        self.verbose = verbose
        self.delay_second = delay_second
        self.loop = asyncio.get_running_loop()
        self.run_async()
    
    def create_task(self, args):
        import time
        import json
        time.sleep(self.delay_second)
        if type(args)==str:
            args=json.loads(args)
        if isinstance(args, dict):
            task = self.loop.create_task(self.func(**args))
        else:
            task = self.loop.create_task(self.func(args))
        task.set_name(json.dumps(args))
        return task
        
    def run_async(self):
        tasks = []
        for args in self.args_list:
            tasks.append(self.create_task(args))
        self.tasks=tasks
    
    def is_done(self):
        return [task.done() for task in self.tasks]
    
    def get_task_result(self, r):
        if r.done():
            import pandas as pd
            try:
                return r.result()
            except Exception as e:
                return str(e)
        else:
            return 'pending'
        
    def get_result_now(self, retry=True):
        if retry:
            self.retry()
        if self.verbose:
            print(f"{sum(self.is_done())} out of {len(self.is_done())} are done!")
        import json
        import pandas as pd
        df = pd.DataFrame([json.loads(task.get_name()) for task in self.tasks])
        df['result']= [self.get_task_result(task) for task in self.tasks]
        def fn(r):
            if type(r)==str:
                if r=='pending':
                    return 'running'
                else:
                    return 'faild'
            else:
                return 'done'
        df['status']=df['result'].map(fn)
        return df            
    
    def retry(self):
        def _retry_task(task, verbose):
            if task.done():
                task_exception = task.exception()
                if task_exception:
                    if verbose: print(task_exception)
                    return self.create_task(task.get_name()) 
                else:
                    return task
            else:
                return task            
        self.tasks = [_retry_task(task, self.verbose) for task in self.tasks]
    
    async def get_result_async(self):
        import asyncio
        return await asyncio.gather(*self.tasks)

    def __repr__(self):
        if self.verbose:
            print(f'tasks_done={self.is_done()}')
        if (sum(self.is_done())/len(self.is_done()))==1:
            return f"done!"
        else:
            return "running..."
    
class PoolRunner:
    '''
    ## Usage example:
    def fn(n): return n**2
    runner = PoolRunner(fn, range(10))
    runner.get_result_now()
    runner.get_result_all()
    '''
    def __init__(self, func, args_list, delay_second=0.01, verbose=True, max_retry=3):
        import asyncio
        import pandas as pd
        import concurrent.futures
        if isinstance(args_list, pd.DataFrame):
            self.args_list=args_list.T.to_dict().values()
        elif isinstance(args_list, list) or isinstance(args_list, range):     
            self.args_list=args_list
        else:
            raise ValueError('args_list need to be list, pd.DataFrame, or range')
        self.func = func
        self.delay_second = delay_second
        self.verbose = verbose
        self.max_retry=max_retry
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1024)
        self.run_pool()
        self.result=[]

    def create_task(self, args, n_retry):
        import time
        time.sleep(self.delay_second)
        if isinstance(args, dict):
            task = self.pool.submit(self.func, **args)
        else:
            task = self.pool.submit(self.func, args)
        return [task, args, n_retry]
        
    def run_pool(self):
        tasks = []
        for args in self.args_list:
            tasks.append(self.create_task(args, n_retry=0))
        self.tasks=tasks
    
    def is_success(self):
        return [task[0].done() for task in self.tasks]
    
    def get_task_result(self, task):
        if task[0].done() or task[2]>self.max_retry:
            try:
                return task[0].result()
            except Exception as e:
                if task[2]<self.max_retry: 
                    return str(e)
                else:
                    return f'Exceeded max_retry ({task[2]-1}|{self.max_retry}): '+str(e)
        else:
            # if task[2]<self.max_retry: 
                return f'pending'
            # else:
            #     return f'Exceeded max_retry'
        
    def get_result_now(self, retry=True, sleep_second=1):
        if retry:
            self.retry()
        else:
            import time
            time.sleep(sleep_second)
        if self.verbose:
            n1=sum(self.is_success())
            n2=len(self.is_success())
            print(f"\r{n1/n2*100:.1f}% ({n1}|{n2}) complete", end='\n')
        import json
        import pandas as pd
        df = pd.DataFrame([task[1] for task in self.tasks])
        self.result=[self.get_task_result(task) for task in self.tasks]
        df['result']=self.result
        def fn(r):
            if type(r)==str:
                if str.startswith(r, 'Exceeded max_retry'):
                    return 'error'
                else:#elif r=='pending':
                    return 'running'#'error_retry'
            else:
                return 'success'
        df['status']=df['result'].map(fn)
        return df            
    
    def retry(self):
        def _retry_task(task, verbose):
            if task[0].done():
                task_exception = task[0].exception()
                if task_exception:
                    if verbose: 
                        print(task_exception)
                    if (task[2]<self.max_retry):
                        if verbose: 
                            print(f'Retry {task[2]+1}|{self.max_retry} for {task[1]}.')
                        return self.create_task(task[1], n_retry=task[2]+1) 
                    else:
                        if verbose: 
                            print(f'Exceeded Max retry {self.max_retry} for {task[1]}.')
                        return [task[0], task[1], task[2]+1]
                else:
                    return task
            else:
                return task            
        self.tasks = [_retry_task(task, self.verbose) for task in self.tasks]
    
    def get_result_all(self, timeout=120):
        import time
        for i in range(timeout):
            df=self.get_result_now(retry=True)
            # if (df.status=='success').mean()==1:
            if (df.status=='running').mean()==0:
                break
            else:
                time.sleep(1)
        if self.verbose:
            print(f"Done!")
        return df

    def get_error(self, sleep_second=3):
        df=self.get_result_now(retry=False, sleep_second=sleep_second)
        if self.verbose:
            print('Status Summary:', df.status.value_counts().to_dict())
            error_mask=df.status=='error'
            if sum(error_mask)==0:
                print('No error.')
            else:
                df_error = df[error_mask]
                for i in range(len(df_error)): 
                    print(f'\nROW: {i} | ERROR {"="*30}')
                    for col in df_error.columns:  
                        print(f'{str(col).upper()}: {df_error.iloc[i][col]}')
        return df
    def get_concat(self, sleep_second=0, verbose=None):
        if verbose != None:
            self.verbose=verbose
        import pandas as pd
        df = self.get_error(sleep_second=sleep_second)
        mask=df.status=='success'
        if mask.sum()==0:
            return 
        else:
            results=[i for i in df[mask].result if i is not None]
            if self.verbose:
                print(f"{100*mask.mean().round(3)} percent success.")
                if len(results)!=mask.sum():
                    print(f"Warnning: {mask.sum()-len(results)}/{mask.sum()} are None values.")
        return pd.concat(results)
    def __repr__(self):
        if self.verbose:
            print(f'tasks_success={self.is_success()}')
        if (sum(self.is_success())/len(self.is_success()))==1:
            return f"success!"
        else:
            return "running..."
            
@fused.cache
def get_parquet_stats(path):
    import pyarrow.parquet as pq
    import pandas as pd
    # Load Parquet file
    parquet_file = pq.ParquetFile(path)
    
    # List to store the metadata for each row group
    stats_list = []

    # Iterate through row groups
    for i in range(parquet_file.num_row_groups):
        row_group = parquet_file.metadata.row_group(i)
        
        # Dictionary to store row group's statistics
        row_stats = {'row_group': i, 'num_rows': row_group.num_rows}
        
        # Iterate through columns and gather statistics
        for j in range(row_group.num_columns):
            column = row_group.column(j)
            stats = column.statistics
            
            if stats:
                col_name = column.path_in_schema
                row_stats[f"{col_name}_min"] = stats.min
                row_stats[f"{col_name}_max"] = stats.max
                row_stats[f"{col_name}_null_count"] = stats.null_count
        
        # Append the row group stats to the list
        stats_list.append(row_stats)
    
    # Convert the list to a DataFrame
    df_stats = pd.DataFrame(stats_list)
    
    return df_stats

@fused.cache
def get_row_groups(key, value, file_path):
    version='1.0'
    df = get_parquet_stats(file_path)[['row_group', key+'_min', key+'_max']]
    con = duckdb_connect()
    df = con.query(f'select * from df where {value} between {key}_min and {key}_max').df()
    return df.row_group.values

def read_row_groups(file_path, chunk_ids, columns=None):
    import pyarrow.parquet as pq   
    table=pq.ParquetFile(file_path)   
    if columns:
        return table.read_row_groups(chunk_ids, columns=columns).to_pandas()   
    else: 
        print('available columns:', table.schema.names)
        return table.read_row_groups(chunk_ids).to_pandas() 


def tiff_bbox(url):
    import rasterio
    import shapely
    import geopandas as gpd
    with rasterio.open(url) as dataset:
        gpd.GeoDataFrame(geometry=[shapely.box(*dataset.bounds)],crs=dataset.crs)
        return list(dataset.bounds)
    
def s3_to_https(path):
    arr = path[5:].split('/')
    out = 'https://'+arr[0]+'.s3.amazonaws.com/'+'/'.join(arr[1:])
    return out

def get_ip():
    import socket
    hostname=socket.gethostname()
    IPAddr=socket.gethostbyname(hostname)
    return IPAddr
    

def scipy_voronoi(gdf):
    """
    Scipy based Voronoi function. Built because fused version at time is on geopandas 0.14.4 which 
    doesnt' have `gdf.geometry.voronoi_polygons()`
    Probably not the most optimised funciton but it gets the job done. 
    Irrelevant once we move to geopandas 1.0+
    """
    from shapely.geometry import Polygon, Point
    from scipy.spatial import Voronoi

    points = np.array([(geom.x, geom.y) for geom in gdf.geometry])
    vor = Voronoi(points)
    
    polygons = []
    for region in vor.regions:
        if not region or -1 in region:  # Ignore regions with open boundaries
            continue
        
        # Get the vertices for the region and construct a polygon
        polygon = Polygon([vor.vertices[i] for i in region])
        polygons.append(polygon)

    voronoi_gdf = gpd.GeoDataFrame(geometry=polygons, crs=gdf.crs)
    return voronoi_gdf





def estimate_zoom(bounds, target_num_tiles=1):
    """
    Estimate the zoom level for a given bounding box.

    This method returns the zoom level at which a tile exists that, potentially
    shifted slightly, fully covers the bounding box.
    
    Args:
        bounds: A list of 4 coordinates (minx, miny, maxx, maxy), a
                GeoDataFrame or Shapely geometry, or a mercantile Tile.
        target_num_tiles: Target number of tiles to cover the bounds (default=1).
                      If 1, finds the zoom where a single tile covers the bounds.
                      If >1, estimates zoom to achieve approximately this many tiles.
    
    Returns:
        The estimated zoom level (0-20).
    """
    from fused._optional_deps import (
        GPD_GEODATAFRAME,
        HAS_GEOPANDAS,
        HAS_MERCANTILE,
        HAS_SHAPELY,
        MERCANTILE_TILE,
        SHAPELY_GEOMETRY,
    )

    # Process input bounds to get standard format
    if HAS_GEOPANDAS and isinstance(bounds, GPD_GEODATAFRAME):
        bounds = bounds.total_bounds
    elif HAS_SHAPELY and isinstance(bounds, SHAPELY_GEOMETRY):
        bounds = bounds.bounds
    elif HAS_MERCANTILE and isinstance(bounds, MERCANTILE_TILE):
        return bounds.z
    elif not isinstance(bounds, list):
        raise TypeError(f"Invalid bounds type: {type(bounds)}")

    if not HAS_MERCANTILE:
        raise ImportError("This function requires the mercantile package.")
    
    import mercantile
    import math
    

    if target_num_tiles == 1:
        minx, miny, maxx, maxy = bounds
        centroid = (minx + maxx) / 2, (miny + maxy) / 2
        width = (maxx - minx) - 1e-11
        height = (maxy - miny) - 1e-11
        
        for z in range(20, 0, -1):
            tile = mercantile.tile(*centroid, zoom=z)
            west, south, east, north = mercantile.bounds(tile)
            if width <= (east - west) and height <= (north - south):
                break
        return z
    

    else:
        minx, miny, maxx, maxy = bounds
        miny = max(miny,-89.9999993) #there is a bug in the mercentile that adds an epsilon to lat lngs and causes issue
        maxy = min(maxy,89.9999993) #there is a bug in the mercentile that adds an epsilon to lat lngs and causes issue
        max_zoom = 20
        x_min, y_min, _ = mercantile.tile(minx, maxy, max_zoom)
        x_max, y_max, _ = mercantile.tile(maxx, miny, max_zoom)
        delta_x = x_max - x_min + 1
        delta_y = y_max - y_min + 1

        zoom = math.log2(math.sqrt(target_num_tiles) / max(delta_x, delta_y)) + max_zoom
        zoom = int(math.floor(zoom)) 
        current_num_tiles = len(mercantile_polyfill(bounds, zooms=[zoom], compact=False))
        if current_num_tiles>=target_num_tiles:
            return zoom
        else:
            return zoom+1


def get_tiles(
    bounds=None, target_num_tiles=1, zoom=None, max_tile_recursion=6, as_gdf=True, clip=False, verbose=False
):
    bounds = to_gdf(bounds)
    import mercantile
    import geopandas as gpd
    import numpy as np

    if bounds.empty or bounds.geometry.isna().any() or len(bounds) == 0:
        if verbose:
            print("Warning: Empty or invalid bounds provided")
        return gpd.GeoDataFrame(columns=["geometry", "x", "y", "z"])

    if np.isnan(bounds.total_bounds).any():
        if verbose:
            print("Warning: Empty or invalid bounds provided")
        return gpd.GeoDataFrame(columns=["geometry", "x", "y", "z"])
    
    if zoom is not None:
        if verbose: 
            print("zoom is provided; target_num_tiles will be ignored.")
        target_num_tiles = None

    if target_num_tiles is not None and target_num_tiles < 1:
        raise ValueError("target_num_tiles should be more than zero.")

    if target_num_tiles == 1:
        
        tile = mercantile.bounding_tile(*bounds.total_bounds)
        if verbose: 
            print(to_gdf((0,0,0)))
        gdf = to_gdf((tile.x, tile.y, tile.z))
    else:
        zoom_level = (
            zoom
            if zoom is not None
            else estimate_zoom(bounds, target_num_tiles=target_num_tiles)
        )
        base_zoom = estimate_zoom(bounds, target_num_tiles=1)
        if zoom_level > (base_zoom + max_tile_recursion + 1):
            zoom_level = base_zoom + max_tile_recursion + 1
            if zoom:
                if verbose: 
                    print(
                    f"Warning: Maximum number of tiles is reached ({zoom=} > {base_zoom+max_tile_recursion+1=} tiles). Increase {max_tile_recursion=} to allow for deeper tile recursion"
                    )
            else:
                if verbose: 
                    print(
                    f"Warning: Maximum number of tiles is reached ({target_num_tiles} > {4**max_tile_recursion-1} tiles). Increase {max_tile_recursion=} to allow for deeper tile recursion"
                    )

        gdf = mercantile_polyfill(bounds, zooms=[zoom_level], compact=False)
        if verbose: 
            print(f"Generated {len(gdf)} tiles at zoom level {zoom_level}")

    if clip:
        return gdf.clip(bounds) if as_gdf else gdf[["x", "y", "z"]].values
    else:
        return gdf if as_gdf else gdf[["x", "y", "z"]].values

def get_utm_epsg(geometry):
    utm_zone = int((geometry.centroid.x + 180) / 6) + 1
    return 32600 + utm_zone if geometry.centroid.y >= 0 else 32700 + utm_zone  # 326XX for Northern Hemisphere, 327XX for Southern


def add_utm_area(gdf, utm_col='utm_epsg', utm_area_col='utm_area_sqm'):
    import geopandas as gpd

    # Step 1: Compute UTM zones
    gdf[utm_col] = gdf.geometry.apply(get_utm_epsg)

    # Step 2: Compute areas in batches while preserving order
    areas_dict = {}

    for utm_zone, group in gdf.groupby(utm_col, group_keys=False):
        utm_crs = f"EPSG:{utm_zone}"
        reprojected = group.to_crs(utm_crs)  # Reproject all geometries in batch
        areas_dict.update(dict(zip(group.index, reprojected.area)))  # Store areas by index

    # Step 3: Assign areas back to original gdf order
    gdf[utm_area_col] = gdf.index.map(areas_dict)
    return gdf


def run_submit_with_defaults(udf_token: str, cache_length: str = "9999d", default_params_token: Optional[str] = None):
    """
    Uses fused.submit() to run a UDF over:
    - A UDF that returns a pd.DataFrame of test arguments (`default_params_token`)
    - Or default params (expectes udf.utils.submit_default_params to return a pd.DataFrame)
    """
    
    # Assume people know what they're doing 
    try:
        # arg_token is a UDF that returns a pd.DataFrame of test arguments
        arg_list = fused.run(default_params_token)

        if 'bounds' in arg_list.columns:
            # This is a hacky workaround for now as we can't pass np.float bounds to `fused.run(udf, bounds) so need to convert them to float
            # but fused.run() returns bounds as `np.float` for whatever reason
            arg_list['bounds'] = arg_list['bounds'].apply(lambda bounds_list: [float(x) for x in bounds_list])
            
        print(f"Loaded default params from UDF {default_params_token}... Running UDF over these")
    except Exception as e:
        print(f"Couldn't load UDF {udf_token} with arg_token {default_params_token}, trying to load default params...")
        
        try:
            udf = fused.load(udf_token)
            
            # Assume we have a funciton called 'submit_default_params` inside the main UDF which returns a pd.DataFrame of test arguments
            # TODO: edit this to directly use `udf.submit_default_params()` once we remove utils
            if hasattr(udf.utils, "submit_default_params"):
                print("Found default params for UDF, using them...")
                arg_list = udf.utils.submit_default_params()
            else:
                raise ValueError("No default params found for UDF, can't run this UDF")

        except Exception as e:
            raise ValueError("Couldn't load UDF, can't run this UDF. Try with another UDF")
        
        #TODO: Add support for using the default view state

    return fused.submit(
        udf_token,
        arg_list,
        cache_max_age=cache_length,
        wait_on_results=True,
    )

def test_udf(udf_token: str, cache_length: str = "9999d", arg_token: Optional[str] = None):
    """
    Testing a UDF:
    1. Does it run and return successful result for all its default parameters?
    2. Are the results identical to the cached results?

    Returns:
    - all_passing: True if the UDF runs and returns successful result for all its default parameters
    - all_equal: True if the results are identical to the cached results
    - prev_run: Cached UDF output
    - current_run: New UDF output
    """
    import pickle

    cached_run = run_submit_with_defaults(udf_token, cache_length, arg_token)
    current_run = run_submit_with_defaults(udf_token, "0s", arg_token)

    # Check if results are valid
    all_passing = (current_run["status"] == "success").all()
    # Check if result matches cached result
    all_equal = pickle.dumps(cached_run) == pickle.dumps(current_run)
    return (bool(all_passing), all_equal, cached_run, current_run)

  
def save_to_agent(
    agent_json_path: str, udf: AnyBaseUdf, agent_name: str, udf_name: str, mcp_metadata: dict[str, Any], overwrite: bool = True,
):
    """
    Save UDF to agent of udf_ai directory
    Args:
        agent_json_path (str): Absolute path to the agent.json file
        agent_name (str): Name of the agent
        udf (AnyBaseUdf): UDF to save
        udf_name (str): Name of the UDF
        mcp_metadata (dict[str, Any]): MCP metadata
        overwrite (bool): If True, overwrites any existing UDF directory with current `udf`
    """
    # load agent.json
    if os.path.exists(agent_json_path):
        agent_json = json.load(open(agent_json_path))
    else:
        agent_json = {"agents": []}
    repo_dir = os.path.dirname(agent_json_path)

    # save udf to repo
    udf.metadata = {}
    udf.name = udf_name
    if not mcp_metadata.get("description") or not mcp_metadata.get("parameters"):
        raise ValueError("mcp_metadata must have description and parameters")
    udf.metadata["fused:mcp"] = mcp_metadata
    udf.to_directory(f"{repo_dir}/udfs/{udf_name}", overwrite=overwrite)

    if agent_name in [agent["name"] for agent in agent_json["agents"]]:
        for agent in agent_json["agents"]:
            if agent["name"] == agent_name:
                # Only append udf_name if it doesn't already exist in the agent's udfs list
                if udf_name not in agent["udfs"]:
                    agent["udfs"].append(udf_name)
                break
    else:
        agent_json["agents"].append({"name": agent_name, "udfs": [udf_name]})

    # save agent.json
    json.dump(agent_json, open(agent_json_path, "w"), indent=4)

def generate_local_mcp_config(config_path: str, agents_list: list[str], repo_path: str, uv_path: str = 'uv', script_path: str = 'main.py'):
    """
    Generate MCP configuration file based on list of agents from the udf_ai directory
    Args:
        config_path (str): Absolute path to the MCP configuration file.
        agents_list (list[str]): List of agent names to be included in the configuration.
        repo_path (str): Absolute path to the locally cloned udf_ai repo directory.
        uv_path (str): Path to `uv`. Defaults to `uv` but might require your local path to `uv`.
        script_path (str): Path to the script to run. Defaults to `run.py`.
    """
    if not os.path.exists(repo_path):
        raise ValueError(f"Repository path {repo_path} does not exist")

    # load agent.json containing agent to udfs mapping
    agent_json = json.load(open(f"{repo_path}/agents.json", "rt"))

    # create config json for all agents
    config_json = {"mcpServers": {}}

    for agent_name in agents_list:
        agent = next(
            (agent for agent in agent_json["agents"] if agent["name"] == agent_name),
            None,
        )
        if not agent or not agent["udfs"]:
            raise ValueError(f"No UDFs found for agent {agent_name}")

        agent_config = {
            "command": uv_path,
            "args": [
                "run",
                "--directory",
                f"{repo_path}",
                f"{script_path}",
                "--runtime=local",
                f"--udf-names={','.join(agent['udfs'])}",
                f"--name={agent_name}",
            ],
        }
        config_json["mcpServers"][agent_name] = agent_config

    # save config json
    json.dump(config_json, open(config_path, "w"), indent=4)

#TODO: fix dill issue in local machine
def func_to_udf(func, cache_max_age='12h'):
    import dill 
    from textwrap import dedent
    source_code = dill.source.getsource(func)
    udf = fused.load(f'@fused.udf(cache_max_age="{cache_max_age}")\n'+dedent(source_code))
    udf.entrypoint = func.__name__
    udf.name = func.__name__
    # print(source_code)
    return udf


def udf_to_json(udf):
    try:
        udf_nail_json = fused.load(udf).json()
    except:
        try:
            udf_nail_json = udf.json()
        except:
            udf_nail_json = func_to_udf(udf).json()
    return udf_nail_json

def get_query_embedding(client, query, model="text-embedding-3-large"):
    """Generate embeddings for a query using OpenAI API
    client = OpenAI(api_key=fused.secrets["my_fused_key"])"""
    embedding = list(map(float, client.embeddings.create(
        input=query, model=model
    ).data[0].embedding))
    return embedding

def query_to_params(query, num_match=1, rerank=True, embedding_path="s3://fused-users/fused/misc/embedings/CDL_crop_name.parquet"):
    import pandas as pd
    from openai import OpenAI
    
    print(f"this is the '{query}'")
    df_crops = pd.read_parquet(embedding_path)
    api_key = fused.secrets["my_fused_key"] 
    
    client = OpenAI(api_key=api_key)
    response = client.embeddings.create(input=query, model="text-embedding-3-large")
    query_embedding = response.data[0].embedding
    
    df_crops['similarity'] = df_crops['embedding'].apply(lambda e: cosine_similarity(query_embedding, e))
    
    if not rerank:
        results = df_crops.sort_values('similarity', ascending=False).head(num_match)
        print(results[['value', 'description']])
        return results['value'].tolist()
    
    candidates = df_crops.sort_values('similarity', ascending=False).head(10)
    
    try:
        items = "\n".join([f"{i}) Value: {row['value']}, Description: {row['description']}" 
                         for i, (_, row) in enumerate(candidates.iterrows(), 1)])
        
        response = client.chat.completions.create(
            model="gpt-4.1",
            messages=[{"role": "user", "content": 
                f"Query: {query}\nRate relevance (0-100):\n{items}\n\nComma-separated scores only:"}],
            temperature=0
        )
        
        scores = [int(s.strip()) for s in response.choices[0].message.content.split(',')]
        if len(scores) != len(candidates):
            raise ValueError(f"Got {len(scores)} scores for {len(candidates)} candidates")
            
        candidates.loc[:, 'rerank_score'] = scores
        relevant = candidates[candidates['rerank_score'] > 40].sort_values('rerank_score', ascending=False)
        
        if len(relevant) == 0 and len(candidates) > 0:
            relevant = candidates.sort_values('rerank_score', ascending=False).head(1)
            
        print(relevant[['value', 'description', 'rerank_score']])
        return relevant['value'].tolist()
        
    except Exception as e:
        print(f"Reranking failed: {e}, using embedding search")
        results = candidates.head(num_match)
        print(results[['value', 'description']])
        return results['value'].tolist()


def cosine_similarity(a, b):
    dot_product = sum(x*y for x, y in zip(a, b))
    norm_a = sum(x*x for x in a)**0.5
    norm_b = sum(y*y for y in b)**0.5
    return dot_product / (norm_a * norm_b)


def submit_job(udf, df_arg, cache_max_age='12h'):
    udf_nail_json = udf_to_json(udf)
    
    #TODO: fix dill issue in local machine
    # def runner(args: dict, udf_nail_json: str):
    #     udf_nail = fused.models.udf.udf.GeoPandasUdfV2.parse_raw(udf_nail_json)
    #     return fused.run(udf_nail, **args, engine="local")
    # runner = func_to_udf(runner)
    runner = fused.load(f"""@fused.udf(cache_max_age="{cache_max_age}")
def udf(args:dict, udf_nail_json:str):
    udf_nail = fused.models.udf.udf.GeoPandasUdfV2.parse_raw(udf_nail_json)
    return fused.run(udf_nail, ** args, engine='local')
    """)

    arg_list = df_arg.to_dict(orient="records")
    job = runner(arg_list=arg_list, udf_nail_json=udf_nail_json)
    return job

