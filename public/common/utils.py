# To use these functions, add the following command in your UDF:
# `common = fused.public.common`
from __future__ import annotations
import fused
import pandas as pd
import numpy as np
from numpy.typing import NDArray
from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union
from loguru import logger

def chunkify(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

@fused.cache
def get_meta_datestr_chunk(base_path, start_year=2020, end_year=2024, n_chunks_datestr=90, total_row_groups=52, n_row_groups=2):
    import pandas as pd
    date_list = pd.date_range(start=f'{start_year}-01-01', end=f'{start_year+1}-01-01').strftime('%Y-%m-%d').tolist()[:-1]
    df = pd.DataFrame([[i[0],i[-1]] for i in chunkify(date_list,n_chunks_datestr)], columns=['start_datestr','end_datestr'])
    df['row_group_ids']=[chunkify(range(total_row_groups),n_row_groups)]*len(df)
    df = df.explode('row_group_ids').reset_index(drop=True)
    df['path'] = df.apply(lambda row:f"{base_path.strip('/')}/file_{row.start_datestr.replace('-','')}_{row.end_datestr.replace('-','')}_{row.row_group_ids[0]}_{row.row_group_ids[-1]}.parquet", axis=1)
    df['idx'] = df.index
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

def dilate_bbox(bbox, chip_len, border_pixel):
    bbox_crs = bbox.crs
    clipped_chip=chip_len-(border_pixel*2)
    bbox = bbox.to_crs(bbox.estimate_utm_crs())
    length = bbox.area[0]**0.5
    buffer_ratio = (chip_len-clipped_chip)/clipped_chip
    buffer_distance=length*buffer_ratio/2
    bbox.geometry = bbox.buffer(buffer_distance)
    bbox = bbox.to_crs(bbox_crs)  
    return bbox

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
def get_url_aws_stac(bbox, collections=["cop-dem-glo-30"]):
    import pystac_client
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    items = catalog.search(
        collections=collections,
        bbox=bbox.total_bounds,
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


def read_tiff_pc(bbox, tiff_url, cache_id=2):
    tiff_url = f"{tiff_url}?{get_pc_token(tiff_url,_cache_id=cache_id)['token']}"
    print(tiff_url)
    arr = read_tiff(bbox, tiff_url)
    return arr, tiff_url


@fused.cache(path="table_to_tile")
def table_to_tile(
    bbox,
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

    try:
        x, y, z = bbox[["x", "y", "z"]].iloc[0]
        if print_xyz:
            print(x, y, z)
    except:
        z = min_zoom
    df = fused.get_chunks_metadata(table)
    if len(bbox) > 1:
        bbox = bbox.dissolve().reset_index(drop=True)
    else:
        bbox = bbox.reset_index(drop=True)
    df = df[df.intersects(bbox.geometry[0])]
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
        df = rows_df[rows_df.intersects(bbox.geometry[0])]
        df.crs = bbox.crs
        if (
            z < min_zoom + centorid_zoom_offset
        ):  # switch to centroid for the last one zoom level before showing metadata
            df.geometry = df.geometry.centroid
        if clip:
            return df.clip(bbox).explode()
        else:
            return df
    else:
        df.crs = bbox.crs
        if clip:
            return df.clip(bbox).explode()
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
    bbox,
    input_tiff_path,
    filter_list=None,
    output_shape=(256, 256),
    overview_level=None,
    return_colormap=False,
    return_transform=False,
    cred=None,
):
    import os
    from contextlib import ExitStack

    import numpy as np
    import rasterio
    from rasterio.warp import Resampling, reproject
    from scipy.ndimage import zoom

    version = "0.2.0"

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

                bbox = bbox.to_crs(3857)
                # transform_bounds = rasterio.warp.transform_bounds(3857, src.crs, *bbox["geometry"].bounds.iloc[0])
                window = src.window(*bbox.to_crs(src.crs).total_bounds)
                original_window = src.window(*bbox.to_crs(src.crs).total_bounds)
                gridded_window = rasterio.windows.round_window_to_full_blocks(
                    original_window, [(1, 1)]
                )
                window = gridded_window  # Expand window to nearest full pixels
                source_data = src.read(window=window, boundless=True, masked=True)
                nodata_value = src.nodatavals[0]
                if filter_list:
                    mask = np.isin(source_data, filter_list, invert=True)
                    source_data[mask] = 0
                src_transform = src.window_transform(window)
                src_crs = src.crs
                minx, miny, maxx, maxy = bbox.total_bounds
                dx = (maxx - minx) / output_shape[-1]
                dy = (maxy - miny) / output_shape[-2]
                dst_transform = [dx, 0.0, minx, 0.0, -dy, maxy, 0.0, 0.0, 1.0]
                if len(source_data.shape) == 3 and source_data.shape[0] > 1:
                    dst_shape = (source_data.shape[0], output_shape[-2], output_shape[-1])
                else:
                    dst_shape = output_shape
                dst_crs = bbox.crs

                destination_data = np.zeros(dst_shape, src.dtypes[0])
                if return_colormap:
                    colormap = src.colormap(1)
        except rasterio.RasterioIOError as err:
            print(f"Caught RasterioIOError {err=}, {type(err)=}")
            return  # Return without data
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise
        
        reproject(
            source_data,
            destination_data,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            # TODO: rather than nearest, get all the values and then get pct
            resampling=Resampling.nearest,
        )
        destination_data = np.ma.masked_array(
            destination_data, destination_data == nodata_value
        )
    if return_colormap:
        # todo: only set transparency to zero
        colormap[0] = [0, 0, 0, 0]
        return destination_data, colormap
    elif (
        return_transform
    ):  # Note: usually you do not need this since it can be calculated using crs=4326 and bounds
        return destination_data, dst_transform
    else:
        return destination_data


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
    bbox,
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
            bbox=bbox,
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


def geo_convert(
    data,
    crs=None,
    cols_lonlat=None,
    col_geom="geometry",
    verbose: bool = False,
):
    """Convert input data into a GeoPandas GeoDataFrame."""
    import geopandas as gpd
    import shapely
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
    ), "no crs was not found. use geo_convert to add crs"
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
        return geo_convert(
            shapely.geometry.box(*data.total_bounds), crs=src_crs, verbose=verbose
        )
    elif str(dst_crs).lower() == "utm":
        dst_crs = data.estimate_utm_crs()
        logger.debug(f"estimated dst_crs={crs_display(dst_crs)}")
    transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    dst_bounds = transformer.transform_bounds(*data.total_bounds)
    return geo_convert(
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
            # transform bbox to utm & buffer & then to df2_crs
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
        left = geo_convert(left, verbose=verbose)
    if type(right) != gpd.GeoDataFrame:
        right = geo_convert(right, verbose=verbose)
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
        left = geo_convert(left, verbose=verbose)
    if type(right) != gpd.GeoDataFrame:
        right = geo_convert(right, verbose=verbose)
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
    return geo_convert(pd.DataFrame(points, columns=["lng", "lat"]))[["geometry"]]


def bbox_stac_items(bbox, table):
    import fused
    import geopandas as gpd
    import pandas as pd
    import pyarrow.parquet as pq
    import shapely

    df = fused.get_chunks_metadata(table)
    df = df[df.intersects(bbox)]
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
    ret_gdf = ret_gdf[ret_gdf.intersects(bbox)]
    return ret_gdf


# todo: switch to read_tiff with requester_pays option
def read_tiff_naip(
    bbox, input_tiff_path, crs, buffer_degree, output_shape, resample_order=0
):
    from io import BytesIO

    import rasterio
    from rasterio.session import AWSSession
    from scipy.ndimage import zoom

    out_buf = BytesIO()
    with rasterio.Env(AWSSession(requester_pays=True)):
        with rasterio.open(input_tiff_path) as src:
            if buffer_degree != 0:
                bbox.geometry = bbox.geometry.buffer(buffer_degree)
            bbox_projected = bbox.to_crs(crs)
            window = src.window(*bbox_projected.total_bounds)
            data = src.read(window=window, boundless=True)
            zoom_factors = np.array(output_shape) / np.array(data[0].shape)
            rgb = np.array(
                [zoom(arr, zoom_factors, order=resample_order) for arr in data]
            )
        return rgb


def image_server_bbox(
    image_url,
    bbox=None,
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

        gdf = gpd.GeoDataFrame(geometry=[shapely.box(*bbox)], crs=bbox_crs).to_crs(
            image_crs
        )
        print(gdf)
        minx, miny, maxx, maxy = gdf.total_bounds
    else:
        minx, miny, maxx, maxy = list(bbox)
    image_url = image_url.strip("/")
    url_template = f"{image_url}?f=image"
    url_template += f"&bbox={minx},{miny},{maxx},{maxy}"
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
def run_pool_tiffs(bbox, df_tiffs, output_shape):
    import numpy as np

    columns = df_tiffs.columns

    @fused.cache
    def fn_read_tiff(tiff_url, bbox=bbox, output_shape=output_shape):
        read_tiff = fused.load(
            "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
        ).utils.read_tiff
        return read_tiff(bbox, tiff_url, output_shape=output_shape)

    tiff_list = []
    for band in columns:
        for i in range(len(df_tiffs)):
            tiff_list.append(df_tiffs[band].iloc[i])

    arrs_tmp = fused.utils.common.run_pool(fn_read_tiff, tiff_list)
    arrs_out = np.stack(arrs_tmp)
    arrs_out = arrs_out.reshape(
        len(columns), len(df_tiffs), output_shape[0], output_shape[1]
    )
    return arrs_out


def search_pc_catalog(
    bbox,
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
        bbox=bbox.total_bounds,
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


def duckdb_connect(home_directory='/tmp/'):
    import duckdb 
    con = duckdb.connect()
    con.sql(
    f"""SET home_directory='{home_directory}';
    INSTALL h3 FROM community;
    LOAD h3;
    install 'httpfs';
    load 'httpfs';
    INSTALL spatial;
    LOAD spatial;
    """)
    print("duckdb version:", duckdb.__version__)
    return con


# @fused.cache
def run_query(query, return_arrow=False):
    con = duckdb_connect()
    if return_arrow:
        return con.sql(query).fetch_arrow_table()
    else:
        return con.sql(query).df()


def ds_to_tile(ds, variable, bbox, na_values=0):
    da = ds[variable]
    x_slice, y_slice = bbox_to_xy_slice(
        bbox.total_bounds, ds.rio.shape, ds.rio.transform()
    )
    window = bbox_to_window(bbox.total_bounds, ds.rio.shape, ds.rio.transform())
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
    data = da.isel(x=x_slice, y=y_slice).fillna(0)
    data = data.pad(
        x=(px0, px1), y=(py0, py1), mode="constant", constant_values=na_values
    )
    return data


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

    tile_list = list(mercantile.tiles(*geom.bounds, zooms=zooms))
    gdf_tiles = gpd.GeoDataFrame(
        tile_list,
        geometry=[shapely.box(*mercantile.bounds(i)) for i in tile_list],
        crs=4326,
    )
    gdf_tiles_intersecting = gdf_tiles[gdf_tiles.intersects(geom)]
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
    def __init__(self, func, args_list, delay_second=0.01, verbose=True):
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
        self.verbose = verbose
        self.delay_second = delay_second
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1024)
        self.run_pool()
        self.result=[]

    def create_task(self, args):
        import time
        time.sleep(self.delay_second)
        if isinstance(args, dict):
            task = self.pool.submit(self.func, **args)
        else:
            task = self.pool.submit(self.func, args)
        return [task, args]
        
    def run_pool(self):
        tasks = []
        for args in self.args_list:
            tasks.append(self.create_task(args))
        self.tasks=tasks
    
    def is_done(self):
        return [task[0].done() for task in self.tasks]
    
    def get_task_result(self, task):
        if task[0].done():
            try:
                return task[0].result()
            except Exception as e:
                return str(e)
        else:
            return 'pending'
        
    def get_result_now(self, retry=True):
        if retry:
            self.retry()
        if self.verbose:
            n1=sum(self.is_done())
            n2=len(self.is_done())
            print(f"\r{n1/n2*100:.1f}% ({n1}|{n2}) complete", end='')
        import json
        import pandas as pd
        df = pd.DataFrame([task[1] for task in self.tasks])
        self.result=[self.get_task_result(task) for task in self.tasks]
        df['result']=self.result
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
            if task[0].done():
                task_exception = task[0].exception()
                if task_exception:
                    if verbose: print(task_exception)
                    return self.create_task(task[1]) 
                else:
                    return task
            else:
                return task            
        self.tasks = [_retry_task(task, self.verbose) for task in self.tasks]
    
    def get_result_all(self, timeout=120):
        import time
        for i in range(timeout):
            df=self.get_result_now(retry=True)
            if (df.status=='done').mean()==1:
                break
            else:
                time.sleep(1)
        if self.verbose:
            print(f"Done!")
        return df

    def __repr__(self):
        if self.verbose:
            print(f'tasks_done={self.is_done()}')
        if (sum(self.is_done())/len(self.is_done()))==1:
            return f"done!"
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
    df = fused.utils.common.get_parquet_stats(file_path)[['row_group', key+'_min', key+'_max']]
    con = fused.utils.common.duckdb_connect()
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