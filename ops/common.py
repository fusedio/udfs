from typing import Literal, Optional, Sequence, Tuple, Union

import ee
import geopandas as gpd
import pyproj
from loguru import logger


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


def earth_session(cred):
    from job2.credentials import get_session
    from rasterio.session import AWSSession

    aws_session = get_session(
        cred["env"],
        earthdatalogin_username=cred["username"],
        earthdatalogin_password=cred["password"],
    )
    return AWSSession(aws_session, requester_pays=False)


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


def get_s3_list(path, suffix=None):
    import s3fs

    fs = s3fs.S3FileSystem()
    if suffix:
        return ["s3://" + i for i in fs.ls(path) if i[-len(suffix) :] == suffix]
    else:
        return ["s3://" + i for i in fs.ls(path)]


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


def crs_display(crs: pyproj.CRS) -> Union[int, pyproj.CRS]:
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


def resolve_crs(
    gdf: gpd.GeoDataFrame, crs: Union[pyproj.CRS, Literal["utm"]], verbose: bool = False
) -> gpd.GeoDataFrame:
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
    credentials = ee.ServiceAccountCredentials(service_account_name, key_path)
    ee.Initialize(
        opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials
    )
