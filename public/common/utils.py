# To use these functions, add the following command in your UDF:
# `common = fused.public.common`

from __future__ import annotations

import random
from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union

import ee
import fused
import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
import shapely
from affine import Affine
from loguru import logger
from numpy.typing import NDArray


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
            # No results at this area
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
        if clip:
            return df.clip(bbox).explode()
        else:
            return df


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
def geom_stats(gdf, arr, chip_len=255):
    import numpy as np

    df_3857 = gdf.to_crs(3857)
    df_tile = df_3857.dissolve()
    minx, miny, maxx, maxy = df_tile.total_bounds
    d = (maxx - minx) / chip_len
    transform = [d, 0.0, minx, 0.0, -d, maxy, 0.0, 0.0, 1.0]
    geom_masks = [
        rasterize_geometry(geom, arr.shape[-2:], transform) for geom in df_3857.geometry
    ]
    gdf["stats"] = [np.nanmean(arr.data[geom_mask]) for geom_mask in geom_masks]
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
        with rasterio.open(input_tiff_path, OVERVIEW_LEVEL=overview_level) as src:
            # with rasterio.Env():
            from rasterio.warp import Resampling, reproject

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
            d = (maxx - minx) / output_shape[-1]
            dst_transform = [d, 0.0, minx, 0.0, -d, maxy, 0.0, 0.0, 1.0]
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
        a.append(
            read_tiff(
                bbox=bbox,
                input_tiff_path=input_tiff_path,
                filter_list=filter_list,
                output_shape=output_shape,
                overview_level=overview_level,
                cred=cred,
            )
        )
    if len(a) == 1:
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


# url='https://raw.githubusercontent.com/stac-utils/stac-geoparquet/main/stac_geoparquet/stac_geoparquet.py'
# remove_strings=['from stac_geoparquet.utils import fix_empty_multipolygon']
# to_geodataframe = read_module(url,remove_strings)['to_geodataframe']

__all__ = (
    "geo_convert",
    "geo_buffer",
    "geo_bbox",
    "geo_join",
    "geo_distance",
    "geo_samples",
)


def get_geo_cols(data: gpd.GeoDataFrame) -> List[str]:
    """Get the names of the geometry columns.

    The first item in the result is the name of the primary geometry column. Following
    items are other columns with a type of GeoSeries.
    """
    main_col = data.geometry.name
    cols = [
        i for i in data.columns if (type(data[i]) == gpd.GeoSeries) & (i != main_col)
    ]
    return [main_col] + cols


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


def lonlat_to_geom(
    df: pd.DataFrame,
    cols_lonlat: Optional[Tuple[str, str]] = None,
    verbose: bool = False,
) -> NDArray[np.object_]:
    """Convert longitude-latitude columns to an array of shapely points."""
    if not cols_lonlat:
        cols_lonlat = infer_lonlat(list(df.columns))
        if not cols_lonlat:
            raise ValueError("no latitude and longitude columns were found.")

    assert cols_lonlat[0] in df.columns, f"column name {cols_lonlat[0]} was not found."
    assert cols_lonlat[1] in df.columns, f"column name {cols_lonlat[1]} was not found."

    if verbose:
        logger.debug(
            f"Converting {cols_lonlat} to points({cols_lonlat[0]},{cols_lonlat[0]})."
        )

    return shapely.points(
        list(zip(df[cols_lonlat[0]].astype(float), df[cols_lonlat[1]].astype(float)))
    )


def geo_convert(
    data: Union[pd.Series, pd.DataFrame, gpd.GeoSeries, gpd.GeoDataFrame],
    crs: Union[pyproj.CRS, Literal["utm"], None] = None,
    cols_lonlat=None,
    col_geom="geometry",
    verbose: bool = False,
) -> gpd.GeoDataFrame:
    """Convert input data into a GeoPandas GeoDataFrame."""
    if cols_lonlat:
        if isinstance(data, pd.Series):
            raise ValueError(
                "Cannot pass a pandas Series or a geopandas GeoSeries in conjunction "
                "with cols_lonlat."
            )

        shapely_points = lonlat_to_geom(data, cols_lonlat, verbose=verbose)
        gdf = gpd.GeoDataFrame(data, geometry=shapely_points, crs=4326)

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
            gdf = gpd.GeoDataFrame(
                data, geometry=lonlat_to_geom(data, cols_lonlat, verbose=verbose)
            ).set_crs(4326)
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
    data: gpd.GeoDataFrame,
    buffer_distance: Union[int, float] = 1000,
    utm_crs: Union[pyproj.CRS, Literal["utm"]] = "utm",
    dst_crs: Union[pyproj.CRS, Literal["original"]] = "original",
    col_geom_buff: str = "geom_buff",
    verbose: bool = False,
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
    data: gpd.GeoDataFrame,
    dst_crs: Union[Literal["utm"], pyproj.CRS, None] = None,
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
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
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
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    buffer_distance: Union[int, float, None] = None,
    utm_crs: Union[pyproj.CRS, Literal["utm"]] = "utm",
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
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    search_distance: Union[int, float] = 1000,
    utm_crs: Union[Literal["utm"], pyproj.CRS] = "utm",
    clip_bbox="left",
    col_distance: str = "distance",
    k_nearest: int = 1,
    cols_agg: Sequence[str] = ("fused_index",),
    cols_right: Sequence[str] = (),
    drop_extra_geoms: bool = True,
    verbose: bool = False,
) -> gpd.GeoDataFrame:
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
) -> gpd.GeoDataFrame:
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
            chunk_gdf.geometry = shapely.from_wkb(chunk_gdf.geometry)
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
    d = (maxx - minx) / arr.shape[-1:]
    transform = [d, 0.0, minx, 0.0, -d, maxy, 0.0, 0.0, 1.0]
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
    credentials = ee.ServiceAccountCredentials(service_account_name, key_path)
    ee.Initialize(
        opt_url="https://earthengine-highvolume.googleapis.com", credentials=credentials
    )


def run_pool(func, arg_list, max_workers=36):
    """
    Executes a given function on a list of arguments using a thread pool.

    Args:
        func (callable): The function to execute. It should take one argument.
        arg_list (list): A list of arguments to pass to the function.
        max_workers (int, optional): The maximum number of worker threads to use. Defaults to 36.

    Returns:
        list: A list of results from applying the function to each argument in arg_list.
    """
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        result = list(pool.map(func, arg_list))
    return result
