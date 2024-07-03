import sys
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import fused
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.transform
import shapely
import xarray
from affine import Affine
from common import get_pc_token
from fused.api import FusedAPI
from loguru import logger
from scipy.ndimage import zoom
from scipy.spatial import KDTree
from shapely.geometry import box


def url_to_arr(url, return_colormap=False):
    from io import BytesIO

    import rasterio
    import requests
    from rasterio.plot import show

    response = requests.get(url)
    print(response.status_code)
    with rasterio.open(BytesIO(response.content)) as dataset:
        if return_colormap:
            return dataset.read(), dataset.colormap(1)
        else:
            return dataset.read()


def read_tiff_pc(bbox, tiff_url, cache_id=2):
    tiff_url = f"{tiff_url}?{get_pc_token(tiff_url,_cache_id=cache_id)['token']}"
    print(tiff_url)
    arr = read_tiff(bbox, tiff_url)
    return arr, tiff_url


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


def rio_distance(
    dataset: xarray.Dataset,
    variable: str,
    suffix: str = "_distance",
    coords_x: str = "x",
    coords_y: str = "y",
    chunk_size: int = 500_000,
    verbose: bool = False,
) -> xarray.Dataset:
    df = dataset[variable].to_dataframe().reset_index()
    df2 = df[df[variable] == 1]
    if len(df2) == 0:
        return xarray.DataArray(
            dataset[variable].values + np.inf,
            name=variable + suffix,
            coords=dataset[variable].coords,
        )
    tree = KDTree(np.vstack(list(zip(df2[coords_x].values, df2[coords_y].values))))
    df["dist"] = 0
    n_chunk = (len(df) // chunk_size) + 1
    for i in range(n_chunk):
        if verbose:
            sys.stdout.write(f"\r{i+1} of {n_chunk}")
        tmp = df.iloc[i * chunk_size : (i + 1) * chunk_size]
        distances, indices = tree.query(
            np.vstack(list(zip(tmp[coords_x].values, tmp[coords_y].values))), k=1
        )
        df.iloc[
            i * chunk_size : (i + 1) * chunk_size, -1
        ] = distances  # [:,-1] guarantees [:,'dist']
    if verbose:
        print(" done!")
    return xarray.DataArray(
        df.dist.values.reshape(dataset[variable].shape),
        name=variable + suffix,
        coords=dataset[variable].coords,
    )


def rio_resample(
    da1, da2, method="linear", reduce="mean", factor=None, order=1, verbose=True
):
    if da1.shape[0] >= da2.shape[0]:
        if not factor:
            factor = int(np.ceil(da1.shape[0] / da2.shape[0]))
        if verbose:
            print(f"downsampling: {reduce=}, {factor=}")
        return (
            da1.interp(
                x=zoom(da2.x, factor, order=order),
                y=zoom(da2.y, factor, order=order),
                method=method,
            )
            .coarsen(x=factor, y=factor)
            .reduce(reduce)
        )
    else:
        if verbose:
            print("upsampling: reduce & factor is not used")
        return da1.interp_like(da2, method="linear")


def rio_super_resolution(da, factor=2, dims=("x", "y"), method="linear", order=1):
    return da.interp({d: zoom(da[d], factor, order=order) for d in dims}, method=method)


def rio_scale_shape_transform(shape, transform, factor=10):
    (
        pixel_width,
        row_rot,
        x_upper_left,
        col_rot,
        pixel_height,
        y_upper_left,
    ) = transform[:6]
    transform = Affine(
        pixel_width / factor,
        row_rot,
        x_upper_left,
        col_rot,
        pixel_height / factor,
        y_upper_left,
    )
    shape = list(shape[-2:])
    shape[0] = shape[0] * factor
    shape[1] = shape[1] * factor
    return shape, transform


def netcdf_to_tiff(
    inpath: str,
    outpath,
    variable,
    transform: Union[Sequence[float], Affine, None] = None,
    crs=None,
    xy_dims=None,
    driver: Optional[str] = "COG",
) -> None:
    """Convert NetCDF to GeoTIFF

    Args:
        inpath: The source NetCDF file to load.
        outpath: The output location of the new GeoTIFFs.
        variable: The variable of the input dataset to write as output.
        transform: The affine transformation matrix of the data to include when writing. Defaults to None.
        crs: The Coordinate Reference System to write on the data. Defaults to None.
        xy_dims: _description_. Defaults to None.
        driver: The GDAL driver to use when saving data. Defaults to "COG".
    """

    try:
        import rioxarray

        _has_rio_xarray = True
    except ImportError:
        _has_rio_xarray = False

    def _read_with_xarray():
        logger.debug("Opening using open_dataset")
        return xarray.open_dataset(inpath, decode_coords="all")[variable]

    if _has_rio_xarray:
        try:
            da = rioxarray.open_rasterio(inpath, decode_coords="all")[variable]
            logger.debug("Opening using open_rasterio")
        except:  # noqa: E722
            da = _read_with_xarray()
    else:
        da = _read_with_xarray()
    if xy_dims:
        da = da.rio.set_spatial_dims(*xy_dims)
    if crs:
        da = da.rio.write_crs(crs)
    if da.rio.crs is None:
        raise ValueError("No crs found. Please manually specify the crs.")
    if transform:
        if isinstance(transform, Sequence):
            transform = rasterio.transform.Affine(*transform)
        da = da.rio.write_transform(transform)
    da.rio.to_raster(outpath, driver=driver)
    da.close()


# raster numpy


def fetch_chip(
    fused_index: int,
    geometry: BaseGeometry,
    href: str,
    *,
    asset_name: Optional[str] = None,
    datetime=None,
    api: Optional[FusedAPI] = None,
) -> ChipResponse:
    """Fetch a single raster chip given a geometry and GeoTIFF

    Args:
        fused_index: An integer derived from the `fused_index` column in the sample data.
        geometry: A Shapely geometry instance, in the same coordinate system as the GeoTIFF image.
        href: The url to a GeoTIFF to load.

    Keyword Args:
        asset_name: The name of the asset. This is returned  Defaults to None.
        datetime: The date time and which . Defaults to None.
        api: An instance of FusedAPI. Defaults to None, in which case it uses the global API.

    Returns:
        A dictionary with a single chip of data.
    """
    if not api:
        api = get_api()

    return api._fetch_row(
        fused_index=fused_index,
        geometry=geometry,
        href=href,
        asset_name=asset_name,
        datetime=datetime,
    )


def get_raster_numpy(
    left,
    right,
    *,
    asset_names: Sequence[str],
    n_rows: Optional[int] = None,
    prefetch_entire_files: bool = False,
    buffer: Optional[float] = None,
    href_modifier: Optional[Callable[[str], str]] = None,
    href_mapping: Optional[Dict[str, str]] = None,
    additional_columns: Optional[Sequence[str]] = None,
    context: None = None,
    api: Optional[FusedAPI] = None,
    load_chips: bool = True,
    serialize_outputs: bool = False,
    write_sidecar_zip: bool = False,
    output=None,
    _fetch_all_at_once: bool = False,
    **kwargs,
) -> Iterator[Dict[str, Any]]:
    """Get a raster as a numpy array

    Args:
        left: The input join object with geometries.
        right: The input join object with STAC data.

    Keyword Args:
        asset_names: The name(s) of assets in the STAC data to use.
        n_rows: A limit to the number of rows used from the left side. Defaults to None.
        prefetch_entire_files: If True, on the backend will fetch entire files to the local machine instead of doing partial reads from S3. Defaults to False.
        buffer: If set, will buffer the data by this amount (in meters) before fetching the raster data. Note that this buffer happens after the join between left and right. Defaults to None.
        href_modifier: A function to modify asset hrefs before fetching. Defaults to None.
        href_mapping: A dictionary from `str` to `str` that specifies how to change asset hrefs before fetching data. Note that `href_modifier` is applied after `href_mapping`. Defaults to None.
        additional_columns: Additional column names from the input DataFrame to add onto the raster results. Defaults to None.
        context: Unused in the local `fused` version. On the server this has context information. Defaults to None.
        api: An instance of `FusedAPI` to use for interacting with the backend. Defaults to None.
        load_chips: If False, will not fetch any data. Defaults to True.
        serialize_outputs: If True, will include `crs`, `transform`, `datetime`, `projected_geom`, and `shape` onto each row. Defaults to False.
        write_sidecar_zip: This is used for writing chips to sidecar, but only on the server side. Defaults to False.
        output: This is used for writing chips to sidecar, but only on the server side. Defaults to None.
        _fetch_all_at_once: If True, will parallelize downloads. Defaults to False.

    Yields:
        A dictionary of records with each row's information.
    """
    # write_sidecar_zip is unused here
    # context is unused here
    # prefetch_entire_files is unused here
    if type(prefetch_entire_files) != bool:
        warnings.warn("prefetch_entire_files has an invalid type (not bool)")

    if write_sidecar_zip and output is None:
        raise TypeError("Pass `output=output` when using write_sidecar_zip")

    left_gdf = left.data[:n_rows]
    right_gdf = right.data
    df = gpd.sjoin(left_gdf, right_gdf.drop(columns=["fused_index"]))

    if not asset_names:
        # TODO: Do we want to sniff the asset names or not?
        if not len(df):
            return
        row0 = df.iloc[0]
        asset_names = row0["assets"].keys()

    if href_mapping:
        orig_href_modifier = href_modifier

        def _href_mapping_modifier(url: str) -> str:
            url = href_mapping.get(url, url)
            return orig_href_modifier(url) if orig_href_modifier is not None else url

        href_modifier = _href_mapping_modifier

    if buffer:
        df = df.copy()
        df.geometry = df.geometry.buffer(buffer)

    def _get_single_row(*, asset_name: str, row: pd.Series) -> Dict:
        datetime = None
        if "datetime" in row:
            datetime = row["datetime"]

        href = row["assets"][asset_name]["href"]
        if href_modifier:
            href = href_modifier(href)

        fetch_chip_results = fetch_chip(
            fused_index=row["fused_index"],
            geometry=row["geometry"],
            href=href,
            asset_name=asset_name,
            datetime=datetime,
            api=api,
        )

        additional_kws = {}
        if additional_columns:
            for col in additional_columns:
                # If the name is provided by fetch_chip,
                # we won't find it on the original row anyways
                if col not in fetch_chip_results:
                    additional_kws[col] = row[col]

        result = {
            **additional_kws,
            **fetch_chip_results,
        }

        # TODO: We do not actually write the chips locally
        result["chip_path"] = "sample value (will be a file path when run)"

        if serialize_outputs:
            # TODO: We do not actually write the chips locally
            # result["chip_path"] = str(result["chip_path"])
            result["crs"] = str(result["crs"])
            result["transform"] = json.dumps(result["transform"])
            result["datetime"] = str(result["datetime"])
            result["projected_geom"] = shapely.to_wkb(result["projected_geom"])
            result["shape"] = json.dumps(result["shape"])

        if not load_chips:
            # This is a fix-up to match what the backend will do
            result.pop("array_data")
            result.pop("array_mask")

        return result

    if _fetch_all_at_once:
        with ThreadPoolExecutor(max_workers=OPTIONS.max_workers) as pool:
            futures: List[Future] = []
            for asset_name in asset_names:
                for _, row in df.iterrows():
                    futures.append(
                        pool.submit(_get_single_row, asset_name=asset_name, row=row)
                    )
            for future in futures:
                yield future.result()
    else:
        for asset_name in asset_names:
            for _, row in df.iterrows():
                result = _get_single_row(asset_name=asset_name, row=row)
                yield result


def get_raster_numpy_grouped(
    left,
    right,
    *,
    group_by: Sequence[str],
    asset_names: Sequence[str],
    n_rows: Optional[int] = None,
    prefetch_entire_files: bool = False,
    buffer: Optional[float] = None,
    href_modifier: Optional[Callable[[str], str]] = None,
    href_mapping: Optional[Dict[str, str]] = None,
    context: None = None,
    api: Optional[FusedAPI] = None,
    load_chips: bool = True,
    serialize_outputs: bool = False,
    write_sidecar_zip: bool = False,
    output=None,
    _fetch_all_at_once: bool = True,
    **kwargs,
) -> Iterable[pd.DataFrame]:
    """Fetch rasters and then group by columns

    Args:
        left: The input join object with geometries.
        right: The input join object with STAC data.

    Keyword Args:
        group_by: The column(s) to group on.
        asset_names: The name(s) of assets in the STAC data to use.
        n_rows: A limit to the number of rows used from the left side. Defaults to None.
        prefetch_entire_files: If True, on the backend will fetch entire files to the local machine instead of doing partial reads from S3. Defaults to False.
        buffer: If set, will buffer the data by this amount (in meters) before fetching the raster data. Note that this buffer happens after the join between left and right. Defaults to None.
        href_modifier: A function to modify asset hrefs before fetching. Defaults to None.
        href_mapping: A dictionary from `str` to `str` that specifies how to change asset hrefs before fetching data. Note that `href_modifier` is applied after `href_mapping`. Defaults to None.
        additional_columns: Additional column names from the input DataFrame to add onto the raster results. Defaults to None.
        context: Unused in the local `fused` version. On the server this has context information. Defaults to None.
        api: An instance of `FusedAPI` to use for interacting with the backend. Defaults to None.
        load_chips: If False, will not fetch any data. Defaults to True.
        serialize_outputs: If True, will include `crs`, `transform`, `datetime`, `projected_geom`, and `shape` onto each row. Defaults to False.
        write_sidecar_zip: This is used for writing chips to sidecar, but only on the server side. Defaults to False.
        output: This is used for writing chips to sidecar, but only on the server side. Defaults to None.
        _fetch_all_at_once: If True, will parallelize downloads. Defaults to False.

    Yields:
        DataFrames with raster values per group of `group_by` columns
    """
    records = list(
        get_raster_numpy(
            left=left,
            right=right,
            asset_names=asset_names,
            n_rows=n_rows,
            prefetch_entire_files=prefetch_entire_files,
            buffer=buffer,
            href_modifier=href_modifier,
            href_mapping=href_mapping,
            additional_columns=group_by,
            context=context,
            api=api,
            load_chips=load_chips,
            serialize_outputs=serialize_outputs,
            write_sidecar_zip=write_sidecar_zip,
            output=output,
            _fetch_all_at_once=_fetch_all_at_once,
            **kwargs,
        )
    )
    out_df = pd.DataFrame.from_records(records)
    # Spread into list as group_by may not be a tuple
    groups = out_df.groupby([*group_by], group_keys=True)
    for group in groups:
        # Return the first group's DataFrame (not the ID in group[0])
        yield group[1]


def get_raster_numpy_from_chips(
    input, sidecar_table_name: str, n_rows: Optional[int] = None, **kwargs
):
    """Get numpy array from chips

    Args:
        input: The input join object with left being geometries and right being STAC data.
        sidecar_table_name: The name of the table in `input` that contains chips as its sidecar file.
        n_rows: The number of chips to load from the sidecar. Defaults to None, which loads all chips.

    Yields:
        Dictionaries that contain chip data.
    """
    # input.data is copied so that we don't permanently modify the types,
    # and this function can be run on the same sample again
    df = input.data.copy()
    df["shape"] = df["shape"].map(json.loads)
    df["projected_geom"] = df["projected_geom"].map(shapely.from_wkb)

    def _load_transform(x: Optional[str]) -> Affine:
        if not x:
            return None
        obj = json.loads(x)
        if not obj:
            return None
        return Affine(*obj)

    df["transform"] = df["transform"].map(_load_transform)

    with BytesIO(input.sidecar[sidecar_table_name]) as bio:
        with ZipFile(bio, mode="r") as zf:
            len_rows = len(df)
            if n_rows is not None:
                len_rows = min(n_rows, len_rows)
            for i in range(len_rows):
                row = df.iloc[i].copy()
                if row["chip_path"]:
                    with zf.open(row["chip_path"], mode="r") as zfio:
                        npz_data = np.load(zfio)
                        row["array_data"] = npz_data["data"]
                        row["array_mask"] = npz_data["mask"]

                        yield row.to_dict()


# raster xarray


def shape_to_xy(
    shape: Union[Tuple[int, int], Tuple[int, int, int]], transform: Affine
) -> Tuple[List[float], List[float]]:
    """Convert a 2D or 3D shape to arrays of x and y coordinates

    Args:
        shape: The 2-dimensional or 3-dimensional shape of the image. Either (rows, columns) or (bands, rows, columns).
        transform: An Affine transform describing the given image.

    Returns:
        A two-tuple each containing a list of floats. The first represents the x coordinates and the second the y coordinates.
    """
    # todo: handle 3 bands and their name
    height = shape[-2]
    width = shape[-1]
    dx = transform[0]
    dy = transform[4]
    x_coor = transform[2]
    y_coor = transform[5]

    # Note: this is half a pixel so that the x and y coordinates are in the centers of
    # each pixel
    x_pixel_offset = dx / 2
    y_pixel_offset = dy / 2

    x = [x_coor + (i * dx) + x_pixel_offset for i in range(width)]
    y = [y_coor + (j * dy) + y_pixel_offset for j in range(height)]
    return x, y


def rows_to_xarray(
    rows: pd.DataFrame,
    group_by: Sequence[str] = ("asset_name"),
    dim_col: str = "datetime",
    sort_by_dim: bool = True,
    mosaic_cols: Optional[Iterable[str]] = None,
    merge: Optional[Dict[str, Any]] = {"compat": "override"},
) -> Union[xarray.Dataset, List[xarray.Dataset]]:
    """Convert raster chips to an xarray Dataset

    Args:
        rows: A DataFrame of raster chips to convert to xarray
        group_by: The column(s) in `rows` to group on. Defaults to ("asset_name").
        dim_col: The column used as the third dimension in the xarray Dataset. Defaults to "datetime".
        sort_by_dim: Sort the Dataset by the `dim_col`. Defaults to True.
        mosaic_cols: Other columns to group on. Defaults to None.
        merge: Keyword arguments to pass to [`xarray.merge`][xarray.merge]. Defaults to `{"compat": "override"}`.

    Returns:
        An xarray dataset with chip information.
    """
    # TODO: For `merge`, make it immutable
    L_names = len(group_by)
    group_by = [*group_by]
    if mosaic_cols:
        group_by.extend(mosaic_cols)

    gb = rows.groupby(group_by)
    arr_ds = []

    for name, rows in iter(gb):
        if isinstance(name, str):
            asset_name = name
        elif L_names == 1:
            asset_name = name[0]
        else:
            asset_name = "_".join([str(i) for i in name[:L_names]])
        row = rows.iloc[0]
        x, y = shape_to_xy(row["shape"], row["transform"])
        # TODO: Something is wrong with using this with NAIP because it has multiple bands
        array_data = np.vstack([v.array_data for _, v in rows.iterrows()])
        array_mask = np.vstack([v.array_mask for _, v in rows.iterrows()])
        if dim_col == "datetime":
            dim_col_data = pd.to_datetime(rows[dim_col])
        else:
            dim_col_data = rows[dim_col]
        coords = {dim_col: dim_col_data.values, "y": y, "x": x}
        attrs = row[
            [
                "filepath",
                "datetime",
                "shape",
                "transform",
                "crs",
                "projected_geom",
                "fused_index",
            ]
        ].to_dict()
        ds = xarray.Dataset(
            data_vars={
                asset_name: xarray.DataArray(array_data, coords=coords, attrs=attrs),
                "mask_"
                + asset_name: xarray.DataArray(array_mask, coords=coords, attrs=attrs),
            }
        )
        if sort_by_dim:
            ds = ds.sortby(dim_col)
        arr_ds.append(ds)

    if merge:
        return xarray.merge(arr_ds, **merge)
    else:
        return arr_ds


def get_raster_xarray(
    left,
    right,
    *,
    xarray_group_by: Sequence[str] = ("asset_name",),
    xarray_dim_col: str = "datetime",
    xarray_sort_by_dim: bool = True,
    xarray_mosaic_cols: Optional[Iterable[str]] = None,
    xarray_merge: Optional[Dict[str, Any]] = {"compat": "override"},
    asset_names: Sequence[str],
    n_rows: Optional[int] = None,
    prefetch_entire_files: bool = False,
    buffer: Optional[float] = None,
    href_modifier: Optional[Callable[[str], str]] = None,
    href_mapping: Optional[Dict[str, str]] = None,
    context: None = None,
    api: Optional[FusedAPI] = None,
    _fetch_all_at_once: bool = True,
    **kwargs,
) -> Union[xarray.Dataset, List[xarray.Dataset]]:
    """Get a raster as an xarray Dataset

    Args:
        left: The input join object with geometries.
        right: The input join object with STAC data.

    Keyword Args:
        xarray_group_by: The column(s) in the numpy raster items to group on. Defaults to ("asset_name").
        xarray_dim_col: The column name to use as the xarray dimension column. Defaults to "datetime".
        xarray_sort_by_dim: Sort by the xarray dimension column. Defaults to True.
        xarray_mosaic_cols:
        xarray_merge: Keyword arguments to pass to [`xarray.merge`][xarray.merge]. Defaults to `{"compat": "override"}`.
        asset_names: The name(s) of assets in the STAC data to use.
        n_rows: A limit to the number of rows used from the left side. Defaults to None.
        prefetch_entire_files: If True, on the backend will fetch entire files to the local machine instead of doing partial reads from S3. Defaults to False.
        buffer: If set, will buffer the data by this amount before fetching the raster data. Note that this buffer happens after the join between left and right. Defaults to None.
        href_modifier: A function to modify asset hrefs before fetching. Defaults to None.
        href_mapping: A dictionary from `str` to `str` that specifies how to change asset hrefs before fetching data. Note that `href_modifier` is applied after `href_mapping`. Defaults to None.
        context: Unused in the local `fused` version. On the server this has context information. Defaults to None.
        api: An instance of `FusedAPI` to use for interacting with the backend. Defaults to None.
        _fetch_all_at_once: If True, will parallelize downloads. Defaults to False.

    Yields:
        A dictionary of records with each row's information.
    """
    # TODO: For `xarray_merge`, make it immutable
    rows = get_raster_numpy(
        left=left,
        right=right,
        asset_names=asset_names,
        n_rows=n_rows,
        prefetch_entire_files=prefetch_entire_files,
        buffer=buffer,
        href_modifier=href_modifier,
        href_mapping=href_mapping,
        additional_columns=xarray_group_by,
        context=context,
        api=api,
        _fetch_all_at_once=_fetch_all_at_once,
        **kwargs,
    )

    df2 = pd.DataFrame.from_records([row for row in rows])

    return rows_to_xarray(
        df2,
        group_by=xarray_group_by,
        dim_col=xarray_dim_col,
        sort_by_dim=xarray_sort_by_dim,
        mosaic_cols=xarray_mosaic_cols,
        merge=xarray_merge,
    )


def get_raster_xarray_grouped(
    left,
    right,
    *,
    xarray_group_by: Sequence[str] = ("asset_name",),
    xarray_dim_col: str = "datetime",
    xarray_sort_by_dim: bool = True,
    xarray_mosaic_cols: Optional[Iterable[str]] = None,
    xarray_merge: Optional[Dict[str, Any]] = {"compat": "override"},
    group_by: Sequence[str] = ("orig_file_index", "orig_row_index"),
    asset_names: Sequence[str],
    n_rows: Optional[int] = None,
    prefetch_entire_files: bool = False,
    buffer: Optional[float] = None,
    href_modifier: Optional[Callable[[str], str]] = None,
    href_mapping: Optional[Dict[str, str]] = None,
    context: None = None,
    api: Optional[FusedAPI] = None,
    _fetch_all_at_once: bool = True,
    **kwargs,
) -> Iterable[Union[xarray.Dataset, List[xarray.Dataset]]]:
    """Get a raster as an xarray Dataset

    Args:
        left: The input join object with geometries.
        right: The input join object with STAC data.

    Keyword Args:
        xarray_group_by:
        xarray_dim_col: The column name to use as the xarray dimension column. Defaults to "datetime".
        xarray_sort_by_dim: Sort by the xarray dimension column. Defaults to True.
        xarray_mosaic_cols:
        xarray_merge: Keyword arguments to pass to [`xarray.merge`][xarray.merge]. Defaults to `{"compat": "override"}`.
        group_by: The column(s) to group on.
        asset_names: The name(s) of assets in the STAC data to use.
        n_rows: A limit to the number of rows used from the left side. Defaults to None.
        prefetch_entire_files: If True, on the backend will fetch entire files to the local machine instead of doing partial reads from S3. Defaults to False.
        buffer: If set, will buffer the data by this amount before fetching the raster data. Note that this buffer happens after the join between left and right. Defaults to None.
        href_modifier: A function to modify asset hrefs before fetching. Defaults to None.
        href_mapping: A dictionary from `str` to `str` that specifies how to change asset hrefs before fetching data. Note that `href_modifier` is applied after `href_mapping`. Defaults to None.
        context: Unused in the local `fused` version. On the server this has context information. Defaults to None.
        api: An instance of `FusedAPI` to use for interacting with the backend. Defaults to None.
        _fetch_all_at_once: If True, will parallelize downloads. Defaults to False.

    Yields:
        An xarray Dataset per group
    """
    # TODO: For `xarray_merge`, make it immutable
    groups = get_raster_numpy_grouped(
        left=left,
        right=right,
        group_by=group_by,
        asset_names=asset_names,
        n_rows=n_rows,
        prefetch_entire_files=prefetch_entire_files,
        buffer=buffer,
        href_modifier=href_modifier,
        href_mapping=href_mapping,
        context=context,
        api=api,
        _fetch_all_at_once=_fetch_all_at_once,
        **kwargs,
    )

    for group in groups:
        yield rows_to_xarray(
            group,
            group_by=xarray_group_by,
            dim_col=xarray_dim_col,
            sort_by_dim=xarray_sort_by_dim,
            mosaic_cols=xarray_mosaic_cols,
            merge=xarray_merge,
        )


def get_raster_xarray_from_chips(
    input,
    *,
    sidecar_table_name: str,
    xarray_group_by: Sequence[str] = ("asset_name",),
    xarray_dim_col: str = "datetime",
    xarray_sort_by_dim: bool = True,
    xarray_mosaic_cols: Optional[Iterable[str]] = None,
    xarray_merge: Optional[Dict[str, Any]] = {"compat": "override"},
    n_rows: Optional[int] = None,
    **kwargs,
) -> Union[xarray.Dataset, List[xarray.Dataset]]:
    """Get numpy array from chips

    Args:
        input: The input object prejoined geometry and STAC data.
        sidecar_table_name: The name of the table in `input` that contains chips as its sidecar file.
        xarray_group_by:
        xarray_dim_col: The column name to use as the xarray dimension column. Defaults to "datetime".
        xarray_sort_by_dim: Sort by the xarray dimension column. Defaults to True.
        xarray_mosaic_cols:
        xarray_merge: Keyword arguments to pass to [`xarray.merge`][xarray.merge]. Defaults to `{"compat": "override"}`.
        n_rows: The number of chips to load from the sidecar. Defaults to None, which loads all chips.

    Returns:
        xarray Dataset containing chip data.
    """
    # TODO: For `xarray_merge`, make it immutable
    rows = get_raster_numpy_from_chips(
        input=input,
        sidecar_table_name=sidecar_table_name,
        n_rows=n_rows,
        **kwargs,
    )

    df2 = pd.DataFrame.from_records([row for row in rows])

    return rows_to_xarray(
        df2,
        group_by=xarray_group_by,
        dim_col=xarray_dim_col,
        sort_by_dim=xarray_sort_by_dim,
        mosaic_cols=xarray_mosaic_cols,
        merge=xarray_merge,
    )


def rasterize_geometry_xarray(
    xds: xarray.Dataset, scale: float = 10, all_touched: bool = False
) -> xarray.Dataset:
    """Create an Xarray Dataset with the geometry.

    Args:
      xds: Dataset to take geometry, transform, and shape attributes from.
      scale: Scale factor to apply when computing geometry weights. Higher values use more pixels to calculate the weight.
      all_touched: rasterization strategy. Defaults to False.

    Returns:
      xarray Dataset containing geometry weights, or NaN where the geometry does not intersect.
    """
    shape = xds.asset.attrs["shape"][1:]
    shape[0] *= scale
    shape[1] *= scale
    aff = xds.asset.attrs["transform"]
    transform = Affine(aff[0] / scale, aff[1], aff[2], aff[3], aff[4] / scale, aff[5])
    geom_mask = rasterize_geometry(
        geom=xds.asset.attrs["projected_geom"],
        shape=shape,
        affine=transform,
        all_touched=all_touched,
    )
    x, y = shape_to_xy(shape, transform)
    coords = {"y": y, "x": x}
    geom_xarray = (
        xarray.DataArray(geom_mask, coords=coords).coarsen(x=scale, y=scale).mean()
    )
    return geom_xarray.where(geom_xarray > 0, np.nan)
