import random
from typing import List, Literal, Optional, Sequence, Tuple, Union

import fused
import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
import shapely
from loguru import logger
from numpy.typing import NDArray
from shapely.geometry import box


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


# TODO: modularize to be general
def get_asset_dissolve(
    url=f"s3://fused-users/fused/plinio/assets_with_bounds_4_4_antimeridian.parquet",
    order=0,
):
    import geopandas as gpd

    gdf = gpd.read_parquet(url)
    gdf["url"] = gdf["url"].map(lambda x: x[order])
    gdf["fused_sub_id"] = gdf["fused_sub_id"].map(lambda x: x[order])
    gdf = gdf.dissolve(["fused_sub_id", "url"])
    gdf.reset_index(inplace=True)
    gdf["ind"] = range(len(gdf))
    del gdf["sub_bounds"]
    return gdf


def geo_subdivide(gdf, xy_grid=(2, 2), adjust_ratio=True):
    import geopandas as gpd
    import pandas as pd
    import shapely

    gdf = gdf.copy()
    gdf["_fidx_"] = range(len(gdf))
    nx, ny = xy_grid
    minx, miny, maxx, maxy = gdf.total_bounds

    if adjust_ratio:
        ratio = abs((minx - maxx) / (miny - maxy))
        if ratio > 1:
            nx = int(nx * ratio)
        else:
            ny = int(ny / ratio)

    width = (maxx - minx) / nx
    height = (maxy - miny) / ny

    bounding_boxes = []

    for i in range(nx):
        for j in range(ny):
            sub_bbox = gpd.GeoDataFrame(
                geometry=[
                    shapely.geometry.box(
                        minx + i * width,
                        miny + j * height,
                        minx + (i + 1) * width,
                        miny + (j + 1) * height,
                    )
                ],
                crs=gdf.crs,
            )
            sub_bbox["fused_sub_id"] = f"{i}_{j}"
            bounding_boxes.append(sub_bbox)

    sub_gdfs = []
    _fidx_ = []
    df_bbox = pd.concat(bounding_boxes)
    tmp = gdf.sjoin(df_bbox).reset_index().drop_duplicates("_idx_")
    del tmp["index_right"]
    tmpg = tmp.groupby("fused_sub_id")
    return [
        tmpg.get_group(g) for g in tmpg.groups.keys()
    ]  # if len(tmpg.get_group(g))>0]
