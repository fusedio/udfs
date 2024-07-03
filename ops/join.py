from __future__ import annotations

from pathlib import Path
from typing import Dict, Sequence, Tuple

import fused
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import shapely
from affine import Affine
from fused.models.udf.common import Chunk, Chunks
from numpy.typing import NDArray
from shapely.geometry import box


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


def rio_geom_to_xy_slice(geom, shape, transform):
    local_bounds = shapely.bounds(geom)
    if transform[4] < 0:  # if pixel_height is negative
        original_window = rasterio.windows.from_bounds(
            *local_bounds, transform=transform
        )
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        y_slice, x_slice = gridded_window.toslices()
        return x_slice, y_slice
    else:  # if pixel_height is not negative
        original_window = rasterio.windows.from_bounds(
            *local_bounds,
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


def sjoin(
    left: Chunk,
    right: Chunks,
    *,
    right_cols: Sequence[str] = (),
    left_cols: Sequence[str] = ("fused_index", "fused_area", "geometry"),
) -> gpd.GeoDataFrame:
    """Helper to run a geopandas sjoin on a join sample

    Args:
        left: a sample on which to run `sjoin`.
        right: samples on which to run `sjoin`.
        right_cols: The columns of the right input to keep.
        left_cols: The columns of the left input to keep. Defaults to ("fused_index", "fused_area", "geometry").

    Returns:
        The spatially joined data.
    """
    # Pandas needs a list of strings as input
    left_cols = list(left_cols)
    right_cols = list(right_cols)

    if len(right_cols) == 0:
        msg = (
            "`right_cols` must be provided. "
            f"The columns in the right dataset are: {right[0].data.columns}"
        )
        raise TypeError(msg)

    left_data = left.data
    assert isinstance(left_data, gpd.GeoDataFrame)
    if not left_cols:
        left_cols = list(left_data.columns)

    right_table = pd.concat([i.data for i in right])

    assert (
        left_data._geometry_column_name in left_cols
    ), "geometry column not included in left_cols"

    assert (
        right_table._geometry_column_name in right_cols
    ), "geometry column not included in right_cols"

    # The only column that overlaps between the two datasets should be the geometry
    # column
    assert (
        set(left_cols).difference({"geometry"}).isdisjoint(right_cols)
    ), "Left and right columns must not overlap."

    assert (
        len(set(left_cols).difference(left_data.columns)) == 0
    ), "All left columns must exist in the left DataFrame."

    assert (
        len(set(right_cols).difference(right_table.columns)) == 0
    ), "All left columns must exist in the right DataFrame."

    return left_data[left_cols].sjoin(right_table[right_cols])


def rio_clip_geom(geom, url):
    import s3fs
    import xarray

    def expand_slice(s, expand_size=1):
        return slice(max(s.start - expand_size, 0), s.stop + expand_size, s.step)

    try:
        path = fused.download(url, url + ".tiff")
        ds = xarray.open_dataset(path, engine="rasterio", lock=False)
        x, y = rio_geom_to_xy_slice(
            geom, shape=ds.band_data.shape[1:], transform=ds.rio.transform()
        )
        x = expand_slice(x, 1)
        y = expand_slice(y, 1)
        da = ds.isel(x=x, y=y).band_data

        return da, ds.isel(x=x, y=y).band_data.values
    except Exception as e:
        return -1, -1


def get_raster_bounds(filename):  # TIFF
    path = Path("https://fused-asset.s3.us-west-2.amazonaws.com/gfc2020/")
    ds = rasterio.open(path / filename)

    # Create a GeoDataFrame with the bounds polygon
    bbox = box(ds.bounds.left, ds.bounds.bottom, ds.bounds.right, ds.bounds.top)
    gdf_bounds = gpd.GeoDataFrame({"geometry": bbox}, index=[0], crs=ds.crs)

    # Define the number of divisions
    gridsize = (nx, ny) = (2, 2)

    # Calculate the size of each smaller polygon in degrees
    crs = gdf_bounds.crs
    minx, miny, maxx, maxy = gdf_bounds.total_bounds

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
                crs=crs,
            )
            sub_bbox["fused_sub_id"] = f"{i}_{j}"
            bounding_boxes.append(sub_bbox)
    return [
        [bbox.geometry.total_bounds, bbox["fused_sub_id"].iloc[0]]
        for bbox in bounding_boxes
    ]  # Array of VECTOR
