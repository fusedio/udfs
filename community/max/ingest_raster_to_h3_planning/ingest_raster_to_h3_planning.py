@fused.udf
def udf(
    input_path: str | list[str] = "s3://fused-asset/data/cdls/2024_30m_cdls.tif",
    metrics: str | list[str] = "cnt",
    res: int | None = None,
    k_ring: int = 1,
    res_offset: int = 1,
    chunk_res: int | None = None,
    file_res: int | None = None,
    overview_res: list[int] | None = None,
    # overview_chunk_res: int | list[int] | None = None,
    # max_rows_per_chunk: int = 0,
    include_source_url: bool = True,
    target_chunk_size: int | None = None,
    # nodata: int | float | None = None,
    # debug_mode: bool = False,
    # steps: list[str] | None = None,
    bounds: list[float] | None = None,
):
    """
    Estimate what the Raster to H3 ingestion will produce and require for resources.

    Arguments are the same as for `run_ingest_raster_to_h3`, except for the
    additional arguments listed below.

    Args:
        bounds (list of float, optional): The spatial bounds of the entire input
            dataset to consider for the ingestion (used to estimate the resulting
            number of files in the dataset). Specify this if your input dataset
            consists of more than 20 files.
    """
    import numpy as np
    import pandas as pd
    import h3.api.basic_int as h3
    import shapely
    from fused._h3.ingest import infer_defaults, _list_files

    # Validate and preprocess input src path
    if isinstance(input_path, str):
        # single file or directory input
        src_files = _list_files(input_path)
        if not src_files:
            raise ValueError(f"No input files found at {input_path}")
    else:
        src_files = input_path
    print(
        f"-- Ingesting {len(src_files)} file(s) at {src_files[0] if len(src_files) == 1 else src_files[0].rsplit('/', maxsplit=1)[0]}"
    )

    res_inferred = res is None
    file_res_inferred = file_res is None
    chunk_res_inferred = chunk_res is None

    res, file_res, chunk_res = infer_defaults(
        src_files[0],
        res,
        file_res,
        chunk_res,
        k_ring=k_ring,
        res_offset=res_offset,
    )

    metrics = _validate_metrics(metrics, k_ring, res_offset)

    overview_res, _ = _validate_overview_res(overview_res, None, res)

    # getting metadata for the source file
    if len(src_files) > 20:
        print(
            "-- Note: when ingesting more than 20 files, the estimates are based on the first 20 files only"
        )
    common = fused.load("https://github.com/fusedio/udfs/tree/642de66/public/common/")
    metas = []
    for input_path in src_files[:20]:
        meta = common.get_tiff_info(input_path, cache_verbose=False)
        metas.append(meta)

    bytes_data = np.dtype(metas[0].dtype.item()).itemsize

    if bounds is not None:
        bbox = shapely.box(*bounds)
    else:
        bbox = shapely.unary_union([meta["geometry"].item() for meta in metas])
        if len(src_files) > 20:
            # when ingesting more than 20 files, assume a full coverage -> take
            # the bounding box of the union of the geometries of the first 20 files
            bbox = bbox.envelope
            print(
                "-- Note: taking the bounding box of the extents of the first 20 "
                "files. Consider specifying `bounds` instead if you know the full "
                "extent of the entire dataset."
            )
    # determine the number of cells that are covered by the bbox
    # -> have to do this in parts, because otherwise if the bbox is larger
    #    than half of the world, h3.geo_to_cells will return the cells for the
    #    inverted bbox
    bbox_split = _split_bbox_by_axes(bbox.bounds)
    cells = []
    for bounds in bbox_split:
        cells.extend(h3.geo_to_cells(shapely.box(*bounds), res=file_res))
    cells = list(set(cells))

    n_files = max(len(cells), 1)
    n_rowgroups_per_file = 7 ** (chunk_res - file_res)

    n_hex_per_file = 7 ** (res - file_res)

    pixel_area = get_pixel_area(src_files[0], cache_verbose=False)
    n_pixels_per_hex = (
        h3.cell_area(h3.latlng_to_cell(bbox.centroid.y, bbox.centroid.x, res), "m^2")
        / pixel_area
    )

    pixel_size = int(round(np.sqrt(pixel_area), -1))
    res_reason = (
        f"inferred based on estimated pixel size of {pixel_size}x{pixel_size} m (https://docs.fused.io/guide/h3-analytics/resolution-guide#resolution-table)"
        if res_inferred
        else "user-specified"
    )
    file_res_reason = (
        "inferred based on target resolution `res`, targetting 100MB to 1GB files"
        if file_res_inferred
        else "user-specified"
    )
    chunk_res_reason = (
        "inferred based on target resolution `res`, targetting at least 1,000,000 rows per file chunk"
        if chunk_res_inferred
        else "user-specified"
    )

    if metrics == ["cnt"]:
        msg_rows_per_file = f"between {n_hex_per_file:,} and {round(n_hex_per_file * n_pixels_per_hex):,}"
    else:
        msg_rows_per_file = f"{n_hex_per_file:,}"

    print(f"""\

Overview
--------

Parameters being used for the ingestion process:
- res={res} ({res_reason})
- file_res={file_res} ({file_res_reason})
- chunk_res={chunk_res} ({chunk_res_reason})

With those parameters, the ingestion will produce an H3 Parquet dataset consisting of:
- {n_files} data file(s)
- up to {n_rowgroups_per_file} row groups per file
- up to {msg_rows_per_file} rows per file

""")

    ###########################################################################
    # Step 1: extract

    if target_chunk_size is None:
        if len(src_files) > 20:
            print(
                "-- Note: processing each file as a single chunk by default (specify "
                "'target_chunk_size' to override this)\n"
            )
            target_chunk_size = 0
        else:
            target_chunk_size = 10_000_000

    n_chunks_per_file = []
    n_rows_per_chunk_per_file = []
    for meta in metas:
        if target_chunk_size > 0:
            # determine number of chunks based on target chunk size
            x_chunks = max(round(meta["width"].item() / np.sqrt(target_chunk_size)), 1)
            y_chunks = max(round(meta["height"].item() / np.sqrt(target_chunk_size)), 1)
        else:
            # allow to process each file as a single chunk
            x_chunks = 1
            y_chunks = 1

        n_chunks = x_chunks * y_chunks
        n_rows_per_chunk = meta["width"].item() * meta["height"].item() / n_chunks

        n_chunks_per_file.append(n_chunks)
        n_rows_per_chunk_per_file.append(n_rows_per_chunk)

    n_chunks_msg = (
        n_chunks_per_file[0]
        if len(set(n_chunks_per_file)) == 1
        else f"between {min(n_chunks_per_file):,} and {max(n_chunks_per_file):,}"
    )
    if len(src_files) > 20:
        n_jobs = n_chunks_per_file[0] * len(src_files)
    else:
        n_jobs = sum(n_chunks_per_file)

    max_gb_extract = _estimate_memory_usage_extract(
        bytes_data, n_rows_per_chunk_per_file, n_pixels_per_hex
    )
    # multiply with factor of 2 as conservative estimate
    max_gb_extract = max_gb_extract * 2
    if max_gb_extract < 10:
        extract_instance_msg = "This should fit on a realtime instance."
    else:
        recommended_instance = None
        for instance_type, memory_gb in _aws_r5_instance_types.items():
            if memory_gb >= max_gb_extract:
                recommended_instance = instance_type
                break
        if recommended_instance is not None:
            extract_instance_msg = f"This might fail on a realtime instance. Consider using '{recommended_instance}'."
        else:
            extract_instance_msg = "This is likely too large for any of the supported batch instances. Consider chunking up your data."

    print(f"""\
Step 1: extract
---------------

- Each source file is processed in {n_chunks_msg} chunk(s)
- Total number of chunks to process (i.e. number of required jobs): {n_jobs}
- Processing each chunk (around {round(n_rows_per_chunk):,} pixels) is expected to need {max_gb_extract:.2f} GiB of memory.
  {extract_instance_msg}
""")

    ###########################################################################
    # Step 2: partition

    max_gb_partition = _estimate_memory_usage_partition(
        bytes_data, n_hex_per_file, n_pixels_per_hex, metrics, include_source_url
    )
    # multiply with factor of 2 as conservative estimate
    max_gb_partition = max_gb_partition * 2
    if max_gb_partition < 10:
        partition_instance_msg = "This should fit on a realtime instance."
    else:
        recommended_instance = None
        for instance_type, memory_gb in _aws_r5_instance_types.items():
            if memory_gb >= max_gb_partition:
                recommended_instance = instance_type
                break
        if recommended_instance is not None:
            partition_instance_msg = (
                f"Consider using an '{recommended_instance}' instance."
            )
        else:
            partition_instance_msg = "This is likely too large for any of the supported batch instances. Consider chunking up your data."

    print(f"""\
Step 2: partition
-----------------

- The re-partition step is executed per resulting file, thus requiring {n_files} jobs.
- Each job is expected to need {max_gb_partition:.2f} GiB of memory.
  {partition_instance_msg}
""")

    ###########################################################################
    # Step 3: overview

    # only estimating it for the largest overview resolution,
    # which should be the most expensive one
    max_gb_overview = _estimate_memory_usage_overview(
        bytes_data, n_files, max(overview_res), file_res, n_pixels_per_hex, metrics
    )
    if max_gb_overview < 10:
        overview_instance_msg = "This should fit on a realtime instance."
    else:
        recommended_instance = None
        for instance_type, memory_gb in _aws_r5_instance_types.items():
            if memory_gb >= max_gb_overview:
                recommended_instance = instance_type
                break
        if recommended_instance is not None:
            overview_instance_msg = (
                f"Consider using an '{recommended_instance}' instance."
            )
        else:
            overview_instance_msg = "This is likely too large for any of the supported batch instances. Consider chunking up your data."

    print(f"""\
Step 3: overview
----------------

- The overview step is executed for all files together per overview resolution, thus requiring {len(overview_res)} jobs.
- The largest of those jobs is expected to need {max_gb_overview:.2f} GiB of memory.
  {overview_instance_msg}
""")

    return meta


_aws_r5_instance_types = {
    "r5.large": 16,
    "r5.xlarge": 32,
    "r5.2xlarge": 64,
    "r5.4xlarge": 128,
    "r5.8xlarge": 256,
    "r5.12xlarge": 384,
    "r5.16xlarge": 512,
}


@fused.cache
def get_pixel_area(src_path: str):
    # copied from fused.h3.ingest
    import pyproj
    import rasterio

    with rasterio.open(src_path) as src:
        src_crs = pyproj.CRS(src.crs)
        # estimate target resolution based on pixel size
        # -> use resolution where 7 cells would roughly cover one pixel
        if src_crs.is_projected:
            pixel_area = (
                (src.bounds.right - src.bounds.left)
                / src.width
                * (src.bounds.top - src.bounds.bottom)
                / src.height
            )
        else:
            # approximate pixel area in m^2 at center of raster
            transformer = pyproj.Transformer.from_crs(
                src_crs, "EPSG:3857", always_xy=True
            )
            x_center = (src.bounds.right + src.bounds.left) / 2
            y_center = (src.bounds.top + src.bounds.bottom) / 2
            x1, y1 = transformer.transform(x_center, y_center)
            x2, y2 = transformer.transform(
                x_center + (src.bounds.right - src.bounds.left) / src.width,
                y_center + (src.bounds.top - src.bounds.bottom) / src.height,
            )
            pixel_area = abs((x2 - x1) * (y2 - y1))

    return pixel_area


def _estimate_memory_usage_extract(
    bytes_data, n_rows_per_chunk_per_file, n_pixels_per_hex
):
    # for the input, estimate the size of the resulting dataframe from reading one chunk
    bytes_per_row_extract_input = (
        bytes_data
        # lat/lng columns
        + 8 * 2
    )
    max_gb_extract_input = (
        max(n_rows_per_chunk_per_file) * bytes_per_row_extract_input / 1024**3
    )

    # for the output, estimate the size of the resulting dataframe from processing
    # one chunk -> expanded and grouped by data and hex, so max rows is assuming
    # each pixel in one hex cell has a different value
    bytes_per_row_extract_output = (
        # hex column -> uint64
        8
        # data column -> depends on raster
        + bytes_data
        # counts column -> int32
        + 4
        # source_url index -> int32
        + 4
    )
    max_gb_extract_output = (
        max(n_rows_per_chunk_per_file)  # TODO this should be the number of hex cells
        * n_pixels_per_hex
        * bytes_per_row_extract_output
        / 1024**3
    )
    return max_gb_extract_input + max_gb_extract_output


def _estimate_memory_usage_partition(
    bytes_data, n_hex_per_file, n_pixels_per_hex, metrics, include_source_url
):
    # Estimate number of rows after extract step,
    # assume every pixel has a different value (worst case scenario)
    bytes_per_row = (
        # hex column -> uint64
        8
        # data column -> depends on raster
        + bytes_data
        # counts column -> int32
        + 4
    )
    if include_source_url:
        # int32 column of index
        bytes_per_row += 4

    max_gb_input = n_hex_per_file * n_pixels_per_hex * bytes_per_row / 1024**3

    if metrics == ["cnt"]:
        bytes_per_row_combined = (
            # hex column -> uint64
            8
            # data column -> depends on raster, here uint8
            + bytes_data
            # counts column -> int32
            + 4
            # cnt_total column -> int32
            + 4
        )
        n_rows = n_hex_per_file * n_pixels_per_hex
    else:
        bytes_per_row_combined = (
            # hex column -> uint64
            8
            # aggregated data columns -> float64 per column
            + 8 * len(metrics)
        )
        n_rows = n_hex_per_file
    if include_source_url:
        # int32 column of index + 4 for list offsets
        bytes_per_row_combined += 4 + 4

    max_gb_result = n_rows * bytes_per_row_combined / 1024**3

    return max_gb_input + max_gb_result


def _estimate_memory_usage_overview(
    bytes_data, n_files, overview_res, file_res, n_pixels_per_hex, metrics
):
    # number of rows for all overview files combined
    n_overview_hex = n_files * (7 ** (overview_res - file_res))

    if metrics == ["cnt"]:
        bytes_per_row_overview_input = (
            # hex column -> uint64
            8
            # data column -> depends on raster, here uint8
            + bytes_data
            # counts column -> int32
            + 4
            # cnt_total column -> int32
            + 4
            # lat/lng columns
            + 8 * 2
        )
        n_rows = n_overview_hex * n_pixels_per_hex
    else:
        bytes_per_row_overview_input = (
            # hex column -> uint64
            8
            # aggregated data columns -> float64 per column
            + 8 * len(metrics)
            # lat/lng columns
            + 8 * 2
        )
        n_rows = n_overview_hex

    max_gb_input = n_rows * bytes_per_row_overview_input / 1024**3

    # we are directly writing this data, sorted
    # -> rough estimate for overall to use 2x the amount of memory
    return max_gb_input * 2


def _split_bbox_by_axes(bounds):
    """
    Splits a bounding box into 1-4 sub-boxes at the 0,0 axes.
    Returns a list of tuples: (xmin, ymin, xmax, ymax)
    """
    xmin, ymin, xmax, ymax = bounds
    # Define the split points
    x_splits = [xmin, xmax]
    if xmin < 0 < xmax:
        x_splits = [xmin, 0, xmax]

    y_splits = [ymin, ymax]
    if ymin < 0 < ymax:
        y_splits = [ymin, 0, ymax]

    # Iterate through splits to creates bboxes
    bboxes = []
    for i in range(len(x_splits) - 1):
        for j in range(len(y_splits) - 1):
            bboxes.append((x_splits[i], y_splits[j], x_splits[i + 1], y_splits[j + 1]))

    return bboxes


def _validate_metrics(
    metrics: str | list[str], k_ring: int, res_offset: int
) -> list[str]:
    if isinstance(metrics, str):
        metrics = [metrics]
    else:
        metrics = list(metrics)
        if len(metrics) > 1 and "cnt" in metrics:
            raise ValueError("The 'cnt' metric cannot be combined with other metrics")

    if "sum" in metrics and k_ring != 1 and res_offset != 1:
        raise NotImplementedError(
            "The 'sum' metric is currently only supported for k_ring=1 and res_offset=1"
        )
    return metrics


def _validate_overview_res(
    overview_res: list[int] | None, overview_chunk_res: int | list[int] | None, res: int
) -> tuple[list[int], list[int]]:
    if overview_res is None:
        max_overview_res = min(res - 1, 7)
        overview_res = list(range(3, max_overview_res + 1))
    elif max(overview_res) >= res:
        raise ValueError(
            "Overview resolutions must be lower than the target resolution `res`"
        )
    if overview_chunk_res is None:
        overview_chunk_res = [max(r - 5, 0) for r in overview_res]
    elif isinstance(overview_chunk_res, int):
        overview_chunk_res = [overview_chunk_res] * len(overview_res)

    return overview_res, overview_chunk_res
