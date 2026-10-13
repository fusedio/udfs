"""AlphaEarth Foundations (AEF) annual embeddings (collection AEF): 64 features (A00..A63) per 10 m pixel, 2017-2025.

Source: int8 COGs on source.coop (tge-labs/aef/v1/annual), nodata -128, de-quantised as sign(q) * (q / 127.5) ** 2.
Each pixel is a vector of about unit length, so a dot product of two vectors is their cosine similarity.
"""

try:
    import su_constants as constants
    import su_geometry as geometry
    import su_time as time
except ImportError:
    constants = fused.load("su_constants")
    geometry = fused.load("su_geometry")
    time = fused.load("su_time")

AEF_INDEX = "https://data.source.coop/tge-labs/aef/v1/annual/aef_index.parquet"
AEF_YEARS = range(2017, 2026)
AEF_FEATURES = constants.DATA_COLLECTIONS[constants.AEF]["bands"]
AEF_NODATA = -128
AEF_ENV = dict(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
               GDAL_HTTP_MULTIPLEX="YES", GDAL_INGESTED_BYTES_AT_OPEN="32768", VSI_CACHE="TRUE",
               VSI_CACHE_SIZE="50000000", AWS_NO_SIGN_REQUEST="YES", AWS_REGION="us-west-2")


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def embedding_year(date):
    """Returns the AEF year (int) of a year (2024, "2024"), a day in that year, or a (start, end) pair in one year."""
    if isinstance(date, (list, tuple)):
        years = {embedding_year(d) for d in date}
        if len(years) != 1:
            raise ValueError(f"AEF has one image per year; the dates {date!r} are in the years {sorted(years)}")
        return years.pop()
    text = str(date).strip()
    year = int(text) if text.isdigit() and len(text) == 4 else time.parse_time(text).year
    if year not in AEF_YEARS:
        raise ValueError(f"AEF year must be one of {list(AEF_YEARS)}, got {date!r}")
    return year


def load_embeddings(area, year, *, resolution=None, geobox=None):
    """Returns a float32 DataArray (band A00..A63, y, x) of unit vectors on the area (or geobox) grid; NaN = no data."""
    import math
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np
    import xarray as xr

    year = embedding_year(year)
    gb = geobox if geobox is not None else geometry.make_geobox(area, resolution=resolution or 10)
    height, width = gb.shape
    if 64 * height * width > constants.MAX_PIXELS:
        raise ValueError(f"64 x {height} x {width} values is more than MAX_PIXELS {constants.MAX_PIXELS:,}; "
                         f"use a coarser resolution")
    west, south, east, north = geometry.transform_bbox(tuple(gb.extent.boundingbox), str(gb.crs), "EPSG:4326")
    if west > east:
        raise ValueError("the grid crosses 180 degrees; split the area in two")
    files = _aef_files(year)
    paths = files.loc[(files.wgs84_west < east) & (files.wgs84_east > west) & (files.wgs84_south < north)
                      & (files.wgs84_north > south), "path"].tolist()
    if not paths:
        raise constants.NoDataError(f"no AEF file for {year} over {[west, south, east, north]}")
    ground = (east - west) * 111_320 * math.cos(math.radians((south + north) / 2)) / width
    level = _aef_overview_level(ground)
    out = np.full((64, height, width), AEF_NODATA, dtype="int8")
    jobs = [(path, list(range(g * 16 + 1, g * 16 + 17))) for path in paths for g in range(4)]
    with ThreadPoolExecutor(16) as pool:
        for got in pool.map(lambda job: _read_aef(job[0], job[1], gb, level), jobs):
            if got is not None:
                part = slice(got[0][0] - 1, got[0][-1])
                out[part] = np.where(out[part] == AEF_NODATA, got[1], out[part])
    q = out.astype("float32")
    values = np.where(out == AEF_NODATA, np.nan, np.sign(q) * (q / 127.5) ** 2).astype("float32")
    a = gb.affine
    da = xr.DataArray(values, dims=("band", "y", "x"), name="embedding", attrs={"year": year, "files": len(paths)},
                      coords={"band": list(AEF_FEATURES), "y": a.f + a.e * (np.arange(height) + 0.5),
                              "x": a.c + a.a * (np.arange(width) + 0.5)})
    return _odc(da).assign_crs(gb.crs)


def embedding_at(emb, lon, lat):
    """Returns the 64-vector (numpy) of the pixel nearest to lon, lat."""
    from pyproj import Transformer

    x, y = Transformer.from_crs("EPSG:4326", str(_odc(emb).crs), always_xy=True).transform(lon, lat)
    return emb.sel(x=x, y=y, method="nearest").values


def embedding_of(emb, area):
    """Returns the mean unit vector (numpy) of the pixels inside area (polygon, GeoDataFrame, GeoJSON, bbox)."""
    import numpy as np
    from odc.geo.geom import Geometry
    from rasterio.features import geometry_mask

    gb = _odc(emb).geobox
    geom = Geometry(geometry.to_geometry(area), crs="EPSG:4326").to_crs(gb.crs).geom
    inside = geometry_mask([geom], out_shape=gb.shape, transform=gb.affine, invert=True, all_touched=True)
    values = emb.values[:, inside]
    values = values[:, ~np.isnan(values).any(axis=0)]
    if not values.size:
        raise constants.NoDataError("the area covers no valid pixel of the embeddings")
    return _unit(values.mean(axis=1))


def embedding_similarity(emb, ref):
    """Returns a DataArray (y, x) of the cosine similarity of each pixel to the 64-vector ref (1 = same)."""
    import numpy as np

    norm = np.linalg.norm(emb.values, axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        sim = np.tensordot(_unit(ref), emb.values, axes=1) / np.where(norm > 0, norm, np.nan)
    return _on_grid(sim.astype("float32"), emb, "similarity")


def embedding_difference(a, b):
    """Returns a DataArray (y, x) of 1 - cosine similarity of two years on one grid (0 = no change, 2 = opposite)."""
    import numpy as np

    den = np.linalg.norm(a.values, axis=0) * np.linalg.norm(b.values, axis=0)
    with np.errstate(invalid="ignore"):
        diff = 1 - (a.values * b.values).sum(0) / np.where(den > 0, den, np.nan)
    return _on_grid(diff.astype("float32"), a, "difference")


def embedding_preview(emb, sample=20_000, seed=0):
    """Returns uint8 RGBA (4, H, W) of the first three principal components, stretched 2-98 %."""
    import numpy as np

    flat = emb.values.reshape(emb.shape[0], -1).T
    ok = ~np.isnan(flat).any(1)
    rgba = np.zeros((4,) + emb.shape[1:], dtype="uint8")
    if ok.sum() < 10:
        return rgba
    pick = np.random.default_rng(seed).choice(int(ok.sum()), min(sample, int(ok.sum())), replace=False)
    sub = flat[ok][pick]
    mean = sub.mean(0)
    pcs = (flat[ok] - mean) @ np.linalg.svd(sub - mean, full_matrices=False)[2][:3].T
    lo, hi = np.percentile(pcs, 2, axis=0), np.percentile(pcs, 98, axis=0)
    rgb = np.zeros((flat.shape[0], 3), dtype="uint8")
    rgb[ok] = (np.clip((pcs - lo) / np.where(hi > lo, hi - lo, 1), 0, 1) * 255).astype("uint8")
    rgba[:3] = rgb.T.reshape((3,) + emb.shape[1:])
    rgba[3] = ok.reshape(emb.shape[1:]) * 255
    return rgba


@fused.cache(cache_max_age="30d", cache_verbose=False)
def _aef_files(year):
    import fsspec
    import pyarrow.parquet as pq

    cols = ["path", "wgs84_west", "wgs84_south", "wgs84_east", "wgs84_north"]
    with fsspec.open(AEF_INDEX).open() as f:
        table = pq.read_table(f, columns=cols + ["year"], filters=[("year", "=", int(year))])
    return table.select(cols).to_pandas()


def _aef_overview_level(resolution):
    level = -1
    while 2 ** (level + 2) * 10 <= resolution and level < 11:
        level += 1
    return level


def _read_aef(path, bands, gb, level):
    import warnings
    import numpy as np
    import rasterio
    from rasterio.warp import Resampling, reproject
    from rasterio.windows import Window

    href = "/vsis3/" + path[5:] if path.startswith("s3://") else path
    try:
        with rasterio.Env(**AEF_ENV), rasterio.open(href, **({"overview_level": level} if level >= 0 else {})) as src:
            bb = geometry.transform_bbox(tuple(gb.extent.boundingbox), str(gb.crs), str(src.crs))
            cols, rows = zip(*[~src.transform * (x, y) for x in (bb[0], bb[2]) for y in (bb[1], bb[3])])
            c0, c1 = max(0, int(min(cols)) - 1), min(src.width, int(max(cols)) + 2)
            r0, r1 = max(0, int(min(rows)) - 1), min(src.height, int(max(rows)) + 2)
            if c1 <= c0 or r1 <= r0:
                return None
            window = Window(c0, r0, c1 - c0, r1 - r0)
            data, transform, crs = src.read(bands, window=window), src.window_transform(window), src.crs
    except rasterio.errors.RasterioIOError as e:
        warnings.warn(f"AEF read failed for {path}: {str(e)[:120]}")
        return None
    dst = np.full((len(bands),) + tuple(gb.shape), AEF_NODATA, dtype="int8")
    reproject(data, dst, src_transform=transform, src_crs=crs, src_nodata=AEF_NODATA, dst_transform=gb.affine,
              dst_crs=str(gb.crs), dst_nodata=AEF_NODATA, resampling=Resampling.nearest)
    return bands, dst


def _on_grid(values, like, name):
    import xarray as xr

    da = xr.DataArray(values, coords={"y": like["y"], "x": like["x"]}, dims=("y", "x"), name=name,
                      attrs=dict(like.attrs))
    return _odc(da).assign_crs(_odc(like).crs)


def _unit(v):
    import numpy as np

    v = np.asarray(v, dtype="float32")
    return v / (np.linalg.norm(v) or 1.0)


def _odc(obj):
    import odc.geo.xr

    return obj.odc
