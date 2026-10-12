"""Sentinel-2 utilities: search, load, mask, index, composite, analyse, classify, render, tile and export Sentinel-2 L2A.

    s2 = fused.load("https://github.com/fusedio/udfs/tree/<commit>/public/sentinel_utils/")
    scenes = s2.search(aoi, "2024-06-01", "2024-08-31")
    summer = s2.image(aoi, "2024-06-01", "2024-08-31", s2.needs("NDVI"))
    ndvi = s2.index(summer, "NDVI")
    rgba = s2.render(summer, "NDVI")

The UDF of this file is a world map tile layer: any place and date, made on request, nothing stored.
README.MD gives the function list, the parameter names, the return types and the data rules.
"""


@fused.udf(cache_max_age="7d")
def udf(bounds: fused.types.Bounds = [-73.9893, 40.7638, -73.9453, 40.7971], date: str = "2024-07-01", days: int = 30,
        what: str = "true_color", method: str = "best", max_cloud: float = 80, cloud_mask: bool = True, cmap: str = "",
        vmin: str = "", vmax: str = "", min_zoom: int = 7, size: int = 256):
    """World map tile layer: the least cloudy Sentinel-2 pixels around `date` (± `days`), as RGBA.

    Call it as /run/tiles/{z}/{x}/{y}?date=2024-07-01&what=NDVI. `what` is a preset (true_color, false_color ...),
    a catalog index (NDVI, NBR ...) or a formula. Tiles below `min_zoom` are empty.
    """
    z, x, y = bounds_to_tile(bounds)
    return tile(z, x, y, date, days=int(days), what=what, method=method, max_cloud=float(max_cloud),
                cloud_mask=bool(cloud_mask), size=int(size), cmap=cmap or None, vmin=float(vmin) if vmin else None,
                vmax=float(vmax) if vmax else None, min_zoom=int(min_zoom))


SCALE = 0.0001
MAX_CLOUD = 80
DAYS = 30
MAX_PIXELS = 200_000_000
C1_GAP = ("2022-01-01", "2022-11-30")
M_PER_DEG_LAT = 110_540
M_PER_DEG_LON = 111_320

BANDS = [
    ("coastal", "B01", "A", 60, 443),
    ("blue", "B02", "B", 10, 490),
    ("green", "B03", "G", 10, 560),
    ("red", "B04", "R", 10, 665),
    ("rededge1", "B05", "RE1", 20, 705),
    ("rededge2", "B06", "RE2", 20, 740),
    ("rededge3", "B07", "RE3", 20, 783),
    ("nir", "B08", "N", 10, 842),
    ("nir08", "B8A", "N2", 20, 865),
    ("nir09", "B09", "WV", 60, 945),
    ("swir16", "B11", "S1", 20, 1610),
    ("swir22", "B12", "S2", 20, 2190),
]
MASK_BANDS = [("scl", "SCL", 20), ("cloud", "CLD", 20), ("snow", "SNW", 20)]
ASSETS = [b[0] for b in BANDS] + [m[0] for m in MASK_BANDS]

SCL = {
    0: "no_data", 1: "saturated", 2: "dark_area", 3: "cloud_shadow", 4: "vegetation", 5: "bare_soil",
    6: "water", 7: "unclassified", 8: "cloud_medium", 9: "cloud_high", 10: "cirrus", 11: "snow",
}
SCL_GROUPS = {
    "clear": (4, 5, 6),
    "clear_snow": (4, 5, 6, 11),
    "land": (4, 5),
    "water": (6,),
    "cloud": (8, 9, 10),
    "shadow": (3,),
    "invalid": (0, 1),
}
RESCUE = (1, 2, 3, 7, 11)

PRESETS = {
    "true_color": (["red", "green", "blue"], 0.0, 0.3, 1.3),
    "false_color": (["nir", "red", "green"], 0.0, 0.5, 1.2),
    "false_color_urban": (["swir22", "swir16", "red"], 0.0, 0.4, 1.2),
    "swir": (["swir22", "nir08", "red"], 0.0, 0.5, 1.2),
    "agriculture": (["swir16", "nir", "blue"], 0.0, 0.5, 1.2),
    "geology": (["swir22", "swir16", "blue"], 0.0, 0.5, 1.2),
    "bathymetric": (["red", "green", "coastal"], 0.0, 0.2, 1.3),
}
PERIODS = {"month": "MS", "quarter": "QS", "season": "QS-DEC", "year": "YS"}

GDAL_ENV = dict(
    GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
    GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
    GDAL_HTTP_MULTIPLEX="YES",
    GDAL_INGESTED_BYTES_AT_OPEN="32768",
    VSI_CACHE="TRUE",
    VSI_CACHE_SIZE="50000000",
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif",
    AWS_NO_SIGN_REQUEST="YES",
)

_BAND_ALIASES = {
    "swir1": "swir16", "swir2": "swir22",
    **{k: b[0] for b in BANDS for k in (b[0], b[1].lower(), b[2].lower(), "b" + b[1][1:].lstrip("0").lower())},
    **{k: m[0] for m in MASK_BANDS for k in (m[0], m[1].lower())},
}


class NoData(ValueError):
    """No scene, or no valid pixel, for the request."""


def band_name(name):
    """Any band spelling (B04, b4, red, R, swir1, SCL) -> the asset name used everywhere here."""
    key = str(name).strip().lower()
    if key not in _BAND_ALIASES:
        raise ValueError(f"unknown band {name!r}; use one of {ASSETS}")
    return _BAND_ALIASES[key]


def band_names(names):
    """List or comma string of band spellings -> unique asset names, order kept."""
    if isinstance(names, str):
        names = [n for n in names.replace(" ", "").split(",") if n]
    out = []
    for n in names:
        if band_name(n) not in out:
            out.append(band_name(n))
    return out


def band_info(name):
    """One band as a dict: name, code, asi (index letter), res (m), wavelength (nm)."""
    n = band_name(name)
    for b, code, asi, res, wl in BANDS:
        if b == n:
            return {"name": b, "code": code, "asi": asi, "res": res, "wavelength": wl}
    code, res = {m[0]: (m[1], m[2]) for m in MASK_BANDS}[n]
    return {"name": n, "code": code, "asi": None, "res": res, "wavelength": None}


def is_mask(name):
    """True for the mask bands: scl, cloud, snow."""
    return band_name(name) in {m[0] for m in MASK_BANDS}


def scl_classes(keep):
    """'clear' / 'land' / (4, 5) / '4,5' -> tuple of SCL class codes."""
    if isinstance(keep, str):
        if keep in SCL_GROUPS:
            return SCL_GROUPS[keep]
        try:
            codes = tuple(int(v) for v in keep.split(",") if v.strip())
        except ValueError:
            raise ValueError(f"keep must be one of {list(SCL_GROUPS)} or SCL codes like '4,5', got {keep!r}") from None
    else:
        codes = tuple(int(v) for v in keep)
    if not codes or any(c not in SCL for c in codes):
        raise ValueError(f"SCL codes must be in 0..11, got {keep!r}")
    return codes


def bbox(aoi):
    """An area of interest -> [w, s, e, n] in degrees. Accepts 'w,s,e,n', [w, s, e, n], a shapely geometry or a
    GeoDataFrame. An AOI that crosses 180° must be split into two."""
    value = aoi
    if isinstance(value, str):
        value = [v for v in value.replace("[", "").replace("]", "").split(",") if v.strip()]
    elif hasattr(value, "total_bounds"):
        value = value.to_crs(4326).total_bounds if value.crs else value.total_bounds
    elif hasattr(value, "geom_type"):
        value = value.bounds
    try:
        vals = [float(v) for v in value]
    except (TypeError, ValueError):
        raise ValueError(f"aoi must be 'w,s,e,n', [w, s, e, n], a shapely geometry or a GeoDataFrame, got {aoi!r}") from None
    if len(vals) != 4 or not (-180 <= vals[0] < vals[2] <= 180 and -90 <= vals[1] < vals[3] <= 90):
        raise ValueError(f"aoi must be west,south,east,north in degrees with west < east and south < north "
                         f"(split an AOI that crosses 180°), got {vals}")
    return vals


def date_window(date, days=DAYS):
    """(start, end) of `date` ± `days` as YYYY-MM-DD strings."""
    import pandas as pd

    d = pd.Timestamp(_day(date))
    half = pd.Timedelta(days=int(days))
    return (d - half).strftime("%Y-%m-%d"), (d + half).strftime("%Y-%m-%d")


def frequency(period):
    """month / quarter / season / year, or any pandas frequency (10D, 2MS ...) -> a pandas frequency."""
    return PERIODS.get(period, period)


def utm_crs(bounds):
    """EPSG string of the UTM zone at the centre of [w, s, e, n]."""
    lon, lat = (bounds[0] + bounds[2]) / 2, (bounds[1] + bounds[3]) / 2
    return f"EPSG:{(32600 if lat >= 0 else 32700) + int((lon + 180) // 6) % 60 + 1}"


def size_m(bounds):
    """(width, height) of [w, s, e, n] in metres, at its centre latitude."""
    import math

    lat = math.radians((bounds[1] + bounds[3]) / 2)
    return (bounds[2] - bounds[0]) * M_PER_DEG_LON * math.cos(lat), (bounds[3] - bounds[1]) * M_PER_DEG_LAT


def auto_res(bounds, max_px, min_res=10):
    """Finest resolution (m, multiple of 10, >= min_res) that keeps the long side of the box under max_px."""
    import math

    return max(float(min_res), math.ceil(max(size_m(bounds)) / int(max_px) / 10) * 10)


def geobox(aoi=None, res=10, crs="utm", shape=None, like=None):
    """Output grid: the grid of `like` (GeoBox or xarray object), else `aoi` at `res` metres (or `shape`) in `crs`.

    `res` is in the metres of a projected crs; for EPSG:3857 it is ground metres at the AOI's centre latitude.
    A geographic crs (EPSG:4326), or a crs in other units (feet), needs `shape`.
    """
    import math
    from odc.geo import CRS
    from odc.geo.geobox import GeoBox
    from rasterio.warp import transform_bounds

    if like is not None:
        return like if isinstance(like, GeoBox) else _odc(like).geobox
    bounds = bbox(aoi)
    crs = CRS(utm_crs(bounds) if crs == "utm" else crs)
    bb = transform_bounds("EPSG:4326", str(crs), *bounds)
    if shape:
        return GeoBox.from_bbox(bb, crs=crs, shape=tuple(int(v) for v in shape))
    if crs.geographic or crs.units[0] != "metre":
        raise ValueError(f"res is in metres, but {crs} uses {crs.units[0]}; give shape= or use a metric crs")
    step = float(res) / math.cos(math.radians((bounds[1] + bounds[3]) / 2)) if crs.epsg == 3857 else float(res)
    return GeoBox.from_bbox(bb, crs=crs, resolution=step)


def on_grid(values, like, name=None, attrs=None):
    """A (y, x) numpy array -> DataArray on the grid of `like`, with its CRS."""
    import xarray as xr

    da = xr.DataArray(values, coords={"y": like["y"], "x": like["x"]}, dims=("y", "x"), name=name, attrs=attrs or {})
    return keep_crs(da, like)


def keep_crs(out, like):
    """`out` with the CRS of `like` (xarray reductions and concat drop it)."""
    crs = _odc(like).crs
    return _odc(out).assign_crs(crs) if crs is not None else out


def reflectance(ds):
    """The Dataset without mask bands (scl, cloud, snow)."""
    masks = {m[0] for m in MASK_BANDS}
    return ds[[v for v in ds.data_vars if v not in masks]]


def nanquantile(a, q, axis=0):
    """NaN-skipping linear quantile along `axis` (sort + index; ~150x faster than np.nanquantile)."""
    import numpy as np

    a = np.moveaxis(np.asarray(a, dtype="float32"), axis, 0)
    n = (~np.isnan(a)).sum(0)
    srt = np.sort(a, axis=0)
    pos = float(q) * np.clip(n - 1, 0, None)
    lo, hi = np.floor(pos).astype("int64"), np.ceil(pos).astype("int64")
    vlo = np.take_along_axis(srt, lo[None], 0)[0]
    vhi = np.take_along_axis(srt, hi[None], 0)[0]
    return np.where(n > 0, vlo + (vhi - vlo) * (pos - lo), np.nan).astype("float32")


def save_file(local, path):
    """Copy a local file to `path`; fd:// and s3:// paths are uploaded. Returns `path`."""
    import shutil

    if str(path).startswith(("fd://", "s3://")):
        fused.api.upload(local, path)
    elif local != path:
        shutil.copy(local, path)
    return path


def _odc(obj):
    import odc.geo.xr

    return obj.odc


def _day(value):
    import pandas as pd

    try:
        return pd.Timestamp(str(value)[:10]).strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"dates must be YYYY-MM-DD, got {value!r}") from None


class BestFill:
    """Best-first pixel fill shared by tiles and exports.

    Feed observations best first with `add(data, valid, clear)`. The first clear value of a pixel wins. A pixel that
    is never clear gets the median of its valid values (`keep_obs=True`) or its darkest value in band `key_band`.
    """

    def __init__(self, nb, shape, keep_obs=True, key_band=0):
        import numpy as np

        self.out = np.full((nb,) + tuple(shape), np.nan, dtype="float32")
        self.filled = np.zeros(shape, bool)
        self.seen = np.zeros(shape, bool)
        self.keep_obs, self.key_band, self.obs = keep_obs, key_band, []
        if not keep_obs:
            self.dark = np.full(shape, np.inf, dtype="float32")
            self.dark_val = np.full((nb,) + tuple(shape), np.nan, dtype="float32")

    def add(self, data, valid, clear):
        import numpy as np

        take = clear & ~self.filled
        self.out[:, take] = data[:, take]
        self.filled |= take
        self.seen |= valid
        if self.keep_obs:
            self.obs.append(np.where(valid[None], data, np.nan))
        else:
            darker = valid & (data[self.key_band] < self.dark)
            self.dark[darker] = data[self.key_band][darker]
            self.dark_val[:, darker] = data[:, darker]

    def clear_share(self):
        """Share of the pixels with data that are clear (0.0 when no pixel has data)."""
        return float(self.filled[self.seen].mean()) if self.seen.any() else 0.0

    def result(self):
        """(data float32 (nb, H, W), alpha bool (H, W)). Data is NaN where no observation has data."""
        import numpy as np

        holes = ~self.filled & self.seen
        if holes.any():
            if self.keep_obs:
                self.out[:, holes] = nanquantile(np.stack([o[:, holes] for o in self.obs]), 0.5)
            else:
                self.out[:, holes] = self.dark_val[:, holes]
        return self.out, self.filled | self.seen


E84_API = "https://earth-search.aws.element84.com/v1/search"
COOP = "https://data.source.coop/tge-labs/s2-stac-geoparquet/sentinel-2-c1-l2a"
FUSED_INDEX = "s3://fused-asset/stac/sentinel-2-l2a/partitioned_2015_2025/"
LEGACY_COG = "https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs"
COMMON = "https://github.com/fusedio/udfs/tree/b6786d6/public/common/"
SOURCES = ("auto", "c1", "coop", "legacy", "index")
SOURCE_RANK = {"c1": 0, "coop": 0, "legacy": 1, "index": 2}
SCENE_COLUMNS = ["id", "datetime", "date", "mgrs", "platform", "cloud_cover", "nodata_pct", "epsg", "baseline", "source", "offset"]
STAC_PAGE = 200
COARSE_MARGIN = 1.3


def search(aoi, start, end, *, max_cloud=MAX_CLOUD, source="auto", limit=None):
    """Scenes that touch `aoi` with local solar `date` from `start` to `end` (YYYY-MM-DD, inclusive), tile cloud <=
    max_cloud.

    Returns a GeoDataFrame in time order with SCENE_COLUMNS + href_<asset> + geometry (the data footprint).
    source: auto | c1 | coop | legacy | index (see README.MD). `limit` keeps the first scenes and warns when it cuts.
    """
    import warnings
    import pandas as pd

    if source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES}")
    start, end = _day(start), _day(end)
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    lo = (pd.Timestamp(start) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    hi = (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    key = (tuple(round(v, 6) for v in bbox(aoi)), lo, hi, float(max_cloud))
    if source == "auto":
        parts = [_try(_search_c1, _search_coop, key)]
        gap = (max(lo, C1_GAP[0]), min(hi, C1_GAP[1]))
        if gap[0] <= gap[1]:
            parts.append(_try(_search_legacy, _search_index, (key[0], gap[0], gap[1], key[3])))
    else:
        reader = {"c1": _search_c1, "coop": _search_coop, "legacy": _search_legacy, "index": _search_index}[source]
        parts = [reader(*key)]
    out = _finish(parts, start, end)
    if limit and len(out) > int(limit):
        warnings.warn(f"search found {len(out)} scenes; keeping the first {limit}")
        out = out.head(int(limit))
    return out


def rank(scenes, aoi=None, *, date=None, per_mgrs=None, order="clear"):
    """Scenes best first, at most `per_mgrs` per MGRS tile; each tile's best scene before any tile's second best.

    order: clear (most clear area first, then nearest `date`) | date (nearest date first, then clear area).
    Adds clear_pct = (100 - nodata %) * (100 - cloud %) / 100, days_off (solar days from `date`) and mgrs_rank.
    """
    import pandas as pd
    import shapely

    if order not in ("clear", "date"):
        raise ValueError("order must be clear or date")
    sc = scenes
    if aoi is not None and len(sc):
        sc = sc[sc.intersects(shapely.box(*bbox(aoi)))]
    sc = sc.copy()
    if not len(sc):
        return sc.assign(clear_pct=[], days_off=[], mgrs_rank=[])
    sc["clear_pct"] = ((100 - sc["nodata_pct"].fillna(0)) * (100 - sc["cloud_cover"].fillna(100)) / 100).round(2)
    sc["days_off"] = (pd.to_datetime(sc["date"]) - pd.Timestamp(_day(date))).abs().dt.days if date else 0
    sc["_unclear"] = -sc["clear_pct"]
    keys = ["_unclear", "days_off"] if order == "clear" else ["days_off", "_unclear"]
    sc = sc.sort_values(keys + ["id"])
    if per_mgrs:
        sc = sc.groupby("mgrs").head(int(per_mgrs))
    sc["mgrs_rank"] = sc.groupby("mgrs").cumcount()
    return sc.sort_values(["mgrs_rank"] + keys + ["id"]).drop(columns="_unclear").reset_index(drop=True)


def dates(scenes):
    """One row per solar date: MGRS tiles, mean tile cloud, sources."""
    import pandas as pd

    if not len(scenes):
        return pd.DataFrame(columns=["date", "mgrs", "cloud_cover", "sources"])
    return scenes.groupby("date").agg(mgrs=("mgrs", "nunique"), cloud_cover=("cloud_cover", "mean"),
                                      sources=("source", lambda s: ",".join(sorted(set(s))))).reset_index()


def to_json(scenes, bands):
    """Compact JSON for passing scenes to workers: SCENE_COLUMNS, the footprint and the hrefs of `bands`."""
    import json
    import shapely

    names = band_names(bands)
    rows = []
    for _, r in scenes.iterrows():
        row = {c: (r[c].item() if hasattr(r[c], "item") else r[c]) for c in SCENE_COLUMNS}
        row.update(footprint=shapely.to_wkt(r.geometry, rounding_precision=6), hrefs={n: r[f"href_{n}"] for n in names})
        rows.append(row)
    return json.dumps(rows)


def from_json(text):
    """Inverse of to_json. An empty list gives an empty scene table."""
    import json
    import geopandas as gpd
    import shapely

    rows = json.loads(text) if text else []
    if not rows:
        return _empty_scenes()
    out = []
    for s in rows:
        row = {k: v for k, v in s.items() if k not in ("hrefs", "footprint")}
        row.update({f"href_{k}": v for k, v in s["hrefs"].items()}, geometry=shapely.from_wkt(s["footprint"]))
        out.append(row)
    return gpd.GeoDataFrame(out, geometry="geometry", crs="EPSG:4326")


def _try(first, fallback, key):
    import warnings

    try:
        return first(*key)
    except Exception as e:
        names = [f.__name__.replace("_search_", "") for f in (first, fallback)]
        warnings.warn(f"{names[0]} failed ({str(e)[:80]}); using {names[1]}")
        return fallback(*key)


def _finish(parts, start, end):
    import geopandas as gpd
    import pandas as pd

    parts = [p for p in parts if p is not None and len(p)]
    if not parts:
        return _empty_scenes()
    df = pd.concat(parts, ignore_index=True)
    lon = df["geometry"].map(lambda g: g.centroid.x)
    t = pd.to_datetime(df["datetime"], utc=True)
    df["date"] = (t + pd.to_timedelta(lon / 15, unit="h")).dt.strftime("%Y-%m-%d")
    df = df[(df["date"] >= start) & (df["date"] <= end)]
    df = df.assign(_r=df["source"].map(SOURCE_RANK)).sort_values(["_r", "baseline"], ascending=[True, False]).drop_duplicates("id")
    acquisition = df["mgrs"] + "_" + df["date"] + "_" + df["platform"]
    df = df[df["_r"] == df.groupby(acquisition)["_r"].transform("min")]
    df = df.drop(columns="_r").sort_values(["datetime", "id"]).reset_index(drop=True)
    cols = SCENE_COLUMNS + [f"href_{a}" for a in ASSETS]
    return gpd.GeoDataFrame(df.reindex(columns=cols + ["geometry"]), geometry="geometry", crs="EPSG:4326")


def _empty_scenes():
    import geopandas as gpd

    return gpd.GeoDataFrame({c: [] for c in SCENE_COLUMNS + [f"href_{a}" for a in ASSETS]}, geometry=[], crs="EPSG:4326")


def _search_stac(bounds, start, end, max_cloud, collection, source, offset, max_pages=50):
    import pandas as pd
    import requests
    import shapely

    body = {"collections": [collection], "bbox": list(bounds), "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
            "query": {"eo:cloud_cover": {"lte": max_cloud}}, "limit": STAC_PAGE}
    rows, url = [], E84_API
    for _ in range(max_pages):
        r = requests.post(url, json=body, timeout=60)
        r.raise_for_status()
        js = r.json()
        for f in js["features"]:
            p, a = f["properties"], f["assets"]
            rows.append({
                "id": f["id"], "datetime": p["datetime"][:19] + "Z",
                "mgrs": f"{p['mgrs:utm_zone']:02d}{p['mgrs:latitude_band']}{p['mgrs:grid_square']}",
                "platform": p["platform"].replace("sentinel-", "S").upper(), "cloud_cover": p.get("eo:cloud_cover"),
                "nodata_pct": p.get("s2:nodata_pixel_percentage"), "epsg": p.get("proj:epsg"),
                "baseline": p.get("s2:processing_baseline"), "source": source, "offset": offset,
                "geometry": shapely.geometry.shape(f["geometry"]),
                **{f"href_{n}": a.get(n, {}).get("href") for n in ASSETS},
            })
        nxt = [link for link in js.get("links", []) if link.get("rel") == "next"]
        if not nxt:
            return pd.DataFrame(rows)
        url, body = nxt[0]["href"], nxt[0].get("body", body)
    raise RuntimeError(f"more than {max_pages * STAC_PAGE} scenes: narrow the area or the dates")


@fused.cache(cache_max_age="24h")
def _search_c1(bounds, start, end, max_cloud):
    return _search_stac(bounds, start, end, max_cloud, "sentinel-2-c1-l2a", "c1", -0.1)


@fused.cache(cache_max_age="24h")
def _search_legacy(bounds, start, end, max_cloud):
    return _search_stac(bounds, start, end, max_cloud, "sentinel-2-l2a", "legacy", 0.0)


@fused.cache(cache_max_age="24h")
def _search_coop(bounds, start, end, max_cloud):
    import datetime
    import math
    import duckdb
    import pandas as pd
    import shapely

    w, s, e, n = bounds
    years = range(max(int(start[:4]), 2015), min(int(end[:4]), datetime.datetime.now(datetime.timezone.utc).year) + 1)
    if not len(years):
        return pd.DataFrame()
    mlon = min(1.0 / max(math.cos(math.radians(max(abs(s), abs(n)))), 0.1) + 0.3, 20)
    files = ", ".join(f"'{COOP}/year={y}/items.parquet'" for y in years)
    hrefs = ", ".join(f"json_extract_string(assets, '$.{a}.href') AS href_{a}" for a in ASSETS)
    con = duckdb.connect()
    con.sql("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; SET TimeZone='UTC';")
    df = con.sql(f"""
        SELECT id, strftime(datetime, '%Y-%m-%dT%H:%M:%SZ') AS datetime, _tile AS mgrs,
               upper(replace(platform, 'sentinel-', 'S')) AS platform, "eo:cloud_cover" AS cloud_cover,
               "s2:nodata_pixel_percentage" AS nodata_pct, "proj:epsg" AS epsg, "s2:processing_baseline" AS baseline,
               'coop' AS source, -0.1 AS offset, {hrefs}, ST_AsWKB(geometry) AS wkb
        FROM read_parquet([{files}])
        WHERE "proj:centroid".lon BETWEEN {w - mlon} AND {e + mlon}
          AND "proj:centroid".lat BETWEEN {s - COARSE_MARGIN} AND {n + COARSE_MARGIN}
          AND bbox[1] <= {e} AND bbox[3] >= {w} AND bbox[2] <= {n} AND bbox[4] >= {s}
          AND datetime >= '{start}' AND datetime <= '{end} 23:59:59' AND "eo:cloud_cover" <= {max_cloud}
    """).df()
    con.close()
    df["geometry"] = shapely.from_wkb(df.pop("wkb").apply(bytes)) if len(df) else []
    return df[shapely.intersects(df["geometry"].values, shapely.box(*bounds))] if len(df) else df


@fused.cache(cache_max_age="24h")
def _search_index(bounds, start, end, max_cloud):
    import re
    import pandas as pd
    import shapely

    common = fused.load(COMMON)
    cols = ["s2:product_uri", "s2:processing_baseline", "s2:nodata_pixel_percentage", "eo:cloud_cover", "proj:epsg", "datetime", "geometry"]
    df = common.read_table_chunks(common.table_chunk_overlaps(list(bounds), table_path=FUSED_INDEX), columns=cols)
    t = pd.to_datetime(df["datetime"], utc=True)
    df = df[shapely.intersects(df.geometry.values, shapely.box(*bounds)) & (df["eo:cloud_cover"] <= max_cloud)
            & (t >= pd.Timestamp(start, tz="UTC")) & (t < pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1))]
    pat = re.compile(r"(S2[A-Z])_MSIL2A_(\d{8})T\d+_N\d+_R\d+_T(\d{1,2})([A-Z])([A-Z]{2})_")
    rows = []
    for _, r in df.iterrows():
        m = pat.search(str(r["s2:product_uri"]))
        if not m:
            continue
        mission, day, utm, lat_band, square = m.groups()
        mgrs = f"{int(utm):02d}{lat_band}{square}"
        sid = f"{mission}_{mgrs}_{day}_0_L2A"
        base = f"{LEGACY_COG}/{int(utm)}/{lat_band}/{square}/{day[:4]}/{int(day[4:6])}/{sid}"
        rows.append({
            "id": sid, "datetime": pd.Timestamp(r["datetime"]).strftime("%Y-%m-%dT%H:%M:%SZ"), "mgrs": mgrs,
            "platform": mission, "cloud_cover": r["eo:cloud_cover"], "nodata_pct": r["s2:nodata_pixel_percentage"],
            "epsg": r["proj:epsg"], "baseline": r["s2:processing_baseline"], "source": "index", "offset": 0.0,
            "geometry": r.geometry,
            **{f"href_{n}": None if n in ("cloud", "snow") else f"{base}/{band_info(n)['code']}.tif" for n in ASSETS},
        })
    return pd.DataFrame(rows)


def load(scenes, bands, aoi=None, *, res=10, crs="utm", shape=None, like=None, keep=None, merge="day",
         max_pixels=MAX_PIXELS, threads=16):
    """Scenes (GeoDataFrame or to_json text) -> Dataset (time, y, x), one variable per band.

    Reflectance is float32 DN * 0.0001 + the scene's own offset; nodata is NaN. Mask bands keep their values:
    scl is the class code, cloud and snow are probabilities in % (0 is a value; NaN only outside the scene). Asking
    for cloud or snow also loads scl.
    keep: an SCL group (clear, land, water ...) applies `mask()` and loads scl when needed.
    merge: "day" merges the scenes of each local solar day; None keeps one step per scene, with a `scene` coordinate.
    The grid is `like` (GeoBox or xarray object), else `aoi` (default: the scenes' extent) at `res` or `shape`.
    """
    import odc.stac
    import pandas as pd
    import rasterio
    import xarray as xr

    if merge not in ("day", None):
        raise ValueError("merge must be 'day' or None")
    if isinstance(scenes, str):
        scenes = from_json(scenes)
    names = band_names(bands)
    if (keep or {"cloud", "snow"} & set(names)) and "scl" not in names:
        names.append("scl")
    missing = [n for n in names if f"href_{n}" not in scenes.columns]
    if missing:
        raise ValueError(f"scenes have no href for {missing}")
    scenes = scenes[scenes[[f"href_{n}" for n in names]].notna().all(axis=1)].sort_values(["datetime", "id"], kind="stable")
    if not len(scenes):
        raise NoData("no scene has every requested band")
    gb = geobox(aoi if aoi is not None else list(scenes.total_bounds), res, crs, shape, like)
    n_values = gb.shape[0] * gb.shape[1] * len(scenes) * len(names)
    if n_values > max_pixels:
        raise ValueError(f"{gb.shape[0]}x{gb.shape[1]} px x {len(scenes)} scenes x {len(names)} bands = {n_values:,} > "
                         f"max_pixels {max_pixels:,}; use a coarser res, fewer scenes (rank per_mgrs) or a smaller aoi")
    refl = [n for n in names if not is_mask(n)]
    masks = [n for n in names if is_mask(n)]
    parts = []
    with rasterio.Env(**GDAL_ENV):
        for offset, group in scenes.groupby("offset"):
            got = []
            items = _items(group, names)
            if refl:
                ds = odc.stac.load(items, bands=refl, geobox=gb, dtype="float32", nodata=0, resampling="bilinear",
                                   groupby="id", pool=threads, fail_on_error=False)
                got.append(ds.where(ds > 0) * SCALE + float(offset))
            if masks:
                dm = odc.stac.load(items, bands=masks, geobox=gb, dtype="uint8", nodata=0, resampling="nearest",
                                   groupby="id", pool=threads, fail_on_error=False)
                got.append(dm.where(dm > 0).astype("float32"))
            part = xr.merge(got, compat="override")
            if part.sizes["time"] != len(group):
                raise RuntimeError(f"odc-stac returned {part.sizes['time']} time steps for {len(group)} scenes")
            times = pd.to_datetime(group["datetime"], utc=True).dt.tz_localize(None).values
            part = part.assign_coords(time=times, scene=("time", list(group["id"])))
            for b in ("cloud", "snow"):
                if b in part:
                    part[b] = part[b].fillna(0).where(part["scl"].notnull())
            parts.append(part)
    ds = xr.concat(parts, dim="time").sortby("time") if len(parts) > 1 else parts[0]
    if merge == "day":
        bb = gb.extent.to_crs("EPSG:4326").boundingbox
        ds = _by_day(ds, lon=(bb.left + bb.right) / 2)
    ds = keep_crs(ds.astype("float32"), parts[0])
    ds.attrs.update({"res": abs(gb.resolution.x), "crs": str(gb.crs), "scenes": len(scenes)})
    return mask(ds, keep) if keep else ds


def mask(ds, keep="clear", *, cloud_prob=None, dilate=0, rescue=True):
    """Set reflectance to NaN where `clear_mask` is False. Mask bands are kept as they are."""
    ok = clear_mask(ds, keep, cloud_prob=cloud_prob, dilate=dilate, rescue=rescue)
    out = ds.copy()
    for v in reflectance(ds).data_vars:
        out[v] = ds[v].where(ok)
    out.attrs.update(mask=str(scl_classes(keep)))
    return out


def clear_mask(ds, keep="clear", *, cloud_prob=None, dilate=0, rescue=True):
    """Boolean DataArray (time, y, x): True where SCL is in `keep`.

    cloud_prob: also require the cloud probability band below this %. dilate: grow cloud (SCL 8, 9, 10) by this many
    pixels in every direction (3 x 3 square). rescue: when `keep` holds every clear class (4, 5, 6), a pixel that is
    never clear on any date (building shadow, bright roof) gets back its SCL 1, 2, 3, 7, 11 dates, so composites
    have no permanent holes. Cloud stays masked, and a narrower `keep` (land, water) is never widened.
    """
    import numpy as np

    if "scl" not in ds:
        raise ValueError("load the scl band to mask (load(..., keep='clear') adds it)")
    classes = scl_classes(keep)
    scl = ds["scl"]
    ok = scl.isin(list(classes))
    if cloud_prob is not None:
        if "cloud" not in ds:
            raise ValueError("cloud_prob needs the cloud band: load it with the other bands")
        ok = ok & (ds["cloud"] < float(cloud_prob))
    if dilate and int(dilate) > 0:
        from scipy.ndimage import binary_dilation

        cloud = scl.isin(list(SCL_GROUPS["cloud"])).values
        grown = np.stack([binary_dilation(c, structure=np.ones((3, 3), bool), iterations=int(dilate)) for c in cloud])
        ok = ok & ~ok.copy(data=grown)
    if rescue and set(SCL_GROUPS["clear"]) <= set(classes):
        never = ~scl.isin(list(classes)).any("time")
        ok = ok | (never & scl.isin(list(RESCUE)))
    return ok


def aoi_cloud(scenes, aoi, *, keep="clear", max_px=512):
    """Per solar date: clear, cloud, shadow, snow, water and covered % inside the AOI (SCL at a coarse grid).

    The tile `cloud_cover` of a scene row is for the whole 110 km tile; this measures the AOI itself. Long scene
    lists are read in batches, so any time range works.
    """
    import pandas as pd

    cols = ["date", "clear_pct", "cloud_pct", "shadow_pct", "snow_pct", "water_pct", "covered_pct", "tile_cloud"]
    if not len(scenes):
        return pd.DataFrame(columns=cols)
    bounds = bbox(aoi)
    res = auto_res(bounds, max_px, 20)
    gb = geobox(bounds, res)
    per_batch = max(1, MAX_PIXELS // (gb.shape[0] * gb.shape[1]))
    days = sorted(scenes["date"].unique())
    rows, batch, n = [], [], 0
    for d in days + [None]:
        k = int((scenes["date"] == d).sum()) if d is not None else 0
        if d is None or (batch and n + k > per_batch):
            scl = load(scenes[scenes["date"].isin(batch)], "scl", like=gb)["scl"]
            seen = scl.notnull().sum(["x", "y"])
            pct = lambda codes: (scl.isin(list(codes)).sum(["x", "y"]) / seen.clip(min=1) * 100).values.round(1)
            rows.append(pd.DataFrame({
                "date": pd.to_datetime(scl["time"].values).strftime("%Y-%m-%d"),
                "clear_pct": pct(scl_classes(keep)), "cloud_pct": pct(SCL_GROUPS["cloud"]),
                "shadow_pct": pct(SCL_GROUPS["shadow"]), "snow_pct": pct((11,)), "water_pct": pct(SCL_GROUPS["water"]),
                "covered_pct": (seen / (scl.sizes["x"] * scl.sizes["y"]) * 100).values.round(1),
            }))
            batch, n = [], 0
        if d is not None:
            batch.append(d)
            n += k
    out = pd.concat(rows, ignore_index=True)
    out["tile_cloud"] = out["date"].map(scenes.groupby("date")["cloud_cover"].mean().round(1))
    return out[cols]


def clear_scenes(scenes, aoi, *, min_clear=80, min_covered=50, keep="clear", max_px=512):
    """Scenes whose solar date is at least `min_clear` % clear inside the AOI and covers `min_covered` % of it."""
    stats = aoi_cloud(scenes, aoi, keep=keep, max_px=max_px)
    good = set(stats.loc[(stats["clear_pct"] >= min_clear) & (stats["covered_pct"] >= min_covered), "date"])
    return scenes[scenes["date"].isin(good)].reset_index(drop=True)


def scl_summary(ds):
    """Share of each SCL class per time step (one column per class)."""
    import pandas as pd

    scl = ds["scl"]
    seen = scl.notnull().sum(["x", "y"]).clip(min=1)
    out = {"time": pd.to_datetime(ds["time"].values)}
    out.update({name: ((scl == code).sum(["x", "y"]) / seen).values.round(4) for code, name in SCL.items() if code})
    return pd.DataFrame(out)


def pixel_series(lat, lng, start, end, bands="red,nir", *, max_cloud=MAX_CLOUD, source="auto", clear_only=False, threads=64):
    """One row per solar date at a point: scl and `bands`. One pixel read per scene and band.

    Reflectance bands are scaled with each scene's offset; cloud and snow are probabilities in %. A 10 m band is
    stored in 1024 x 1024 blocks (~1.8 MB), so each read moves one block: this is bandwidth-bound. clear_only reads
    SCL first (much smaller blocks), reads the bands only where SCL is clear, and drops the other dates.
    Read errors other than "outside the scene" are raised, not stored as NaN.
    """
    names = [n for n in band_names(bands) if n != "scl"]
    return _pixel_series(round(float(lat), 6), round(float(lng), 6), _day(start), _day(end), ",".join(names),
                         float(max_cloud), source, bool(clear_only), int(threads))


@fused.cache(cache_max_age="24h")
def _pixel_series(lat, lng, start, end, bands, max_cloud, source, clear_only, threads):
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np
    import pandas as pd
    import rasterio

    names = [n for n in bands.split(",") if n]
    d = 1e-4
    scenes = search([lng - d, lat - d, lng + d, lat + d], start, end, max_cloud=max_cloud, source=source)
    cols = ["date", "datetime", "id", "mgrs", "source", "cloud_cover", "scl"] + names
    if not len(scenes):
        return pd.DataFrame(columns=cols)
    rows = scenes.reset_index(drop=True)

    def read(job):
        i, n = job
        href = rows.at[i, f"href_{n}"]
        v = _read_pixel(href, lat, lng) if isinstance(href, str) else np.nan
        return i, n, v if (is_mask(n) or v != v) else v * SCALE + float(rows.at[i, "offset"])

    out = rows[["date", "datetime", "id", "mgrs", "source", "cloud_cover"]].copy()
    with rasterio.Env(**GDAL_ENV), ThreadPoolExecutor(threads) as ex:
        scl = list(ex.map(read, [(i, "scl") for i in range(len(rows))]))
        out["scl"] = [v for _, _, v in scl]
        keep = [i for i, _, v in scl if v == v and int(v) in SCL_GROUPS["clear"]] if clear_only else list(range(len(rows)))
        for n in names:
            out[n] = np.nan
        for i, n, v in ex.map(read, [(i, n) for i in keep for n in names]):
            out.at[i, n] = v
    for n in names:
        if n in ("cloud", "snow"):
            out[n] = out[n].where(out[n].notna() | out["scl"].isna() | rows[f"href_{n}"].isna(), 0.0)
    if clear_only:
        out = out.loc[keep]
    out["_valid"] = out[names].notnull().sum(axis=1) if names else out["scl"].notnull().astype(int)
    out = out.sort_values(["date", "_valid"], ascending=[True, False]).drop_duplicates("date").drop(columns="_valid")
    return out[cols].reset_index(drop=True)


def _read_pixel(href, lat, lng, retries=2):
    import time
    import numpy as np
    import rasterio
    from rasterio.warp import transform
    from rasterio.windows import Window

    for attempt in range(retries + 1):
        try:
            with rasterio.open(href) as src:
                xs, ys = transform("EPSG:4326", src.crs, [lng], [lat])
                row, col = src.index(xs[0], ys[0])
                if not (0 <= row < src.height and 0 <= col < src.width):
                    return np.nan
                v = float(src.read(1, window=Window(col, row, 1, 1))[0, 0])
                return v if v > 0 else np.nan
        except rasterio.errors.RasterioIOError as e:
            if any(s in str(e).lower() for s in ("404", "does not exist", "no such file")):
                return np.nan
            if attempt == retries:
                raise
            time.sleep(0.5 * (attempt + 1))


def _items(scenes, names):
    from datetime import datetime, timedelta, timezone
    import pystac
    import shapely

    items, last = [], None
    for _, r in scenes.iterrows():
        dt = datetime.fromisoformat(str(r["datetime"]).replace("Z", "+00:00"))
        dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        if last is not None and dt <= last:
            dt = last + timedelta(microseconds=1)
        last = dt
        bb = list(r.geometry.bounds)
        item = pystac.Item(id=r["id"], geometry=shapely.geometry.mapping(shapely.box(*bb)), bbox=bb, datetime=dt,
                           properties={"proj:epsg": int(r["epsg"])})
        for n in names:
            item.add_asset(n, pystac.Asset(href=r[f"href_{n}"], media_type="image/tiff; application=geotiff; profile=cloud-optimized", roles=["data"]))
        items.append(item)
    return items


def _by_day(ds, lon):
    import numpy as np

    day = (ds["time"] + np.timedelta64(int(lon / 15 * 3600), "s")).dt.floor("D")
    if day.to_series().is_unique:
        return ds.assign_coords(time=day.values).drop_vars("scene", errors="ignore")
    out = ds.drop_vars("scene", errors="ignore").assign_coords(day=("time", day.values)).groupby("day").first(skipna=True)
    return keep_crs(out.rename(day="time"), ds)


ASI_URL = "https://raw.githubusercontent.com/awesome-spectral-indices/awesome-spectral-indices/1b032ca8ca1fdadfc730d1abec849bd337a8a05a/output"
FORMULA_MAX_LEN = 1000
FORMULA_MAX_NODES = 300
FORMULA_FUNCS = {"where": 3, "sqrt": 1, "log": 1, "log10": 1, "exp": 1, "abs": 1, "tanh": 1, "sin": 1, "cos": 1, "tan": 1,
                 "arctan": 1, "min": 2, "max": 2, "clip": 3}
INDEX_ALIASES = {"NDRE": "NDREI", "BSI": "BI"}
_CATALOG = {}


def index(ds, what, *, name=None, **constants):
    """One catalog index (Awesome Spectral Indices) or formula on a Dataset of reflectance. Returns a float32 DataArray.

    Names in a formula resolve in this order: index band letters in upper case (A B G R RE1 RE2 RE3 N N2 WV S1 S2),
    constants (catalog defaults such as g, n, L, or keyword arguments), wavelengths (lambdaN ... in nm), then band
    names and codes (red, nir, B04, b8a ...). A formula may use numbers, + - * / ** %, comparisons, and / or / not,
    `x if c else y` and FORMULA_FUNCS. True is 1.0 and false is 0.0, so conditions can be added up. The formula is
    parsed with an allow-list and never passed to eval(). The result is NaN wherever an input band is NaN.
    The DataArray is named `name`, else the catalog name, else "value"; attrs hold the formula.
    """
    parts = _split(what)
    if len(parts) != 1:
        raise ValueError(f"index takes one index or formula, got {len(parts)}; use indices() for several")
    key = _index_key(parts[0])
    catalog, const = index_catalog()
    formula = catalog[key]["formula"] if key else parts[0]
    da = _evaluate(ds, formula, {**const, **constants})
    da.attrs = {"formula": formula, "long_name": catalog[key]["long_name"] if key else formula}
    return keep_crs(da.rename(name or key or "value"), ds)


def indices(ds, what, **constants):
    """Several catalog indices or formulas (a list, or a comma string such as "NDVI, max(N, R)").

    Returns a Dataset with one variable per item, named as written.
    """
    import xarray as xr

    out = xr.Dataset({n: index(ds, n, name=n, **constants) for n in _split(what)})
    out.attrs = dict(ds.attrs)
    return keep_crs(out, ds)


def needs(what, **constants):
    """Bands to load for a preset, a catalog index, a formula, or a list / comma string of them."""
    if isinstance(what, str) and what in PRESETS:
        return list(PRESETS[what][0])
    catalog, const = index_catalog()
    const = {**const, **constants}
    out = []
    for part in _split(what):
        key = _index_key(part)
        for n in sorted(parse_formula(catalog[key]["formula"] if key else part)[1]):
            kind, v = _resolve(n, const)
            if kind == "band" and v not in out:
                out.append(v)
    return out


def is_index(name):
    """True if `name` is a catalog index (or alias). Raises ValueError for a name that matches two indices that differ
    only in case (bai: BAI and BaI); write the exact name."""
    return isinstance(name, str) and _index_key(name) is not None


def index_catalog():
    """(indices, constants): the Sentinel-2 indices of Awesome Spectral Indices (pinned commit), kept per worker."""
    if not _CATALOG:
        idx, const = _download_catalog()
        _CATALOG["indices"] = {k: {"formula": v["formula"], "domain": v["application_domain"], "long_name": v["long_name"],
                                   "reference": v.get("reference", "")}
                               for k, v in idx.items()
                               if "Sentinel-2" in v.get("platforms", []) and v.get("application_domain") != "kernel"}
        _CATALOG["constants"] = const
    return _CATALOG["indices"], dict(_CATALOG["constants"])


def index_info(name):
    """Catalog entry as a dict: index, formula, domain, long_name, reference, bands, constants."""
    key = _index_key(name)
    if not key:
        raise ValueError(f"unknown index {name!r}; see list_indices()")
    catalog, const = index_catalog()
    entry = catalog[key]
    return {"index": key, **entry, "bands": needs(key),
            "constants": {n: const[n] for n in parse_formula(entry["formula"])[1] if n in const}}


def list_indices(domain=None, text=None):
    """The catalog as a table, filtered by domain (vegetation, water, burn, snow, urban, soil, clouds) and text."""
    import pandas as pd

    rows = []
    for k, v in index_catalog()[0].items():
        if (domain and v["domain"] != domain) or (text and text.lower() not in f"{k} {v['long_name']}".lower()):
            continue
        try:
            bands = ",".join(needs(k))
        except ValueError:
            bands = "needs an external input"
        rows.append({"index": k, "domain": v["domain"], "long_name": v["long_name"], "formula": v["formula"], "bands": bands})
    return pd.DataFrame(rows, columns=["index", "domain", "long_name", "formula", "bands"])


def parse_formula(expr):
    """Validate a formula. Returns (ast tree, variable names, function names); raises ValueError on anything else."""
    import ast

    expr = str(expr).strip()
    if not expr or len(expr) > FORMULA_MAX_LEN:
        raise ValueError(f"formula must be 1..{FORMULA_MAX_LEN} characters")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"cannot parse formula {expr!r}: {e.msg}") from None
    nodes = list(ast.walk(tree))
    if len(nodes) > FORMULA_MAX_NODES:
        raise ValueError(f"formula too long ({len(nodes)} nodes > {FORMULA_MAX_NODES})")
    allowed = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Compare, ast.BoolOp, ast.IfExp, ast.Load, ast.Name, ast.Add,
               ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod, ast.USub, ast.UAdd, ast.Not, ast.Gt, ast.Lt, ast.GtE, ast.LtE,
               ast.Eq, ast.NotEq, ast.And, ast.Or)
    funcs = set()
    for node in nodes:
        if isinstance(node, allowed):
            continue
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
            continue
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in FORMULA_FUNCS and not node.keywords:
            if len(node.args) != FORMULA_FUNCS[node.func.id] or any(isinstance(a, ast.Starred) for a in node.args):
                raise ValueError(f"{node.func.id}() takes {FORMULA_FUNCS[node.func.id]} arguments")
            funcs.add(node.func.id)
            continue
        raise ValueError(f"not allowed in a formula: {type(node).__name__}; functions: {list(FORMULA_FUNCS)}")
    calls = {id(n.func) for n in nodes if isinstance(n, ast.Call)}
    return tree, {n.id for n in nodes if isinstance(n, ast.Name) and id(n) not in calls}, funcs


@fused.cache(cache_max_age="30d")
def _download_catalog():
    import requests

    idx = requests.get(f"{ASI_URL}/spectral-indices-dict.json", timeout=30).json()["SpectralIndices"]
    const = requests.get(f"{ASI_URL}/constants.json", timeout=30).json()
    return idx, {k: v.get("default") for k, v in const.items()}


def _index_key(name):
    catalog = index_catalog()[0]
    name = INDEX_ALIASES.get(str(name).upper(), str(name))
    if name in catalog:
        return name
    found = [k for k in catalog if k.lower() == name.lower()]
    if len(found) > 1:
        raise ValueError(f"{name!r} matches the indices {found}; write the exact name")
    return found[0] if found else None


def _split(what):
    import ast

    if isinstance(what, (list, tuple)):
        parts = [str(w).strip() for w in what]
    else:
        text = str(what).strip()
        try:
            node = ast.parse(text, mode="eval").body
        except SyntaxError as e:
            raise ValueError(f"cannot parse {text!r}: {e.msg}") from None
        parts = [ast.get_source_segment(text, e).strip() for e in node.elts] if isinstance(node, ast.Tuple) else [text]
    if not parts or not all(parts):
        raise ValueError(f"nothing to compute in {what!r}")
    return parts


def _resolve(name, constants):
    letters = {b[2]: b[0] for b in BANDS}
    if name in letters:
        return "band", letters[name]
    if constants.get(name) is not None:
        return "value", float(constants[name])
    if name.startswith("lambda") and name[6:] in letters:
        return "value", float(band_info(letters[name[6:]])["wavelength"])
    if name.upper() in letters:
        raise ValueError(f"{name!r} in a formula: write band letters in upper case ({name.upper()}), as in the catalog")
    try:
        return "band", band_name(name)
    except ValueError:
        raise ValueError(f"unknown name {name!r} in formula: not a band and no value given for this constant") from None


def _evaluate(ds, expr, constants):
    import ast
    import operator as op
    import numpy as np
    import xarray as xr

    tree, names, _ = parse_formula(expr)
    env = {}
    for n in names:
        kind, v = _resolve(n, constants)
        if kind == "band" and v not in ds:
            raise ValueError(f"formula uses {n!r} (band {v!r}), which is not loaded; load {needs(expr, **constants)}")
        env[n] = v if kind == "value" else ds[v]

    def num(b):
        return b.astype("float32") if hasattr(b, "astype") else float(b)

    def truth(v):
        return v != 0

    fns = {"where": lambda c, a, b: xr.where(truth(c), a, b), "sqrt": np.sqrt, "log": np.log, "log10": np.log10,
           "exp": np.exp, "abs": np.abs, "tanh": np.tanh, "sin": np.sin, "cos": np.cos, "tan": np.tan,
           "arctan": np.arctan, "min": np.fmin, "max": np.fmax, "clip": np.clip}
    bins = {ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul, ast.Div: op.truediv, ast.Pow: op.pow, ast.Mod: op.mod}
    cmps = {ast.Gt: op.gt, ast.Lt: op.lt, ast.GtE: op.ge, ast.LtE: op.le, ast.Eq: op.eq, ast.NotEq: op.ne}

    def ev(node):
        if isinstance(node, ast.Expression):
            return ev(node.body)
        if isinstance(node, ast.Constant):
            return float(node.value)
        if isinstance(node, ast.Name):
            return env[node.id]
        if isinstance(node, ast.BinOp):
            return bins[type(node.op)](ev(node.left), ev(node.right))
        if isinstance(node, ast.UnaryOp):
            v = ev(node.operand)
            if isinstance(node.op, ast.Not):
                return num(np.logical_not(truth(v)))
            return -v if isinstance(node.op, ast.USub) else v
        if isinstance(node, ast.Compare):
            left, out = ev(node.left), True
            for o, c in zip(node.ops, node.comparators):
                right = ev(c)
                out = np.logical_and(out, cmps[type(o)](left, right))
                left = right
            return num(out)
        if isinstance(node, ast.BoolOp):
            join = np.logical_and if isinstance(node.op, ast.And) else np.logical_or
            vals = [truth(ev(v)) for v in node.values]
            out = vals[0]
            for v in vals[1:]:
                out = join(out, v)
            return num(out)
        if isinstance(node, ast.IfExp):
            return xr.where(truth(ev(node.test)), ev(node.body), ev(node.orelse))
        return fns[node.func.id](*[ev(a) for a in node.args])

    try:
        with np.errstate(all="ignore"):
            out = ev(tree)
    except (ArithmeticError, TypeError) as e:
        raise ValueError(f"cannot evaluate {expr!r}: {e}") from None
    if not hasattr(out, "dims"):
        raise ValueError("formula uses no band")
    valid = None
    for v in env.values():
        if hasattr(v, "dims"):
            valid = v.notnull() if valid is None else valid & v.notnull()
    return out.astype("float32").where(valid)


def composite(ds, method="median", period=None):
    """Reduce the time axis of a Dataset (mask bands are dropped). Returns a Dataset (y, x) or (time, y, x).

    method: median, mean, min, max, count, pNN (p10, p90 ...), first, last, or max:<what> / min:<what>, which take
    the whole pixel (every band) from the date where <what> (a variable, catalog index or formula) is highest /
    lowest, and add `best_doy`. period: None (all), month, quarter, season (DJF ...), year or a pandas frequency.
    """
    data = reflectance(ds)
    if method.startswith(("max:", "min:")) and method[4:] not in data and method[4:] in ds:
        data = data.assign({method[4:]: ds[method[4:]]})
    if not period:
        out = _reduce(data, method)
    else:
        freq = frequency(period)
        out = data.resample(time=freq).map(lambda g: _reduce(g, method))
        n = data[list(data.data_vars)[0]].notnull().any(["x", "y"]).resample(time=freq).sum()
        out = out.sel(time=n["time"][n > 0])
    out.attrs = dict(ds.attrs, composite=method, period=str(period or ""))
    return keep_crs(out, ds)


def clear_count(ds):
    """Clear observations per pixel (from the first reflectance band). Returns a DataArray (y, x)."""
    data = reflectance(ds)
    return keep_crs(data[list(data.data_vars)[0]].notnull().sum("time").astype("int32").rename("count"), ds)


def image(aoi, start, end, bands, *, res=10, crs="utm", shape=None, like=None, method="median", keep="clear",
          max_cloud=MAX_CLOUD, min_clear=0, per_mgrs=None, max_pixels=MAX_PIXELS):
    """One composite of `bands` over `aoi` from `start` to `end`. Returns a Dataset (y, x).

    Pipeline: search -> rank (`per_mgrs` best scenes per MGRS tile) -> clear_scenes (dates at least `min_clear` %
    clear inside the AOI) -> load with the SCL mask `keep` -> composite(method). Raises NoData without scenes.
    """
    scenes = search(aoi, start, end, max_cloud=max_cloud)
    if per_mgrs:
        scenes = rank(scenes, aoi, per_mgrs=per_mgrs)
    if min_clear and len(scenes):
        scenes = clear_scenes(scenes, aoi, min_clear=min_clear)
    if not len(scenes):
        raise NoData(f"no scenes for {aoi} {start}..{end} (max_cloud {max_cloud}, min_clear {min_clear})")
    ds = load(scenes, bands, aoi, res=res, crs=crs, shape=shape, like=like, keep=keep, max_pixels=max_pixels)
    return composite(ds, method)


def cube(aoi, start, end, what, *, period=None, method="median", res=60, crs="utm", keep="clear", months=None,
         max_cloud=MAX_CLOUD, min_clear=0):
    """`what` (catalog index, formula or band) over `aoi` per date, or per `period`. Returns a DataArray (time, y, x).

    Loads one year at a time and keeps only `what`, so long ranges stay inside the load size guard. Periods are
    formed after the years are joined, so a December-February season is one step. `months` keeps only those
    calendar months (e.g. [6] for every June).
    """
    import pandas as pd
    import xarray as xr

    bounds, grid, parts = bbox(aoi), None, []
    t0, t1 = pd.Timestamp(_day(start)), pd.Timestamp(_day(end))
    for year in range(t0.year, t1.year + 1):
        ys = max(t0, pd.Timestamp(f"{year}-01-01")).strftime("%Y-%m-%d")
        ye = min(t1, pd.Timestamp(f"{year}-12-31")).strftime("%Y-%m-%d")
        scenes = search(bounds, ys, ye, max_cloud=max_cloud)
        if months and len(scenes):
            scenes = scenes[scenes["date"].str[5:7].astype(int).isin([int(m) for m in months])]
        if min_clear and len(scenes):
            scenes = clear_scenes(scenes, bounds, min_clear=min_clear)
        if not len(scenes):
            continue
        ds = load(scenes, needs(what), bounds, res=res, crs=crs, like=grid, keep=keep)
        grid = _odc(ds).geobox
        parts.append(index(ds, what, name="value"))
    if not parts:
        raise NoData(f"no scenes for {aoi} {start}..{end}")
    da = keep_crs(xr.concat(parts, dim="time").sortby("time"), parts[0])
    if period:
        da = composite(da.to_dataset(name="value"), method, period)["value"]
    da.attrs = {"what": str(what), "res": res, "period": str(period or "")}
    return da.rename(what if is_index(what) else "value")


def _reduce(ds, method):
    import numpy as np

    if method in ("median", "mean", "min", "max"):
        return getattr(ds, method)("time", skipna=True, keep_attrs=True)
    if method == "count":
        return ds.notnull().sum("time").astype("float32")
    if method.startswith("p") and method[1:].replace(".", "", 1).isdigit():
        q = float(method[1:])
        if not 0 <= q <= 100:
            raise ValueError(f"percentile must be in p0..p100, got {method}")
        return ds.reduce(lambda a, axis: nanquantile(a, q / 100, axis), dim="time", keep_attrs=True)
    if method in ("first", "last"):
        src = ds if method == "first" else ds.isel(time=slice(None, None, -1))
        return ds.isel(time=0, drop=True).assign({v: src[v].isel(time=src[v].notnull().argmax("time"), drop=True) for v in src.data_vars})
    if method.startswith(("max:", "min:")):
        how, what = method.split(":", 1)
        score = ds[what] if what in ds else index(ds, what)
        has = score.notnull().any("time")
        filled = score.fillna(-np.inf if how == "max" else np.inf)
        i = filled.argmax("time") if how == "max" else filled.argmin("time")
        out = ds.isel(time=i, drop=True).where(has)
        out["best_doy"] = ds["time"].isel(time=i, drop=True).dt.dayofyear.astype("float32").where(has)
        return out
    raise ValueError("method: median, mean, min, max, count, pNN, first, last, max:<what>, min:<what>")


CHANGE_PRESETS = {
    "burn": ("NBR", "drop", [-0.25, -0.1, 0.1, 0.27, 0.44, 0.66],
             ["regrowth_high", "regrowth_low", "unburned", "low", "moderate_low", "moderate_high", "high"]),
    "vegetation": ("NDVI", "drop", [-0.1, 0.1, 0.25], ["gain", "no_change", "loss_low", "loss_high"]),
    "water": ("MNDWI", "rise", [-0.3, 0.3], ["water_loss", "no_change", "water_gain"]),
    "moisture": ("NDMI", "drop", [-0.1, 0.1, 0.2], ["wetter", "no_change", "drier", "much_drier"]),
}


def zone_labels(like, zones):
    """Label raster (y, x) on the grid of `like`: 0 outside, i + 1 inside row i of `zones`.

    Zones may be in any CRS. Where zones overlap, the later row wins the shared pixels. A zone that gets no pixel
    that way (smaller than a pixel) takes the pixels it touches that are free or belong to an earlier row.
    """
    import numpy as np
    from rasterio.features import rasterize

    g = _odc(like).geobox
    z = (zones if zones.crs is not None else zones.set_crs(4326)).to_crs(str(g.crs))
    shapes = [(geom, i + 1) for i, geom in enumerate(z.geometry) if geom is not None and not geom.is_empty]
    if not shapes:
        return np.zeros(g.shape, dtype="int32")
    lab = rasterize(shapes, out_shape=g.shape, transform=g.affine, fill=0, dtype="int32")
    missing = sorted({i for _, i in shapes} - set(np.unique(lab)))
    if missing:
        extra = rasterize([(z.geometry.iloc[i - 1], i) for i in missing], out_shape=g.shape, transform=g.affine,
                          fill=0, dtype="int32", all_touched=True)
        lab = np.where((extra > 0) & (lab < extra), extra, lab)
    return lab


def zonal_stats(data, zones, stats="mean,median,std,count", *, id_col=None):
    """Statistics per zone, per date if `data` has time, for each variable of a Dataset (or one DataArray).

    stats: mean, median, std, min, max, count, pNN. Returns a long DataFrame:
    zone, [time], variable, <stats>, pixels (zone size), valid_pct (valid pixels / pixels). A zone with pixels but
    no valid value on a date gets a row with valid_pct 0.
    """
    import numpy as np
    import pandas as pd

    stats = [s.strip() for s in stats.split(",")] if isinstance(stats, str) else list(stats)
    for s in stats:
        if s not in ("mean", "median", "std", "min", "max", "count") and not (s[:1] == "p" and s[1:].isdigit() and int(s[1:]) <= 100):
            raise ValueError(f"unknown statistic {s!r}; use mean, median, std, min, max, count or pNN")
    ds = reflectance(_dataset(data))
    lab = zone_labels(ds, zones).ravel()
    inside = lab > 0
    size = np.bincount(lab[inside], minlength=len(zones) + 1)
    present = np.flatnonzero(size)
    ids = zones[id_col].values if id_col else np.arange(len(zones))
    has_time = "time" in ds.dims
    lead = ["zone"] + (["time"] if has_time else []) + ["variable"]
    rows = []
    for i in range(ds.sizes["time"] if has_time else 1):
        sub = ds.isel(time=i) if has_time else ds
        for v in sub.data_vars:
            frame = pd.DataFrame({"zone": lab[inside], "v": sub[v].values.ravel()[inside]})
            g = frame.dropna().groupby("zone")["v"]
            agg = pd.DataFrame({s: g.count() if s == "count" else g.quantile(int(s[1:]) / 100) if s[0] == "p" else getattr(g, s)()
                                for s in stats}).reindex(present)
            agg["_valid"] = g.count().reindex(present).fillna(0)
            if "count" in agg:
                agg["count"] = agg["count"].fillna(0).astype(int)
            rows.append(agg.assign(variable=v, **({"time": pd.Timestamp(sub["time"].values)} if has_time else {})))
    if not rows or not len(present):
        return pd.DataFrame(columns=lead + stats + ["pixels", "valid_pct"])
    out = pd.concat(rows).rename_axis("zone").reset_index()
    out["pixels"] = size[out["zone"].values]
    out["valid_pct"] = (out.pop("_valid") / out["pixels"] * 100).round(1)
    out["zone"] = ids[out["zone"].values - 1]
    return out[lead + [c for c in out.columns if c not in lead]].sort_values(lead).reset_index(drop=True)


def sample(data, points, *, id_col=None, lat_col="lat", lng_col="lng"):
    """Nearest-pixel values at points (GeoDataFrame, or DataFrame with lat / lng). One row per point and date.

    `inside` is False for points outside the grid; their values are NaN.
    """
    import geopandas as gpd
    import xarray as xr

    ds = _dataset(data)
    if not isinstance(points, gpd.GeoDataFrame):
        points = gpd.GeoDataFrame(points, geometry=gpd.points_from_xy(points[lng_col], points[lat_col]), crs=4326)
    p = points.to_crs(str(_odc(ds).crs))
    vals = ds.sel(x=xr.DataArray(p.geometry.x.values, dims="point"), y=xr.DataArray(p.geometry.y.values, dims="point"), method="nearest")
    df = vals.drop_vars([c for c in vals.coords if c not in ("point", "time")]).to_dataframe().reset_index()
    bb = _odc(ds).geobox.extent.boundingbox
    inside = (p.geometry.x.between(bb.left, bb.right) & p.geometry.y.between(bb.bottom, bb.top)).values
    df["inside"] = inside[df["point"].values]
    df.loc[~df["inside"], list(ds.data_vars)] = float("nan")
    if id_col:
        df["point"] = points[id_col].values[df["point"].values]
    return df


def change(before, after, preset="burn", *, what=None, direction=None, breaks=None, labels=None):
    """Change of an index between two images. Returns a Dataset: before, after, delta, class (attrs labels, breaks).

    `before` and `after` are 2-D Datasets on the same grid (a time axis of length 1 is dropped).
    preset: burn (dNBR, USGS severity), vegetation (dNDVI), water (dMNDWI), moisture (dNDMI), or None with your own
    `what` (index or formula), `direction` (drop = before - after, rise = after - before), `breaks` and `labels`.
    """
    import numpy as np
    import xarray as xr

    if preset and preset not in CHANGE_PRESETS:
        raise ValueError(f"preset must be one of {list(CHANGE_PRESETS)} or None")
    p_what, p_dir, p_breaks, p_labels = CHANGE_PRESETS[preset] if preset else (None, "drop", [], None)
    what, direction = what or p_what, direction or p_dir
    if direction not in ("drop", "rise"):
        raise ValueError("direction must be drop or rise")
    breaks = list(breaks if breaks is not None else p_breaks)
    labels = list(labels or p_labels or [f"class_{i}" for i in range(len(breaks) + 1)])
    if not what or len(labels) != len(breaks) + 1:
        raise ValueError("give a preset, or `what` with len(labels) == len(breaks) + 1")
    before, after = _single(before, "before"), _single(after, "after")
    pre, post = index(before, what), index(after, what)
    delta = (pre - post) if direction == "drop" else (post - pre)
    cls = delta.copy(data=np.digitize(delta.values, breaks).astype("float32")).where(delta.notnull())
    cls.attrs = {"labels": labels, "breaks": breaks}
    out = xr.Dataset({"before": pre, "after": post, "delta": delta.astype("float32"), "class": cls})
    out.attrs = {"what": what, "direction": direction, "labels": labels, "breaks": breaks}
    return keep_crs(out, before)


def class_area(da, labels=None):
    """km2 and share per class of a class raster. Pixel areas are measured on the WGS84 ellipsoid (per row, at the
    centre column), so any grid works (UTM, Web Mercator, degrees, feet). Labels come from `labels` or
    da.attrs["labels"]."""
    import numpy as np
    import pandas as pd

    labels = labels or da.attrs.get("labels")
    v = np.asarray(da.values, dtype="float32")
    ok = ~np.isnan(v)
    area = np.broadcast_to(_pixel_area(da), v.shape)
    codes = v[ok].astype(int)
    out = pd.DataFrame({"class": codes, "m2": area[ok]}).groupby("class").agg(pixels=("m2", "size"), m2=("m2", "sum")).reset_index()
    out["label"] = [labels[c] if labels and 0 <= c < len(labels) else str(c) for c in out["class"]]
    out["km2"] = (out["m2"] / 1e6).round(3)
    out["pct"] = (out["m2"] / out["m2"].sum() * 100).round(2) if len(out) else []
    return out[["class", "label", "pixels", "km2", "pct"]]


def _single(ds, name):
    if "time" in ds.dims:
        if ds.sizes["time"] != 1:
            raise ValueError(f"{name} has {ds.sizes['time']} time steps; composite it to one image first")
        ds = ds.isel(time=0, drop=True)
    return ds


def _pixel_area(da):
    import numpy as np
    from pyproj import Geod, Transformer

    g = _odc(da).geobox
    a = g.affine
    if g.crs.epsg is not None and (32601 <= g.crs.epsg <= 32660 or 32701 <= g.crs.epsg <= 32760):
        return np.full((g.shape[0], 1), abs(a.a * a.e))
    to_ll = Transformer.from_crs(g.crs.to_wkt(), "EPSG:4326", always_xy=True)
    geod = Geod(ellps="WGS84")
    tops = a.f + a.e * np.arange(g.shape[0])
    x0 = a.c + a.a * (g.shape[1] // 2)
    out = np.empty((g.shape[0], 1))
    for i, top in enumerate(tops):
        lon, lat = to_ll.transform([x0, x0 + a.a, x0 + a.a, x0], [top, top, top + a.e, top + a.e])
        out[i, 0] = abs(geod.polygon_area_perimeter(lon, lat)[0])
    return out


def _dataset(data):
    import xarray as xr

    return data.to_dataset(name=data.name or "value") if isinstance(data, xr.DataArray) else data


WORLDCOVER = "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/ESA_WorldCover_10m_2021_v200_{tile}_Map.tif"
WORLDCOVER_CLASSES = {10: "tree", 20: "shrub", 30: "grass", 40: "crop", 50: "built", 60: "bare", 70: "snow_ice",
                      80: "water", 90: "wetland", 95: "mangrove", 100: "moss_lichen"}


def features(ds, what=None):
    """Bands (+ catalog indices or formulas in `what`) -> DataArray (feature, y, x).

    With a time axis, one feature per band and date, named <band>_<YYYY-MM-DD>. Class rasters made from features
    carry their class names in attrs["labels"]; pixels with any NaN feature are skipped and come back NaN.
    """
    data = ds[[v for v in reflectance(ds).data_vars if v != "best_doy"]]
    if what:
        data = data.merge(indices(data, what))
    da = data.to_array("feature")
    if "time" in da.dims:
        names = [f"{v}_{str(t)[:10]}" for t in da["time"].values for v in da["feature"].values]
        da = da.transpose("time", "feature", "y", "x").stack(f=("time", "feature")).transpose("f", "y", "x")
        da = da.drop_vars(["f", "time", "feature"]).rename(f="feature").assign_coords(feature=names)
    da = da.assign_coords(feature=[str(f) for f in da["feature"].values]).astype("float32")
    da.attrs = dict(ds.attrs)
    return keep_crs(da, ds)


def cluster(feat, k=6, *, sample=50_000, seed=0):
    """MiniBatchKMeans on standardised features. Returns (DataArray of cluster ids, DataFrame of centres)."""
    import numpy as np
    import pandas as pd
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler

    X, ok = _matrix(feat)
    if ok.sum() < k:
        raise NoData("not enough valid pixels to cluster")
    rng = np.random.default_rng(seed)
    fit = X[ok][rng.choice(ok.sum(), min(sample, int(ok.sum())), replace=False)]
    scaler = StandardScaler().fit(fit)
    km = MiniBatchKMeans(n_clusters=int(k), random_state=seed, n_init=3, batch_size=4096).fit(scaler.transform(fit))
    lab = km.predict(scaler.transform(X[ok]))
    centres = pd.DataFrame(scaler.inverse_transform(km.cluster_centers_), columns=list(feat["feature"].values))
    centres.insert(0, "cluster", range(int(k)))
    centres.insert(1, "share_pct", (np.bincount(lab, minlength=int(k)) / len(lab) * 100).round(1))
    return _raster(lab, ok, feat, "cluster", [f"cluster_{i}" for i in range(int(k))]), centres


def labels_worldcover(like):
    """ESA WorldCover 2021 (10 m) on the grid of `like` (nearest). Returns codes 0..n-1 with attrs["labels"]."""
    import math
    import numpy as np
    import rasterio
    from rasterio.warp import Resampling, reproject, transform_bounds

    g = _odc(like).geobox
    w, s, e, n = transform_bounds(str(g.crs), "EPSG:4326", *g.extent.boundingbox)
    dst = np.zeros(g.shape, dtype="uint8")
    with rasterio.Env(**GDAL_ENV):
        for lat in range(math.floor(s / 3) * 3, math.floor(n / 3) * 3 + 1, 3):
            for lon in range(math.floor(w / 3) * 3, math.floor(e / 3) * 3 + 1, 3):
                cell = f"{'N' if lat >= 0 else 'S'}{abs(lat):02d}{'E' if lon >= 0 else 'W'}{abs(lon):03d}"
                part = np.zeros(g.shape, dtype="uint8")
                try:
                    with rasterio.open(WORLDCOVER.format(tile=cell)) as src:
                        reproject(rasterio.band(src, 1), part, dst_transform=g.affine, dst_crs=str(g.crs),
                                  resampling=Resampling.nearest, src_nodata=0, dst_nodata=0)
                except rasterio.errors.RasterioIOError:
                    continue
                dst = np.where(dst == 0, part, dst)
    present = [c for c in WORLDCOVER_CLASSES if (dst == c).any()]
    codes = np.full(dst.shape, np.nan, dtype="float32")
    for i, c in enumerate(present):
        codes[dst == c] = i
    return on_grid(codes, like, "worldcover", {"labels": [WORLDCOVER_CLASSES[c] for c in present]})


def labels_from_zones(like, zones, label_col):
    """Labelled polygons on the grid of `like`. Returns a Dataset: label (codes, attrs labels) and zone (row index)."""
    import numpy as np
    import xarray as xr

    labels = sorted(zones[label_col].astype(str).unique())
    lab = zone_labels(like, zones)
    codes = np.array([np.nan] + [labels.index(str(v)) for v in zones[label_col]], dtype="float32")[lab]
    return xr.Dataset({"label": on_grid(codes, like, "label", {"labels": labels}),
                       "zone": on_grid(np.where(lab > 0, lab - 1, np.nan).astype("float32"), like, "zone")})


def train(feat, labels, *, groups=None, model="rf", per_class=500, test_frac=0.3, seed=0, n_estimators=200):
    """Fit a classifier on a stratified pixel sample. Returns (model, report).

    labels: class codes with attrs["labels"]. groups: optional raster of group ids (labels_from_zones(...)["zone"]);
    then whole groups are held out, so pixels of one field cannot leak between train and test. Pixels without a
    group are used for training only.
    report: accuracy, kappa, per_class, confusion, importance (rf only, else None), train_px, test_px, test_groups.
    """
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
    from sklearn.metrics import accuracy_score, classification_report, cohen_kappa_score, confusion_matrix

    models = {"rf": lambda: RandomForestClassifier(n_estimators=n_estimators, n_jobs=-1, random_state=seed, class_weight="balanced"),
              "gbt": lambda: HistGradientBoostingClassifier(random_state=seed)}
    if model not in models:
        raise ValueError(f"model must be one of {list(models)}")
    names = labels.attrs.get("labels")
    X, ok = _matrix(feat)
    y = labels.values.ravel()
    ok = ok & ~np.isnan(y)
    if len(np.unique(y[ok])) < 2:
        raise NoData("train needs at least 2 classes with valid pixels")
    rng = np.random.default_rng(seed)
    idx = np.concatenate([rng.choice(m, min(per_class, len(m)), replace=False)
                          for m in (np.flatnonzero(ok & (y == c)) for c in np.unique(y[ok]))])
    held = []
    if groups is not None:
        g = groups.values.ravel()[idx]
        found = np.unique(g[~np.isnan(g)])
        if len(found) < 2:
            raise ValueError("groups needs at least 2 groups with labelled pixels")
        held = rng.choice(found, max(1, int(len(found) * test_frac)), replace=False)
        is_test = np.isin(g, held)
    else:
        is_test = rng.random(len(idx)) < test_frac
    tr, te = idx[~is_test], idx[is_test]
    if not len(tr) or not len(te):
        raise ValueError("the split left no training or no test pixels; change test_frac or per_class")
    clf = models[model]().fit(X[tr], y[tr].astype(int))
    pred, truth = clf.predict(X[te]), y[te].astype(int)
    codes = sorted(np.unique(np.concatenate([truth, pred])))
    nm = [names[i] if names else str(i) for i in codes]
    report = {
        "train_px": int(len(tr)), "test_px": int(len(te)),
        "accuracy": round(float(accuracy_score(truth, pred)), 3), "kappa": round(float(cohen_kappa_score(truth, pred)), 3),
        "per_class": pd.DataFrame(classification_report(truth, pred, labels=codes, target_names=nm, output_dict=True, zero_division=0)).T.round(3),
        "confusion": pd.DataFrame(confusion_matrix(truth, pred, labels=codes), index=nm, columns=nm),
        "importance": (pd.Series(clf.feature_importances_, index=list(feat["feature"].values)).sort_values(ascending=False).round(3)
                       if hasattr(clf, "feature_importances_") else None),
        "test_groups": sorted(int(v) for v in held),
    }
    clf.feature_names_, clf.labels_ = list(feat["feature"].values), names
    return clf, report


def predict(model, feat, chunk=500_000):
    """Class codes (y, x) for every pixel with complete features, with attrs["labels"] from the model."""
    import numpy as np

    if list(feat["feature"].values) != getattr(model, "feature_names_", list(feat["feature"].values)):
        raise ValueError("features differ from the ones the model was trained on")
    X, ok = _matrix(feat)
    rows = np.flatnonzero(ok)
    out = np.concatenate([model.predict(X[rows[i:i + chunk]]) for i in range(0, len(rows), chunk)]) if len(rows) else np.array([])
    return _raster(out, ok, feat, "class", getattr(model, "labels_", None))


def vote(pred, zones):
    """Majority class per polygon: zones + class, label, agreement (share of the polygon's pixels that agree)."""
    import numpy as np
    import pandas as pd

    lab = zone_labels(pred, zones).ravel()
    p = pred.values.ravel()
    ok = (lab > 0) & ~np.isnan(p)
    cnt = pd.DataFrame({"z": lab[ok] - 1, "c": p[ok].astype(int)}).groupby(["z", "c"]).size().rename("n").reset_index()
    cnt["share"] = cnt["n"] / cnt.groupby("z")["n"].transform("sum")
    top = cnt.sort_values(["z", "n"], ascending=[True, False]).drop_duplicates("z").set_index("z")
    out = zones.reset_index(drop=True).copy()
    out["class"] = top["c"].reindex(range(len(out))).values
    out["agreement"] = top["share"].reindex(range(len(out))).round(3).values
    names = pred.attrs.get("labels")
    out["label"] = [names[int(c)] if names and c == c else None for c in out["class"]]
    return out


def vectorize(da, *, min_px=8, simplify=None, keep=None):
    """Class raster -> polygons (EPSG:4326) with class, label, area_m2 (on the WGS84 ellipsoid). Patches under
    `min_px` pixels are sieved first. `simplify` is a tolerance in the units of the raster's CRS.

    keep: class codes to return (default all).
    """
    import geopandas as gpd
    import numpy as np
    import shapely
    from pyproj import Geod
    from rasterio.features import shapes, sieve

    g = _odc(da).geobox
    valid = ~np.isnan(da.values)
    codes = np.where(valid, da.values, -1).astype("int32") + 1
    if min_px and min_px > 1:
        codes = sieve(codes, size=int(min_px), mask=valid.astype("uint8"))
    parts = [(shapely.geometry.shape(geom), int(v) - 1) for geom, v in shapes(codes, mask=codes > 0, transform=g.affine)]
    parts = [(geom, c) for geom, c in parts if keep is None or c in keep]
    out = gpd.GeoDataFrame({"class": [c for _, c in parts]}, geometry=[geom for geom, _ in parts], crs=str(g.crs))
    names = da.attrs.get("labels")
    out["label"] = [names[c] if names and c < len(names) else str(c) for c in out["class"]]
    if simplify:
        out["geometry"] = out.geometry.simplify(float(simplify))
    out = out.to_crs(4326)
    geod = Geod(ellps="WGS84")
    out["area_m2"] = [round(abs(geod.geometry_area_perimeter(geom)[0]), 1) for geom in out.geometry]
    return out


def _matrix(feat):
    import numpy as np

    X = feat.values.reshape(feat.sizes["feature"], -1).T
    return X, ~np.isnan(X).any(axis=1)


def _raster(values, ok, like, name, labels=None):
    import numpy as np

    out = np.full(ok.shape[0], np.nan, dtype="float32")
    out[ok] = values
    return on_grid(out.reshape(like.sizes["y"], like.sizes["x"]), like, name, {"labels": labels} if labels else None)


def regularize(x, period="10D", method="median"):
    """Resample to regular steps (10D, 5D, month, ...) with median / mean / max / min. Empty steps stay NaN.

    The time functions take an xarray DataArray with a `time` axis or a pandas Series indexed by date, and return
    the same kind. Build a cube with `cube`; smooth only regular, gap-filled series.
    """
    if method not in ("median", "mean", "max", "min"):
        raise ValueError("method must be median, mean, max or min")
    freq = frequency(period)
    return getattr(x.sort_index().resample(freq) if _is_series(x) else x.resample(time=freq), method)()


def fill_gaps(x, max_gap=None):
    """Linear interpolation in time; leading / trailing gaps take the nearest value. Gaps longer than `max_gap`
    (like '60D') stay NaN."""
    import numpy as np
    import pandas as pd

    if _is_series(x):
        y = x.interpolate(method="time", limit_area="inside")
        if max_gap:
            y = y.where(_short_gaps(x, pd.Timedelta(max_gap)))
        return y.ffill(limit_area="outside").bfill(limit_area="outside")
    y = x.interpolate_na("time", method="linear", use_coordinate=True)
    data = _edge_fill(y.values, y.get_axis_num("time"))
    if max_gap:
        days = np.asarray((pd.to_datetime(x["time"].values) - pd.Timestamp(x["time"].values[0])) / pd.Timedelta(days=1))
        long = _long_gaps(np.moveaxis(np.isnan(np.asarray(x.values, dtype="float32")), y.get_axis_num("time"), 0), days,
                          pd.Timedelta(max_gap) / pd.Timedelta(days=1))
        data = np.where(np.moveaxis(long, 0, y.get_axis_num("time")), np.nan, data)
    return y.copy(data=data)


def smooth(x, window=5, poly=2):
    """Savitzky-Golay filter in time (window in steps, odd). Gaps are interpolated for the filter only, so they do
    not pull neighbours toward 0; NaN input stays NaN."""
    import numpy as np
    from scipy.signal import savgol_filter

    w = int(window) | 1
    if (len(x) if _is_series(x) else x.sizes["time"]) < w:
        return x
    axis = 0 if _is_series(x) else x.get_axis_num("time")
    filled = np.nan_to_num(np.asarray(fill_gaps(x).values, dtype="float64"))
    out = np.where(np.isnan(np.asarray(x.values, dtype="float64")), np.nan, savgol_filter(filled, w, int(poly), axis=axis))
    return x.__class__(out, index=x.index, name=x.name) if _is_series(x) else x.copy(data=out.astype("float32"))


def harmonic(x, n=2, period_days=365.25, trend=True):
    """Least-squares fit of mean (+ trend) + n annual harmonics, per pixel, skipping NaN. Returns (fitted, coefficients).

    coefficients: mean (level at mid-period), trend (per year), amplitude_k, phase_k (day of year of the maximum).
    A cube is returned with time as its first axis.
    """
    import numpy as np
    import pandas as pd
    import xarray as xr

    if not _is_series(x):
        x = x.transpose("time", ...)
    t = pd.to_datetime(x.index if _is_series(x) else x["time"].values)
    days = ((t - t[0]) / pd.Timedelta(days=1)).values.astype("float64")
    w = 2 * np.pi * days / period_days
    A = np.stack([np.ones_like(days)] + ([(days - days.mean()) / 365.25] if trend else [])
                 + [f(k * w) for k in range(1, n + 1) for f in (np.cos, np.sin)], axis=1)
    Y = np.asarray(x.values, dtype="float64").reshape(len(days), -1)
    ok = ~np.isnan(Y)
    P = A.shape[1]
    M = np.einsum("tp,tq,tn->npq", A, A, ok.astype("float64")) + 1e-9 * np.eye(P)
    coef = np.linalg.solve(M, np.einsum("tp,tn->np", A, np.where(ok, Y, 0.0))[..., None])[..., 0].T
    coef[:, ok.sum(0) < P + 1] = np.nan
    o = 1 + int(trend)
    res = {"mean": coef[0], **({"trend": coef[1]} if trend else {})}
    for k in range(1, n + 1):
        a, b = coef[o + 2 * (k - 1)], coef[o + 2 * (k - 1) + 1]
        res[f"amplitude_{k}"] = np.hypot(a, b)
        res[f"phase_{k}"] = (np.arctan2(b, a) / (2 * np.pi * k) * period_days + t[0].dayofyear) % (period_days / k)
    fitted = (A @ coef).reshape(np.asarray(x.values).shape)
    if _is_series(x):
        return pd.Series(fitted, index=x.index, name="fit"), pd.Series({k: float(v[0]) for k, v in res.items()})
    space = x.isel(time=0)
    coefs = xr.Dataset({k: (space.dims, v.reshape(space.shape)) for k, v in res.items()}, coords={d: x[d] for d in space.dims})
    return x.copy(data=fitted), keep_crs(coefs, x)


def phenology(series, *, threshold=0.5, min_amplitude=0.15, prominence=0.15, min_length=20, period="5D", window=7):
    """Seasons of one series (pandas, by date). One row per season: sos, peak_date, eos, length_days, base, peak,
    amplitude, integral and the days of year.

    The series is regularised to `period`, gap-filled and smoothed. A season is a peak with at least `prominence`,
    `min_amplitude` and `min_length` days. SOS / EOS: where the curve crosses base + threshold * (peak - base).
    """
    import numpy as np
    import pandas as pd
    from scipy.signal import find_peaks

    cols = ["season", "sos", "peak_date", "eos", "length_days", "base", "peak", "amplitude", "integral", "sos_doy",
            "peak_doy", "eos_doy"]
    if series.dropna().empty:
        return pd.DataFrame(columns=cols)
    x = smooth(fill_gaps(regularize(series.dropna(), period)), window=window)
    v, d = x.values, x.index
    step = (d[1] - d[0]).days if len(d) > 1 else 1
    peaks, props = find_peaks(v, prominence=prominence)
    rows = []
    for i, p in enumerate(peaks):
        lo, hi = int(props["left_bases"][i]), int(props["right_bases"][i])
        lb, rb = v[lo:p + 1].min(), v[p:hi + 1].min()
        if v[p] - max(lb, rb) < min_amplitude:
            continue
        lt, rt = lb + threshold * (v[p] - lb), rb + threshold * (v[p] - rb)
        sos = min(next((j + 1 for j in range(p, lo - 1, -1) if v[j] < lt), lo), p)
        eos = max(next((j - 1 for j in range(p, hi + 1) if v[j] < rt), hi), p)
        if (d[eos] - d[sos]).days < min_length:
            continue
        base = (lb + rb) / 2
        rows.append({"season": len(rows) + 1, "sos": d[sos].date(), "peak_date": d[p].date(), "eos": d[eos].date(),
                     "length_days": (d[eos] - d[sos]).days, "base": round(float(base), 3), "peak": round(float(v[p]), 3),
                     "amplitude": round(float(v[p] - base), 3),
                     "integral": round(float(np.clip(v[sos:eos + 1] - base, 0, None).sum() * step), 1),
                     "sos_doy": d[sos].dayofyear, "peak_doy": d[p].dayofyear, "eos_doy": d[eos].dayofyear})
    return pd.DataFrame(rows, columns=cols)


def phenology_map(ts, *, threshold=0.5, min_amplitude=0.15):
    """Main season per pixel of a regular, gap-filled, smoothed cube (time, y, x).

    Returns a Dataset: sos_doy, peak_doy, eos_doy, length_days, base, peak, amplitude (NaN where amplitude is small).
    """
    import numpy as np
    import pandas as pd
    import xarray as xr

    ts = ts.transpose("time", ...)
    v = ts.values.astype("float32")
    nt = v.shape[0]
    has = ~np.isnan(v)
    p = np.where(has, v, -np.inf).argmax(0)
    peak = np.take_along_axis(v, p[None], 0)[0]
    idx = np.arange(nt).reshape((nt,) + (1,) * (v.ndim - 1))
    before, after = idx <= p[None], idx >= p[None]
    lb, rb = np.where(before & has, v, np.inf).min(0), np.where(after & has, v, np.inf).min(0)
    below_l = before & (v < (lb + threshold * (peak - lb))[None])
    below_r = after & (v < (rb + threshold * (peak - rb))[None])
    sos = np.minimum(np.where(below_l.any(0), nt - np.argmax(below_l[::-1], 0), 0), p)
    eos = np.maximum(np.where(below_r.any(0), np.argmax(below_r, 0) - 1, nt - 1), p)
    t = pd.to_datetime(ts["time"].values)
    doy = np.asarray(t.dayofyear, dtype="float32")
    days = np.asarray((t - t[0]).days, dtype="float32")
    base = (lb + rb) / 2
    with np.errstate(invalid="ignore"):
        good = has.any(0) & np.isfinite(base) & (peak - base >= min_amplitude)
    space = ts.isel(time=0)
    mk = lambda a: (space.dims, np.where(good, a, np.nan).astype("float32"))
    out = xr.Dataset({"sos_doy": mk(doy[sos]), "peak_doy": mk(doy[p]), "eos_doy": mk(doy[eos]), "length_days": mk(days[eos] - days[sos]),
                      "base": mk(base), "peak": mk(peak), "amplitude": mk(peak - base)}, coords={d: ts[d] for d in space.dims})
    return keep_crs(out, ts)


def climatology(x, baseline, by="month"):
    """mean, std (sample, ddof=1), min, max and count per month (or ISO week, by='week') over the `baseline` years."""
    import warnings
    import xarray as xr

    if _is_series(x):
        base = x[x.index.year.isin(list(baseline))]
        return base.groupby(_key(base.index, by)).agg(["mean", "std", "min", "max", "count"])
    base = x.sel(time=x["time"].dt.year.isin(list(baseline)))
    key = xr.DataArray(_key(base["time"].to_index(), by), dims="time", name=by)
    g = base.groupby(key)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "Degrees of freedom", RuntimeWarning)
        warnings.filterwarnings("ignore", "Mean of empty slice", RuntimeWarning)
        std = g.std("time", ddof=1)
    return xr.Dataset({"mean": g.mean("time"), "std": std, "min": g.min("time"), "max": g.max("time"),
                       "count": base.notnull().groupby(key).sum("time")})


def anomaly(x, baseline, *, target_years=None, by="month", method="zscore", min_count=2, min_std=None):
    """Anomaly of each step against the same month (or week) of the `baseline` years.

    method: zscore (x - mean) / std | diff (x - mean) | pct (100 * diff / |mean|) | vci (100 * (x - min) / (max - min)).
    Steps whose month has fewer than `min_count` baseline values are NaN. `min_std` floors the std for zscore.
    target_years: years to return (default: every year not in `baseline`). Returns the same kind as `x`
    (a cube DataArray, or a DataFrame with value, baseline_mean and anomaly for a Series).
    """
    import numpy as np
    import pandas as pd

    if method not in ("zscore", "diff", "pct", "vci"):
        raise ValueError("method must be zscore, diff, pct or vci")
    clim = climatology(x, baseline, by)
    if _is_series(x):
        tgt = x[x.index.year.isin(target_years or sorted(set(x.index.year) - set(baseline)))]
        c = clim.reindex(_key(tgt.index, by))
        c.index = tgt.index
    else:
        tgt = x.sel(time=x["time"].dt.year.isin(target_years or sorted(set(x["time"].dt.year.values.tolist()) - set(baseline))))
        c = clim.reindex({by: _key(tgt["time"].to_index(), by)}).rename({by: "time"}).assign_coords(time=tgt["time"].values)
    c = c.where(c["count"] >= min_count)
    std = c["std"].clip(min_std) if min_std else c["std"]
    with np.errstate(divide="ignore", invalid="ignore"):
        out = {"zscore": lambda: (tgt - c["mean"]) / std, "diff": lambda: tgt - c["mean"],
               "pct": lambda: 100 * (tgt - c["mean"]) / abs(c["mean"]),
               "vci": lambda: 100 * (tgt - c["min"]) / (c["max"] - c["min"])}[method]()
    if _is_series(x):
        return pd.DataFrame({"value": tgt, "baseline_mean": c["mean"], "anomaly": out.replace([np.inf, -np.inf], np.nan)})
    return keep_crs(out.where(np.isfinite(out)).rename(f"{method}_anomaly"), x)


def _is_series(x):
    import pandas as pd

    return isinstance(x, pd.Series)


def _key(times, by):
    import numpy as np

    if by == "month":
        return np.asarray(times.month)
    if by == "week":
        return np.asarray(times.isocalendar().week, dtype="int64")
    raise ValueError("by must be month or week")


def _short_gaps(x, max_gap):
    import pandas as pd

    valid = x.dropna().index
    ok = pd.Series(True, index=x.index)
    for a, b in zip(valid[:-1], valid[1:]):
        if b - a > max_gap:
            ok[(x.index > a) & (x.index < b)] = False
    return ok


def _long_gaps(missing, days, max_days):
    import numpy as np

    n = missing.shape[0]
    idx = np.arange(n).reshape((n,) + (1,) * (missing.ndim - 1))
    prev = np.maximum.accumulate(np.where(~missing, idx, -1), axis=0)
    nxt = np.minimum.accumulate(np.where(~missing, idx, n)[::-1], axis=0)[::-1]
    inner = missing & (prev >= 0) & (nxt < n)
    span = days[np.clip(nxt, 0, n - 1)] - days[np.clip(prev, 0, n - 1)]
    return inner & (span > max_days)


def _edge_fill(a, axis=0):
    import numpy as np

    a = np.moveaxis(np.asarray(a, dtype="float32").copy(), axis, 0)
    ok = ~np.isnan(a)
    n = a.shape[0]
    first, last = ok.argmax(0), n - 1 - ok[::-1].argmax(0)
    idx = np.arange(n).reshape((n,) + (1,) * (a.ndim - 1))
    a = np.where(idx < first[None], np.take_along_axis(a, first[None], 0), a)
    a = np.where(idx > last[None], np.take_along_axis(a, last[None], 0), a)
    return np.moveaxis(a, 0, axis)


AEF_INDEX = "https://data.source.coop/tge-labs/aef/v1/annual/aef_index.parquet"
AEF_YEARS = range(2017, 2026)
AEF_FEATURES = [f"A{i:02d}" for i in range(64)]
AEF_MAX_VALUES = 200_000_000
AEF_ENV = {**{k: v for k, v in GDAL_ENV.items() if k != "CPL_VSIL_CURL_ALLOWED_EXTENSIONS"}, "AWS_REGION": "us-west-2"}


def load_embeddings(aoi=None, year=2024, *, res=10, crs="utm", shape=None, like=None, threads=16):
    """AlphaEarth Foundations (AEF) annual embeddings on the grid of `like`, or of `aoi` at `res` / `shape` in `crs`.

    Returns a DataArray (feature=A00..A63, y, x) of unit-length vectors, in the layout of `features`, so cluster,
    train, predict and vote take it directly. Source: COGs on source.coop (tge-labs/aef/v1/annual), int8, nodata
    -128, de-quantised as sign(q) * (q / 127.5)^2. Years 2017-2025.
    """
    import math
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np
    import rasterio
    import xarray as xr
    from rasterio.warp import transform_bounds

    if int(year) not in AEF_YEARS:
        raise ValueError(f"AEF years are {AEF_YEARS.start}..{AEF_YEARS.stop - 1}")
    gb = geobox(aoi, res, crs, shape, like)
    H, W = gb.shape
    if 64 * H * W > AEF_MAX_VALUES:
        raise ValueError(f"64 x {H} x {W} values > {AEF_MAX_VALUES:,}; use a coarser res")
    w, s, e, n = transform_bounds(str(gb.crs), "EPSG:4326", *gb.extent.boundingbox)
    if w > e:
        raise ValueError("the grid crosses 180°; split it into two")
    idx = _aef_files(int(year))
    paths = idx.loc[(idx.wgs84_west < e) & (idx.wgs84_east > w) & (idx.wgs84_south < n) & (idx.wgs84_north > s), "path"]
    out = np.full((64, H, W), -128, dtype="int8")
    ground = abs(gb.resolution.x) * (math.cos(math.radians((s + n) / 2)) if gb.crs.epsg == 3857 else 1.0)
    level = _aef_overview_level(ground)
    jobs = [(p, list(range(g * 16 + 1, g * 16 + 17))) for p in paths for g in range(4)]
    with rasterio.Env(**AEF_ENV), ThreadPoolExecutor(int(threads)) as ex:
        for got in ex.map(lambda job: _read_aef(job[0], job[1], gb, level), jobs):
            if got is not None:
                sl = slice(got[0][0] - 1, got[0][-1])
                out[sl] = np.where(out[sl] == -128, got[1], out[sl])
    q = out.astype("float32")
    v = np.where(out == -128, np.nan, np.sign(q) * (q / 127.5) ** 2).astype("float32")
    a = gb.affine
    da = xr.DataArray(v, dims=("feature", "y", "x"), name="aef", attrs={"year": int(year), "files": int(len(paths))},
                      coords={"feature": AEF_FEATURES, "y": a.f + a.e * (np.arange(H) + 0.5), "x": a.c + a.a * (np.arange(W) + 0.5)})
    return _odc(da).assign_crs(gb.crs)


def embedding_at(emb, lat, lng):
    """The 64-d vector of the pixel nearest to lat / lng."""
    from rasterio.warp import transform

    xs, ys = transform("EPSG:4326", str(_odc(emb).crs), [lng], [lat])
    return emb.sel(x=xs[0], y=ys[0], method="nearest").values


def embedding_of(emb, zones):
    """Mean unit vector over the pixels inside the polygons of a GeoDataFrame."""
    import numpy as np

    inside = zone_labels(emb.isel(feature=0), zones) > 0
    if not inside.any():
        raise NoData("the zones cover no pixel of the embeddings")
    return _unit(np.nanmean(emb.values[:, inside], axis=1))


def embedding_similarity(emb, ref=None, *, lat=None, lng=None, zones=None):
    """Cosine similarity of every pixel to a 64-vector `ref`, the pixel at lat / lng, or the mean of `zones`."""
    import numpy as np

    if zones is not None:
        r = embedding_of(emb, zones)
    elif lat is not None and lng is not None:
        r = _unit(embedding_at(emb, lat, lng))
    elif ref is not None:
        r = _unit(ref)
    else:
        raise ValueError("give ref, lat and lng, or zones")
    return on_grid(np.tensordot(r, emb.values, axes=1).astype("float32"), emb, "similarity")


def embedding_difference(a, b):
    """1 - cosine similarity between two embedding years on the same grid (0 = unchanged, up to 2 = opposite)."""
    import numpy as np

    den = np.linalg.norm(a.values, axis=0) * np.linalg.norm(b.values, axis=0)
    with np.errstate(invalid="ignore"):
        d = 1 - (a.values * b.values).sum(0) / np.where(den > 0, den, np.nan)
    return on_grid(d.astype("float32"), a, "difference", {"years": f"{a.attrs.get('year')}-{b.attrs.get('year')}"})


def embedding_preview(emb, sample=20_000, seed=0):
    """uint8 RGBA (4, H, W) of the first three principal components, stretched 2-98 %."""
    import numpy as np

    X = emb.values.reshape(64, -1).T
    ok = ~np.isnan(X).any(1)
    rgba = np.zeros((4,) + emb.shape[1:], dtype="uint8")
    if ok.sum() < 10:
        return rgba
    S = X[ok][np.random.default_rng(seed).choice(ok.sum(), min(sample, int(ok.sum())), replace=False)]
    mu = S.mean(0)
    P = (X[ok] - mu) @ np.linalg.svd(S - mu, full_matrices=False)[2][:3].T
    lo, hi = np.percentile(P, 2, axis=0), np.percentile(P, 98, axis=0)
    flat = np.zeros((X.shape[0], 3), dtype="uint8")
    flat[ok] = (np.clip((P - lo) / np.where(hi > lo, hi - lo, 1), 0, 1) * 255).astype("uint8")
    rgba[:3] = flat.T.reshape((3,) + emb.shape[1:])
    rgba[3] = ok.reshape(emb.shape[1:]) * 255
    return rgba


@fused.cache(cache_max_age="30d")
def _aef_files(year):
    import fsspec
    import pyarrow.parquet as pq

    cols = ["path", "wgs84_west", "wgs84_south", "wgs84_east", "wgs84_north"]
    with fsspec.open(AEF_INDEX).open() as f:
        return pq.read_table(f, columns=cols + ["year"], filters=[("year", "=", int(year))]).select(cols).to_pandas()


def _aef_overview_level(res):
    level = -1
    while 2 ** (level + 2) * 10 <= res and level < 11:
        level += 1
    return level


def _read_aef(path, bands, gb, level):
    import warnings
    import numpy as np
    import rasterio
    from rasterio.warp import Resampling, reproject, transform_bounds
    from rasterio.windows import Window

    try:
        with rasterio.open("/vsis3/" + path[5:] if path.startswith("s3://") else path, **({"overview_level": level} if level >= 0 else {})) as src:
            bb = transform_bounds(str(gb.crs), src.crs, *gb.extent.boundingbox)
            cs, rs = zip(*[~src.transform * (x, y) for x in (bb[0], bb[2]) for y in (bb[1], bb[3])])
            c0, c1 = max(0, int(min(cs)) - 1), min(src.width, int(max(cs)) + 2)
            r0, r1 = max(0, int(min(rs)) - 1), min(src.height, int(max(rs)) + 2)
            if c1 <= c0 or r1 <= r0:
                return None
            win = Window(c0, r0, c1 - c0, r1 - r0)
            arr, src_t, src_crs = src.read(bands, window=win), src.window_transform(win), src.crs
    except rasterio.errors.RasterioIOError as e:
        warnings.warn(f"AEF read failed for {path}: {str(e)[:80]}")
        return None
    dst = np.full((len(bands),) + tuple(gb.shape), -128, dtype="int8")
    reproject(arr, dst, src_transform=src_t, src_crs=src_crs, src_nodata=-128, dst_transform=gb.affine, dst_crs=str(gb.crs),
              dst_nodata=-128, resampling=Resampling.nearest)
    return bands, dst


def _unit(v):
    import numpy as np

    v = np.asarray(v, dtype="float32")
    return v / (np.linalg.norm(v) or 1.0)


INDEX_STYLE = {
    "vegetation": ("RdYlGn", -0.2, 0.9),
    "water": ("RdBu", -0.6, 0.6),
    "burn": ("RdYlGn", -0.5, 0.8),
    "snow": ("Blues", -0.2, 1.0),
    "urban": ("magma", -0.5, 0.3),
    "soil": ("YlOrBr", -0.3, 0.3),
    "clouds": ("Greys", 0.0, 1.0),
}
BURN_COLORS = ["#1a9850", "#91cf60", "#d9d9d9", "#fee08b", "#fdae61", "#f46d43", "#a50026"]


def render(ds, what="true_color", *, cmap=None, vmin=None, vmax=None, gamma=None):
    """A preset, catalog index or formula of a 2-D Dataset -> uint8 RGBA (4, H, W). Alpha is 0 where there is no data.

    Presets use fixed stretches and indices use the colour map and range of their domain, so map tiles compare.
    """
    import numpy as np

    if "time" in ds.dims:
        raise ValueError("render needs one image: composite the time axis first")
    if what in PRESETS:
        names, lo, hi, gm = PRESETS[what]
        missing = [n for n in names if n not in ds]
        if missing:
            raise ValueError(f"{what} needs bands {missing}")
        lo, hi = lo if vmin is None else vmin, hi if vmax is None else vmax
        rgb = np.stack([stretch(ds[n], lo, hi, gamma or gm).values for n in names])
        alpha = np.all([ds[n].notnull().values for n in names], axis=0)
        return np.concatenate([rgb, (alpha * 255).astype("uint8")[None]])
    s_cmap, s_min, s_max = render_style(what)
    return colorize(index(ds, what), cmap or s_cmap, s_min if vmin is None else vmin, s_max if vmax is None else vmax)


def render_style(what):
    """(cmap, vmin, vmax) for a catalog index from its domain; (viridis, None, None) for a formula."""
    if is_index(what):
        return INDEX_STYLE.get(index_info(what)["domain"], ("viridis", None, None))
    return "viridis", None, None


def stretch(da, lo, hi, gamma=1.0):
    """Values -> uint8 with a linear stretch from lo to hi and a gamma. NaN -> 0."""
    if hi == lo:
        raise ValueError("stretch needs lo != hi")
    x = ((da - lo) / (hi - lo)).clip(0, 1)
    if gamma != 1.0:
        x = x ** (1.0 / gamma)
    return (x * 255).fillna(0).astype("uint8")


def colorize(da, cmap="viridis", vmin=None, vmax=None):
    """2-D values -> uint8 RGBA through a matplotlib colour map. vmin / vmax default to the 2nd / 98th percentile;
    vmin > vmax reverses the ramp."""
    import matplotlib
    import numpy as np

    arr = np.asarray(da.values, dtype="float32")
    valid = np.isfinite(arr)
    if vmin is None or vmax is None:
        lo, hi = np.percentile(arr[valid], [2, 98]) if valid.any() else (0.0, 1.0)
        vmin, vmax = lo if vmin is None else vmin, hi if vmax is None else vmax
    span = float(vmax) - float(vmin)
    with np.errstate(invalid="ignore"):
        x = np.clip((arr - vmin) / (span if span else 1e-9), 0, 1)
    rgba = (matplotlib.colormaps[cmap](np.nan_to_num(x)) * 255).astype("uint8")
    rgba[..., 3] = np.where(valid, 255, 0)
    return np.moveaxis(rgba, -1, 0)


def colorize_classes(da, colors=None):
    """Class codes (NaN = none) -> uint8 RGBA, one colour per code (any matplotlib colour; default tab10, or tab20
    above 10 classes)."""
    import matplotlib
    import numpy as np

    arr = np.asarray(da.values, dtype="float32")
    ok = np.isfinite(arr)
    n = int(arr[ok].max()) + 1 if ok.any() else 0
    if colors is None:
        cmap = matplotlib.colormaps["tab10" if n <= 10 else "tab20"]
        colors = [cmap(i % cmap.N) for i in range(n)]
    elif len(colors) < n:
        raise ValueError(f"{n} classes but {len(colors)} colours")
    out = np.zeros((4,) + arr.shape, dtype="uint8")
    for i, c in enumerate(colors):
        sel = ok & (arr == i)
        out[:3, sel] = (np.array(matplotlib.colors.to_rgb(c)) * 255).round().astype("uint8")[:, None]
        out[3, sel] = 255
    return out


def css_gradient(cmap, n=6):
    """CSS linear-gradient of a matplotlib colour map (for legends in pages)."""
    import matplotlib

    n = max(int(n), 2)
    stops = ",".join(matplotlib.colors.to_hex(matplotlib.colormaps[cmap](i / (n - 1))) for i in range(n))
    return f"linear-gradient(90deg,{stops})"


TILE_REGION_ZOOM = 8
TILE_LOW_ZOOM = 9
TILE_STOP_SHARE = {"high": 0.995, "low": 0.97}
TILE_MAX_ROUNDS = {"high": 6, "low": 3}
TILE_MAX_SCENES = 48
TILE_PER_MGRS = 6
MOSAIC_MAX_VALUES = 50_000_000


def tile(z, x, y, date, *, days=DAYS, what="true_color", method="best", max_cloud=MAX_CLOUD, per_mgrs=TILE_PER_MGRS,
         order="clear", cloud_mask=True, size=256, cmap=None, vmin=None, vmax=None, min_zoom=6):
    """One uint8 RGBA (4, size, size) map tile of `what` (preset, index or formula) around `date` (± `days`).

    Scenes are ranked once per zoom-8 region, so neighbouring tiles agree, and read with `mosaic` (method best:
    stop at 99.5 % done, 97 % below zoom 9; method median / mean / pNN: reduce the clear values).
    """
    import numpy as np
    import xarray as xr

    empty = np.zeros((4, int(size), int(size)), dtype="uint8")
    if int(z) < int(min_zoom):
        return empty
    scenes = tile_scenes(z, x, y, date, days=days, max_cloud=max_cloud, per_mgrs=per_mgrs, order=order)
    if not len(scenes):
        return empty
    bands = needs(what)
    level = "low" if int(z) < TILE_LOW_ZOOM else "high"
    data, _ = mosaic(scenes, bands, tile_geobox(z, x, y, size), cloud_mask=cloud_mask, method=method,
                     stop=TILE_STOP_SHARE[level], max_rounds=TILE_MAX_ROUNDS[level], max_scenes=TILE_MAX_SCENES)
    if np.isnan(data).all():
        return empty
    return render(xr.Dataset({n: (("y", "x"), data[i]) for i, n in enumerate(bands)}), what, cmap=cmap, vmin=vmin, vmax=vmax)


def tile_scenes(z, x, y, date, *, days=DAYS, max_cloud=MAX_CLOUD, per_mgrs=TILE_PER_MGRS, order="clear"):
    """The scenes tile z/x/y reads, best first (ranked per zoom-8 region)."""
    import mercantile
    import shapely

    t = mercantile.Tile(int(x), int(y), int(z))
    region = mercantile.bounds(mercantile.parent(t, zoom=TILE_REGION_ZOOM) if t.z > TILE_REGION_ZOOM else t)
    rbox = [round(v, 6) for v in region]
    start, end = date_window(date, days)
    ranked = rank(search(rbox, start, end, max_cloud=max_cloud), rbox, date=date, per_mgrs=per_mgrs, order=order)
    return ranked[ranked.intersects(shapely.box(*mercantile.bounds(t)))].reset_index(drop=True) if len(ranked) else ranked


def tile_geobox(z, x, y, size=256):
    """The Web Mercator grid of map tile z/x/y with `size` x `size` pixels."""
    import mercantile
    from odc.geo.geobox import GeoBox

    b = mercantile.xy_bounds(int(x), int(y), int(z))
    return GeoBox.from_bbox((b.left, b.bottom, b.right, b.top), crs="EPSG:3857", shape=(int(size), int(size)))


def bounds_to_tile(bounds):
    """Web Mercator tile (z, x, y) of tile bounds [w, s, e, n], as a Fused tile UDF receives them."""
    import math
    import mercantile

    w, s, e, n = [float(v) for v in bounds]
    t = mercantile.tile((w + e) / 2, (s + n) / 2, max(0, int(round(math.log2(360.0 / max(e - w, 1e-9))))))
    return t.z, t.x, t.y


def mosaic(scenes, bands, gb, *, cloud_mask=True, method="best", stop=0.995, max_rounds=6, max_scenes=TILE_MAX_SCENES,
           keep_obs=True, key_band=0, threads=32):
    """Pixels of ranked scenes (`rank`) on the grid `gb`. Returns (reflectance float32 (n, H, W), report).

    method best: rounds of each MGRS tile's next best scene. The first clear pixel wins; cloud (SCL 8, 9, 10) is
    filled from later rounds. Reading stops when at most 1 - `stop` of the grid is not yet clear where a later scene
    still has data, or after `max_rounds`. A pixel that is never clear takes the median of its observations
    (`keep_obs`) or its darkest value in band `key_band`. method median / mean / pNN: reduce the clear values of up
    to `max_scenes` scenes. The result is NaN where no scene has data. Scenes are read in batches that keep about
    MOSAIC_MAX_VALUES values in memory. `gb` must be north-up.
    report: scenes (ids read, in order), clear_pct (share of pixels with data that are clear), rounds.
    """
    from concurrent.futures import ThreadPoolExecutor
    import warnings
    import numpy as np

    if gb.affine.e > 0 or gb.affine.b or gb.affine.d:
        raise ValueError("mosaic needs a north-up grid without rotation")
    bands = list(bands)
    stat = method in ("mean", "median") or (method[:1] == "p" and method[1:].isdigit() and int(method[1:]) <= 100)
    if method != "best" and not stat:
        raise ValueError("method must be best, median, mean or pNN")
    if method == "best":
        if "mgrs_rank" not in scenes:
            raise ValueError("method best needs ranked scenes: pass them through rank() first")
        groups = [g for _, g in scenes.groupby("mgrs_rank", sort=True)][:int(max_rounds)]
    else:
        groups = [scenes]
    kept, n = [], 0
    for g in groups:
        g = g.head(max(0, int(max_scenes) - n))
        if len(g):
            kept.append(g)
            n += len(g)
    fill = BestFill(len(bands), gb.shape, keep_obs=keep_obs, key_band=key_band)
    reach = _reach(kept, gb) if method == "best" else None
    stack, used, rounds = [], [], 0
    batch = max(1, MOSAIC_MAX_VALUES // (gb.shape[0] * gb.shape[1] * (len(bands) + 1)))
    with ThreadPoolExecutor(int(threads)) as pool:
        for k, g in enumerate(kept):
            rounds += 1
            for i in range(0, len(g), batch):
                for sid, got in read_scenes(g.iloc[i:i + batch], bands, gb, cloud_mask=cloud_mask, pool=pool):
                    if got is None:
                        continue
                    used.append(sid)
                    fill.add(*got)
                    if stat:
                        stack.append(np.where(got[2][None], got[0], np.nan))
            if reach is not None and k + 1 < len(kept) and (reach[k + 1] & ~fill.filled).mean() <= 1 - float(stop):
                break
    data, alpha = fill.result()
    if stat and stack:
        arr = np.stack(stack)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            out = np.nanmean(arr, axis=0).astype("float32") if method == "mean" else nanquantile(arr, 0.5 if method == "median" else int(method[1:]) / 100)
        holes = np.isnan(out).any(0)
        out[:, holes] = data[:, holes]
        data = out
    data[:, ~alpha] = np.nan
    return data, {"scenes": used, "clear_pct": round(fill.clear_share() * 100, 1), "rounds": rounds}


def read_scenes(scenes, bands, gb, *, cloud_mask=True, pool=None):
    """Read `bands` (+ scl) of each scene on the grid `gb`, in parallel.

    Returns one (scene id, result) per scene, in order. result is (reflectance float32 (n, H, W) NaN outside the
    data, valid bool (H, W), clear bool (H, W)), or None when the scene has no data on the grid. With cloud_mask,
    a scene whose scl cannot be read has no clear pixel.
    """
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np

    names = list(bands) + (["scl"] if cloud_mask else [])
    rows = [scenes.iloc[i] for i in range(len(scenes))]
    jobs = [(i, n) for i in range(len(rows)) for n in names]

    def go(job):
        href = rows[job[0]].get(f"href_{job[1]}")
        return _read_band(href, gb, job[1] == "scl") if isinstance(href, str) else (None, None)

    if pool is None:
        with ThreadPoolExecutor(min(32, max(1, len(jobs)))) as ex:
            got = dict(zip(jobs, ex.map(go, jobs)))
    else:
        got = dict(zip(jobs, pool.map(go, jobs)))
    out = []
    for i, r in enumerate(rows):
        parts = [got[(i, n)] for n in bands]
        if any(d is None for d, _ in parts):
            out.append((r["id"], None))
            continue
        raw = np.stack([d for d, _ in parts]).astype("float32")
        valid = np.all([m for _, m in parts], axis=0) & np.all(raw > 0, axis=0)
        if not valid.any():
            out.append((r["id"], None))
            continue
        if cloud_mask:
            scl = got[(i, "scl")][0]
            clear = valid & ~np.isin(scl, SCL_GROUPS["cloud"]) if scl is not None else np.zeros_like(valid)
        else:
            clear = valid
        out.append((r["id"], (np.where(valid[None], raw * SCALE + float(r["offset"]), np.nan).astype("float32"), valid, clear)))
    return out


def _reach(groups, gb):
    import numpy as np
    from rasterio.features import rasterize

    res = abs(gb.resolution.x)
    masks = []
    for g in groups:
        geoms = [geom for geom in g.geometry.to_crs(gb.crs.to_wkt()).buffer(-2 * res) if geom is not None and not geom.is_empty]
        masks.append(rasterize([(geom, 1) for geom in geoms], out_shape=gb.shape, transform=gb.affine, fill=0, dtype="uint8") > 0
                     if geoms else np.zeros(gb.shape, bool))
    later, acc = [None] * len(masks), np.zeros(gb.shape, bool)
    for k in range(len(masks) - 1, -1, -1):
        acc = acc | masks[k]
        later[k] = acc
    return later


def _read_band(href, gb, nearest, retries=2):
    import time
    import warnings
    import rasterio
    from rasterio.crs import CRS
    from rio_tiler.errors import TileOutsideBounds
    from rio_tiler.io import Reader

    crs = CRS.from_wkt(gb.crs.to_wkt())
    bb = gb.extent.boundingbox
    how = "nearest" if nearest else "bilinear"
    for attempt in range(retries + 1):
        try:
            with rasterio.Env(**GDAL_ENV), Reader(href) as r:
                img = r.part((bb.left, bb.bottom, bb.right, bb.top), dst_crs=crs, bounds_crs=crs, width=gb.shape[1],
                             height=gb.shape[0], max_size=None, resampling_method=how, reproject_method=how)
            return img.data[0], img.mask > 0
        except TileOutsideBounds:
            return None, None
        except rasterio.errors.RasterioIOError as e:
            if any(s in str(e).lower() for s in ("404", "does not exist", "no such file")):
                return None, None
            if attempt == retries:
                warnings.warn(f"read failed for {href}: {str(e)[:120]}")
                return None, None
            time.sleep(0.5 * (attempt + 1))


EXPORT_MAX_PX = 4096
EXPORT_MAX_VALUES = 120_000_000
EXPORT_DIR = "fd://sentinel_utils/exports"


def export(aoi, date, *, days=DAYS, kind="rgb", what="true_color", bands="blue,green,red,nir", method="best",
           cloud_mask=True, max_cloud=MAX_CLOUD, per_mgrs=6, max_px=EXPORT_MAX_PX, res=None, crs="utm", cmap=None,
           vmin=None, vmax=None, out_dir=EXPORT_DIR):
    """Write `aoi` around `date` (± days) as one COG. Returns a dict: url (signed for 1 h; None for a local out_dir),
    path, size, scenes, clear_pct, seconds.

    kind rgb: uint8 RGBA of `what` (preset, index or formula, with cmap / vmin / vmax). kind bands: reflectance
    `bands` as uint16 DN = reflectance * 10000 + 1000 (nodata 0) with scale 0.0001 and offset -0.1 set, so GIS tools
    read reflectance. method best picks pixels as `tile` does (a pixel never clear takes its darkest blue value);
    median, mean and pNN reduce the clear values of the best scenes that fit in EXPORT_MAX_VALUES. Resolution:
    `res` metres, else the finest multiple of 10 m that keeps the long side at or under `max_px`.
    """
    import hashlib
    import json
    import os
    import tempfile
    import time
    import numpy as np
    import xarray as xr

    t0 = time.time()
    if kind not in ("rgb", "bands"):
        raise ValueError("kind must be rgb or bands")
    if int(max_px) > EXPORT_MAX_PX:
        raise ValueError(f"max_px is limited to {EXPORT_MAX_PX}; for larger areas use save_tiles or run_blocks")
    names = needs(what) if kind == "rgb" else band_names(bands)
    if kind == "bands" and any(is_mask(n) for n in names):
        raise ValueError("kind bands writes reflectance; leave out scl, cloud and snow")
    bounds = bbox(aoi)
    res, gb = _export_grid(bounds, res, crs, max_px)
    if gb.shape[0] * gb.shape[1] * (len(names) + 1) > EXPORT_MAX_VALUES:
        raise ValueError(f"{gb.shape[0]}x{gb.shape[1]} px x {len(names) + 1} bands is too large; raise res or lower max_px")
    key = hashlib.sha1(json.dumps([bounds, _day(date), days, kind, what, names, method, cloud_mask, max_cloud, per_mgrs,
                                   res, str(crs), cmap, vmin, vmax], default=str).encode()).hexdigest()[:12]
    start, end = date_window(date, days)
    scenes = rank(search(bounds, start, end, max_cloud=max_cloud), bounds, date=date, per_mgrs=per_mgrs)
    if not len(scenes):
        raise NoData("no scenes for this box and window")
    px = gb.shape[0] * gb.shape[1]
    opts = dict(cloud_mask=cloud_mask, keep_obs=False, key_band=names.index("blue") if "blue" in names else 0,
                threads=max(4, min(32, EXPORT_MAX_VALUES // px)))
    if method == "best":
        data, rep = mosaic(scenes, names, gb, stop=0.97 if res >= 100 else 0.995, max_rounds=per_mgrs,
                           max_scenes=len(scenes), **opts)
    else:
        data, rep = mosaic(scenes, names, gb, method=method, max_scenes=max(1, EXPORT_MAX_VALUES // (px * len(names))), **opts)
    if np.isnan(data).all():
        raise NoData("no scene has data in this box")
    used, clear = rep["scenes"], rep["clear_pct"]
    local = os.path.join(tempfile.mkdtemp(), f"s2_{kind}_{key}.tif")
    if kind == "rgb":
        rgba = render(xr.Dataset({n: (("y", "x"), data[i]) for i, n in enumerate(names)}), what, cmap=cmap, vmin=vmin, vmax=vmax)
        _write_tif(local, gb, rgba, colorinterp="rgba")
    else:
        dn = np.where(np.isnan(data), 0, np.clip(np.round(data / SCALE + 1000), 1, 65535)).astype("uint16")
        _write_tif(local, gb, dn, nodata=0, names=names, scale=SCALE, offset=-0.1)
    path = save_file(local, f"{str(out_dir).rstrip('/')}/{os.path.basename(local)}")
    url = fused.api.sign_url(fused.api.resolve(path)) if path.startswith(("fd://", "s3://")) else None
    return {"url": url, "path": path, "kind": kind, "what": what if kind == "rgb" else ",".join(names),
            "width": int(gb.shape[1]), "height": int(gb.shape[0]), "res_m": res,
            "crs": f"EPSG:{gb.crs.epsg}" if gb.crs.epsg else str(gb.crs), "mb": round(os.path.getsize(local) / 1e6, 2),
            "scenes": ",".join(used), "clear_pct": clear, "seconds": round(time.time() - t0, 1)}


def _export_grid(bounds, res, crs, max_px):
    auto = not res
    res = auto_res(bounds, max_px) if auto else float(res)
    gb = geobox(bounds, res, crs)
    while auto and max(gb.shape) > int(max_px):
        res += 10
        gb = geobox(bounds, res, crs)
    if max(gb.shape) > int(max_px):
        raise ValueError(f"{gb.shape[0]}x{gb.shape[1]} px is larger than max_px {max_px}; raise res")
    return res, gb


def to_cog(data, path, *, nodata=None):
    """Write a 2-D DataArray, or a Dataset of 2-D variables (one band each, named), as a COG. fd:// and s3:// paths
    are uploaded. Returns `path`."""
    import os
    import tempfile
    import numpy as np
    import xarray as xr

    if isinstance(data, xr.Dataset):
        names, arrs = list(data.data_vars), [data[v] for v in data.data_vars]
    else:
        names, arrs = [str(data.name or "value")], [data]
    for a in arrs:
        if set(a.dims) != {"y", "x"}:
            raise ValueError(f"to_cog writes 2-D (y, x) layers; {a.name} has dims {a.dims}")
    if _odc(data).crs is None:
        raise ValueError("data has no CRS; set one with keep_crs or .odc.assign_crs")
    arr = np.stack([np.asarray(a.transpose("y", "x").values) for a in arrs])
    local = os.path.join(tempfile.mkdtemp(), os.path.basename(path))
    _write_tif(local, _odc(data).geobox, arr, nodata=nodata, names=names)
    return save_file(local, path)


def to_h3(da, h3_res=9, method="avg"):
    """2-D DataArray -> DataFrame(hex, value, n): pixel centres aggregated per H3 cell with DuckDB.

    method: avg, min, max, sum, median or stddev.
    """
    import duckdb
    import numpy as np
    import pandas as pd
    from rasterio.warp import transform

    if method not in ("avg", "min", "max", "sum", "median", "stddev"):
        raise ValueError("method must be avg, min, max, sum, median or stddev")
    if not 0 <= int(h3_res) <= 15:
        raise ValueError("h3_res must be 0..15")
    xs, ys = np.meshgrid(da["x"].values, da["y"].values)
    v = np.asarray(da.transpose("y", "x").values, dtype="float32").ravel()
    ok = ~np.isnan(v)
    lng, lat = transform(str(_odc(da).crs), "EPSG:4326", xs.ravel()[ok], ys.ravel()[ok])
    df = pd.DataFrame({"lat": lat, "lng": lng, "v": v[ok]})
    con = duckdb.connect()
    con.sql("INSTALL h3 FROM community; LOAD h3;")
    out = con.sql(f"SELECT h3_latlng_to_cell(lat, lng, {int(h3_res)}) AS hex, {method}(v) AS value, count(*) AS n FROM df GROUP BY 1").df()
    con.close()
    return out


def _write_tif(path, gb, arr, *, nodata=None, colorinterp=None, names=None, scale=None, offset=None):
    import numpy as np
    import rasterio

    if arr.dtype == bool:
        arr = arr.astype("uint8")
    elif arr.dtype.kind in "iu" and arr.dtype.itemsize == 8:
        arr = arr.astype("int32") if np.abs(arr).max(initial=0) < 2 ** 31 else arr.astype("float64")
    floating = np.issubdtype(arr.dtype, np.floating)
    profile = dict(driver="COG", width=gb.shape[1], height=gb.shape[0], count=arr.shape[0], dtype=str(arr.dtype),
                   crs=str(gb.crs), transform=gb.affine, compress="deflate", predictor=3 if floating else 2, blocksize=512,
                   overview_resampling="average", BIGTIFF="IF_SAFER", nodata=nodata)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr)
        if colorinterp == "rgba":
            ci = rasterio.enums.ColorInterp
            dst.colorinterp = [ci.red, ci.green, ci.blue, ci.alpha]
        if names:
            dst.descriptions = tuple(names)
        if scale is not None:
            dst.scales = [scale] * arr.shape[0]
            dst.offsets = [offset] * arr.shape[0]


def block_grid(aoi, zoom=12):
    """Web Mercator tiles at `zoom` covering `aoi`, as a GeoDataFrame (z, x, y, name, bounds, geometry)."""
    import geopandas as gpd
    import mercantile
    import shapely

    rows = []
    for t in mercantile.tiles(*bbox(aoi), zooms=int(zoom)):
        b = mercantile.bounds(t)
        rows.append({"z": t.z, "x": t.x, "y": t.y, "name": f"z{t.z}_x{t.x}_y{t.y}", "bounds": [b.west, b.south, b.east, b.north],
                     "geometry": shapely.box(*b)})
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


def run_fanout(worker, args, *, max_workers=64, on_error="report"):
    """fused.submit `worker` over a list of argument dicts and wait. Returns the concatenated DataFrame of successes,
    in argument order.

    worker: a UDF, or the name of a worker in this file (block_worker, tile_worker, frame_worker).
    on_error: report (warn with the first failure and continue) | raise | ignore.
    """
    import warnings
    import pandas as pd

    if on_error not in ("report", "raise", "ignore"):
        raise ValueError("on_error must be report, raise or ignore")
    if not len(args):
        return pd.DataFrame()
    name = worker if isinstance(worker, str) else getattr(worker, "name", None) or "udf"
    job = fused.submit(worker_udf(worker) if isinstance(worker, str) else worker, args, max_workers=int(max_workers), collect=False)
    job.wait()
    failed = [(i, (str(e).strip().splitlines() or [repr(e)])[-1][:200]) for i, e in sorted(job.errors().items())]
    if failed and on_error != "ignore":
        msg = f"{name}: {len(failed)} of {len(args)} failed; first: arg #{failed[0][0]}: {failed[0][1]}"
        if on_error == "raise":
            raise RuntimeError(msg)
        warnings.warn(msg)
    frames = [r for _, r in sorted(job.success().items()) if isinstance(r, pd.DataFrame)]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def worker_udf(name):
    """A worker of this file as a UDF for fused.submit. Its code is this whole file (the copy that was loaded), so it
    can call every function. The cache time comes from the worker's own @fused.udf decorator."""
    if name not in ("block_worker", "tile_worker", "frame_worker"):
        raise ValueError("worker must be block_worker, tile_worker or frame_worker")
    if f"def {name}(" not in _SOURCE:
        raise RuntimeError("the source of this file is not available; load it with fused.load(...) before a fan-out")
    base = fused.load(_SOURCE, import_globals=False)
    return type(base).model_validate({**base.model_dump(), "entrypoint": name, "name": name, "cache_max_age": None},
                                     context={"import_globals": False, "load_parameter_list": True})


def _module_source():
    import linecache

    return "".join(linecache.getlines(_module_source.__code__.co_filename))


_SOURCE = _module_source()


@fused.udf(cache_max_age="0s")
def block_worker(bounds: list = [-74.0, 40.75, -73.96, 40.78], name: str = "block", scenes_json: str = "",
                 bands: str = "red,green,blue", method: str = "median", px: int = 512, out_path: str = ""):
    """One block: composite the given scenes on a Web Mercator grid. Returns band means; with out_path, writes a COG."""
    import time
    import pandas as pd

    t0 = time.time()
    scenes = from_json(scenes_json)
    row = {"name": name, "scenes": len(scenes), "clear_pct": 0.0, "path": "", "seconds": 0.0}
    if len(scenes):
        names = band_names(bands)
        comp = composite(load(scenes, names, bounds, crs="EPSG:3857", shape=(int(px), int(px)), keep="clear"), method)
        row.update(clear_pct=round(float(comp[names[0]].notnull().mean()) * 100, 1), **{n: round(float(comp[n].mean()), 4) for n in names})
        if out_path:
            row["path"] = to_cog(comp, f"{out_path.rstrip('/')}/{name}.tif")
    row["seconds"] = round(time.time() - t0, 1)
    return pd.DataFrame([row])


def run_blocks(aoi, start, end, *, zoom=13, bands="red,green,blue", method="median", max_cloud=MAX_CLOUD, px=512,
               out_dir="", max_workers=64):
    """Search once, then one `block_worker` per Web Mercator tile at `zoom`. Returns one row per block.

    With `out_dir`, each block is also written as a COG. Workers that write files are not cached.
    """
    names = band_names(bands)
    blocks = block_grid(aoi, zoom)
    scenes = search(aoi, start, end, max_cloud=max_cloud)
    args = [{"bounds": b.bounds, "name": b.name, "scenes_json": to_json(scenes[scenes.intersects(b.geometry)], names + ["scl"]),
             "bands": ",".join(names), "method": method, "px": int(px), "out_path": out_dir} for b in blocks.itertuples()]
    return run_fanout("block_worker", args, max_workers=max_workers)


@fused.udf(cache_max_age="0s")
def tile_worker(tile_zxy: str = "13/2412/3078", date: str = "2024-07-01", days: int = 30, what: str = "true_color",
                max_cloud: float = 80, out_dir: str = "fd://sentinel_utils/tiles/demo", fmt: str = "webp"):
    """Render one map tile and write it to {out_dir}/{z}/{x}/{y}.{fmt}. Takes "z/x/y" as one string."""
    import os
    import tempfile
    import numpy as np
    import pandas as pd
    from PIL import Image

    z, x, y = [int(v) for v in tile_zxy.split("/")]
    rgba = tile(z, x, y, date, days=int(days), what=what, max_cloud=float(max_cloud), min_zoom=0)
    row = {"tile": tile_zxy, "path": "", "empty": bool(rgba[3].max() == 0)}
    if not row["empty"]:
        local = os.path.join(tempfile.mkdtemp(), f"{y}.{fmt}")
        Image.fromarray(np.moveaxis(rgba, 0, -1)).save(local, format=fmt.upper(), quality=85)
        row["path"] = save_file(local, f"{out_dir.rstrip('/')}/{z}/{x}/{y}.{fmt}")
    return pd.DataFrame([row])


def save_tiles(aoi, zooms, date, out_dir, *, days=DAYS, what="true_color", max_cloud=MAX_CLOUD, fmt="webp",
               max_tiles=2000, max_workers=64):
    """Write a static XYZ pyramid {out_dir}/{z}/{x}/{y}.{fmt} (webp or png) for `aoi` and a zoom (or (min, max) zoom
    range), one worker per tile. Any static file host can then serve it (a bucket behind a MapLibre raster source,
    QGIS XYZ, a CDN). Empty tiles are not written."""
    import mercantile

    if fmt not in ("webp", "png"):
        raise ValueError("fmt must be webp or png")
    z0, z1 = (int(zooms), int(zooms)) if isinstance(zooms, (int, float, str)) else (int(zooms[0]), int(zooms[-1]))
    if not 0 <= z0 <= z1 <= 22:
        raise ValueError(f"zooms must be 0..22 with min <= max, got {zooms!r}")
    tiles = [t for z in range(z0, z1 + 1) for t in mercantile.tiles(*bbox(aoi), zooms=z)]
    if len(tiles) > int(max_tiles):
        raise ValueError(f"{len(tiles)} tiles > max_tiles={max_tiles}; cut the AOI or the zoom range")
    args = [{"tile_zxy": f"{t.z}/{t.x}/{t.y}", "date": _day(date), "days": int(days), "what": what, "max_cloud": float(max_cloud),
             "out_dir": out_dir, "fmt": fmt} for t in tiles]
    return run_fanout("tile_worker", args, max_workers=max_workers)


SEASONS = {12: "DJF", 3: "MAM", 6: "JJA", 9: "SON"}


def period_ranges(start, end, period="month"):
    """[(label, start, end)] covering start..end in time order. period: month, quarter, season (DJF, MAM, JJA, SON),
    year, or a number of days such as 10D. The first and last ranges are cut at start and end."""
    import re
    import pandas as pd

    t0, t1 = pd.Timestamp(_day(start)), pd.Timestamp(_day(end))
    if t1 < t0:
        raise ValueError(f"end {t1:%Y-%m-%d} is before start {t0:%Y-%m-%d}")
    out = []
    if period in ("month", "quarter", "season", "year"):
        freq = {"month": "M", "quarter": "Q", "season": "Q-NOV", "year": "Y"}[period]
        for p in pd.period_range(t0, t1, freq=freq):
            a, b = p.start_time, p.end_time.normalize()
            label = {"month": lambda: f"{a:%Y-%m}", "quarter": lambda: f"{a.year} Q{a.quarter}",
                     "season": lambda: f"{a.year} {SEASONS[a.month]}", "year": lambda: f"{a.year}"}[period]()
            out.append((label, f"{max(a, t0):%Y-%m-%d}", f"{min(b, t1):%Y-%m-%d}"))
        return out
    m = re.fullmatch(r"(\d+)D", str(period))
    if not m or int(m.group(1)) < 1:
        raise ValueError("period must be month, quarter, season, year or a number of days such as 10D")
    step = pd.Timedelta(days=int(m.group(1)))
    a = t0
    while a <= t1:
        b = min(a + step - pd.Timedelta(days=1), t1)
        out.append((f"{a:%Y-%m-%d}", f"{a:%Y-%m-%d}", f"{b:%Y-%m-%d}"))
        a = a + step
    return out


@fused.udf(cache_max_age="7d")
def frame_worker(bounds: list = [-118.62, 34.02, -118.50, 34.10], label: str = "frame", scenes_json: str = "",
                 what: str = "true_color", method: str = "median", size: int = 512, keep: str = "clear",
                 cmap: str = "", vmin: str = "", vmax: str = ""):
    """One timelapse frame: composite the given scenes on a Web Mercator grid (long side `size` px), render, return a
    PNG (base64)."""
    import base64
    import io
    import numpy as np
    import pandas as pd
    from PIL import Image

    row = {"label": label, "scenes": 0, "clear_pct": 0.0, "png": ""}
    scenes = from_json(scenes_json)
    if not len(scenes):
        return pd.DataFrame([row])
    ds = load(scenes, needs(what), bounds, crs="EPSG:3857", shape=_mercator_shape(bounds, size), keep=keep)
    rgba = render(composite(ds, method), what, cmap=cmap or None, vmin=float(vmin) if vmin else None, vmax=float(vmax) if vmax else None)
    buf = io.BytesIO()
    Image.fromarray(np.moveaxis(rgba, 0, -1)).save(buf, format="PNG")
    row.update(scenes=len(scenes), clear_pct=round(float((rgba[3] > 0).mean()) * 100, 1), png=base64.b64encode(buf.getvalue()).decode())
    return pd.DataFrame([row])


def timelapse(aoi, start, end, *, period="month", what="true_color", method="median", size=512, max_cloud=MAX_CLOUD,
              min_clear=60, months=None, keep="clear", cmap=None, vmin=None, vmax=None, fps=2, fmt="webp", max_workers=32):
    """Animation bytes (webp or gif) and a frame table (label, scenes, clear_pct, used) in period order.

    The search runs once; each period is one `frame_worker`. Frames with less than `min_clear` % of pixels with data
    are dropped and marked in the table. WebP is about 5x smaller than GIF for imagery.
    """
    import base64
    import io
    import numpy as np
    from PIL import Image

    if fmt not in ("webp", "gif"):
        raise ValueError("fmt must be webp or gif")
    bounds = bbox(aoi)
    plan = period_ranges(start, end, period)
    scenes = search(bounds, start, end, max_cloud=max_cloud)
    if months and len(scenes):
        scenes = scenes[scenes["date"].str[5:7].astype(int).isin([int(m) for m in months])]
    bands = needs(what) + ["scl"]
    args = []
    for label, lo, hi in plan:
        sub = scenes[(scenes["date"] >= lo) & (scenes["date"] <= hi)] if len(scenes) else scenes
        if len(sub):
            args.append({"bounds": bounds, "label": label, "scenes_json": to_json(sub, bands), "what": what, "method": method,
                         "size": int(size), "keep": keep, "cmap": cmap or "", "vmin": "" if vmin is None else str(vmin),
                         "vmax": "" if vmax is None else str(vmax)})
    if not args:
        raise NoData("no scenes in any period")
    table = run_fanout("frame_worker", args, max_workers=max_workers)
    if not len(table):
        raise RuntimeError("every frame failed; see the run_fanout warning")
    order = {label: i for i, (label, _, _) in enumerate(plan)}
    table = table.assign(_o=table["label"].map(order)).sort_values("_o").drop(columns="_o").reset_index(drop=True)
    table["used"] = table["clear_pct"] >= float(min_clear)
    if not table["used"].any():
        raise NoData(f"no frame reached {min_clear} % clear pixels; lower min_clear or widen the period")
    frames = [np.moveaxis(np.array(Image.open(io.BytesIO(base64.b64decode(p)))), -1, 0) for p in table.loc[table["used"], "png"]]
    return animate(frames, list(table.loc[table["used"], "label"]), fps=fps, fmt=fmt), table.drop(columns="png")


def animate(frames, labels=None, fps=2, fmt="webp", quality=75, background=(255, 255, 255)):
    """uint8 RGBA (4, H, W) frames of one size -> animated WebP or GIF bytes, with an optional label in the top-left
    corner."""
    import io
    import numpy as np
    from PIL import Image, ImageDraw, ImageFont

    if fmt not in ("webp", "gif"):
        raise ValueError("fmt must be webp or gif")
    if not len(frames):
        raise ValueError("animate needs at least one frame")
    if len({np.shape(f) for f in frames}) > 1:
        raise ValueError(f"frames must have one size, got {sorted({np.shape(f) for f in frames})}")
    if labels is not None and len(labels) != len(frames):
        raise ValueError("give one label per frame")
    out = []
    for i, f in enumerate(frames):
        rgba = Image.fromarray(np.moveaxis(np.asarray(f, dtype="uint8"), 0, -1), "RGBA")
        im = Image.new("RGB", rgba.size, background)
        im.paste(rgba, mask=rgba.split()[3])
        if labels:
            d = ImageDraw.Draw(im)
            try:
                font = ImageFont.load_default(size=max(12, im.width // 22))
            except TypeError:
                font = ImageFont.load_default()
            text = str(labels[i])
            pad, box = max(4, im.width // 100), d.textbbox((0, 0), text, font=font)
            d.rectangle([0, 0, box[2] + 2 * pad, box[3] + 2 * pad], fill=(0, 0, 0))
            d.text((pad, pad), text, fill=(255, 255, 255), font=font)
        out.append(im.convert("P", palette=Image.ADAPTIVE, colors=255) if fmt == "gif" else im)
    buf = io.BytesIO()
    extra = {} if fmt == "gif" else {"quality": int(quality)}
    out[0].save(buf, format="GIF" if fmt == "gif" else "WEBP", save_all=True, append_images=out[1:],
                duration=int(1000 / max(fps, 0.1)), loop=0, **extra)
    return buf.getvalue()


def animation_html(anim, title=""):
    """Animation bytes -> a small HTML page that shows it (a UDF that returns a str renders as a page)."""
    import base64
    import html

    mime = "image/gif" if anim[:3] == b"GIF" else "image/webp"
    head = f"<p style='font:14px sans-serif;margin:8px 0'>{html.escape(title)}</p>" if title else ""
    return (f"<!doctype html><html><body style='margin:0;padding:12px;background:#111;color:#eee'>{head}"
            f"<img style='max-width:100%' src='data:{mime};base64,{base64.b64encode(anim).decode()}'></body></html>")


def _mercator_shape(bounds, size):
    from rasterio.warp import transform_bounds

    l, b, r, t = transform_bounds("EPSG:4326", "EPSG:3857", *bbox(bounds))
    w, h = r - l, t - b
    return (int(size), max(1, round(int(size) * w / h))) if h >= w else (max(1, round(int(size) * h / w)), int(size))
