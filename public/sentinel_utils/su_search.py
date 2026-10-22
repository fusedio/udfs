"""Constants and bands, areas, dates and scene search: the Sentinel-2 catalog layer that the other modules build on."""

SENTINEL2_L2A = "sentinel-2-l2a"
DEM = "cop-dem-glo-30"
AEF = "alphaearth-embeddings"

SCALE = 0.0001
MAX_PIXELS = 200_000_000

MOSAICKING = ("simple", "orbit")
MOSAIC_ORDERS = ("least_cloudy", "clearest_day", "newest", "oldest")
UNITS = ("reflectance", "dn")
RESAMPLING = ("nearest", "bilinear", "cubic")
OUTPUTS = ("dataset", "array", "geotiff", "png")

S2_BANDS = (
    {"name": "B01", "asset": "coastal", "alias": "coastal", "resolution": 60, "wavelength": 443},
    {"name": "B02", "asset": "blue", "alias": "blue", "resolution": 10, "wavelength": 490},
    {"name": "B03", "asset": "green", "alias": "green", "resolution": 10, "wavelength": 560},
    {"name": "B04", "asset": "red", "alias": "red", "resolution": 10, "wavelength": 665},
    {"name": "B05", "asset": "rededge1", "alias": "rededge1", "resolution": 20, "wavelength": 705},
    {"name": "B06", "asset": "rededge2", "alias": "rededge2", "resolution": 20, "wavelength": 740},
    {"name": "B07", "asset": "rededge3", "alias": "rededge3", "resolution": 20, "wavelength": 783},
    {"name": "B08", "asset": "nir", "alias": "nir", "resolution": 10, "wavelength": 842},
    {"name": "B8A", "asset": "nir08", "alias": "nir08", "resolution": 20, "wavelength": 865},
    {"name": "B09", "asset": "nir09", "alias": "nir09", "resolution": 60, "wavelength": 945},
    {"name": "B11", "asset": "swir16", "alias": "swir1", "resolution": 20, "wavelength": 1610},
    {"name": "B12", "asset": "swir22", "alias": "swir2", "resolution": 20, "wavelength": 2190},
    {"name": "SCL", "asset": "scl", "alias": "scl", "resolution": 20, "wavelength": None},
    {"name": "CLD", "asset": "cloud", "alias": "cloud", "resolution": 20, "wavelength": None},
    {"name": "SNW", "asset": "snow", "alias": "snow", "resolution": 20, "wavelength": None},
    {"name": "AOT", "asset": "aot", "alias": "aot", "resolution": 10, "wavelength": None},
    {"name": "WVP", "asset": "wvp", "alias": "wvp", "resolution": 10, "wavelength": None},
    {"name": "dataMask", "asset": None, "alias": "datamask", "resolution": None, "wavelength": None},
)

SCL_CLASSES = {
    0: "no_data", 1: "saturated", 2: "dark_area", 3: "cloud_shadow", 4: "vegetation", 5: "not_vegetated",
    6: "water", 7: "unclassified", 8: "cloud_medium", 9: "cloud_high", 10: "thin_cirrus", 11: "snow",
}

MASKS = {
    "clear": (4, 5, 6),
    "clear_snow": (4, 5, 6, 11),
    "land": (4, 5),
    "water": (6,),
    "snow": (11,),
    "cloud": (8, 9, 10),
    "shadow": (3,),
    "invalid": (0, 1),
    "no_cloud": (2, 3, 4, 5, 6, 7, 11),
}

DATA_COLLECTIONS = {
    SENTINEL2_L2A: {
        "title": "Sentinel-2 L2A surface reflectance",
        "resolution": 10,
        "bands": tuple(b["name"] for b in S2_BANDS),
        "temporal": ("2015-06-27", None),
        "provider": "ESA Copernicus, via source.coop and Element84 Earth Search",
        "url": "https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a",
    },
    DEM: {
        "title": "Copernicus DEM GLO-30",
        "resolution": 30,
        "bands": ("DEM",),
        "temporal": None,
        "provider": "ESA Copernicus, via Element84 Earth Search",
        "url": "https://earth-search.aws.element84.com/v1/collections/cop-dem-glo-30",
    },
    AEF: {
        "title": "AlphaEarth Foundations annual embeddings",
        "resolution": 10,
        "bands": tuple(f"A{i:02d}" for i in range(64)),
        "temporal": ("2017-01-01", "2025-12-31"),
        "provider": "Google DeepMind, via source.coop",
        "url": "https://data.source.coop/tge-labs/aef",
    },
}

_BAND_KEYS = {
    "datamask": "dataMask",
    **{key: b["name"] for b in S2_BANDS if b["asset"]
       for key in (b["name"].lower(), b["asset"], b["alias"], "b" + b["name"][1:].lstrip("0").lower())},
}


class NoDataError(ValueError):
    """No scene, or no valid pixel, for the request."""


def api_table(namespace):
    """Returns a DataFrame (name, signature, doc) of the public functions in a module namespace, without udf and api."""
    import inspect
    import pandas as pd

    rows = [{"name": n, "signature": f"{n}{inspect.signature(f)}", "doc": (inspect.getdoc(f) or "").split("\n")[0]}
            for n, f in namespace.items() if inspect.isfunction(f) and not n.startswith("_") and n not in ("udf", "api")]
    return pd.DataFrame(rows, columns=["name", "signature", "doc"])


def check_choice(name, value, allowed):
    """Returns `value` when it is in `allowed`, else raises ValueError that lists the allowed values."""
    if value not in allowed:
        raise ValueError(f"{name} must be one of {list(allowed)}, got {value!r}")
    return value


def get_available_collections():
    """Returns the names of the data collections."""
    return list(DATA_COLLECTIONS)


def collection_info(collection):
    """Returns the description dict of a data collection."""
    return {"name": collection, **DATA_COLLECTIONS[check_choice("collection", collection, DATA_COLLECTIONS)]}


def band_names(bands, collection=SENTINEL2_L2A):
    """Returns canonical band names (B04, B8A, SCL ...) for a list or comma string of band spellings."""
    check_choice("collection", collection, DATA_COLLECTIONS)
    if isinstance(bands, str):
        bands = [b for b in bands.split(",") if b.strip()]
    allowed = DATA_COLLECTIONS[collection]["bands"]
    out = []
    for band in bands:
        key = str(band).strip()
        if collection == SENTINEL2_L2A:
            key = _BAND_KEYS.get(key.lower(), key)
        else:
            key = next((b for b in allowed if b.lower() == key.lower()), key)
        check_choice("band", key, allowed)
        if key not in out:
            out.append(key)
    return out


def band_info(band):
    """Returns the dict of one Sentinel-2 band: name, asset, alias, resolution (m), wavelength (nm)."""
    name = band_names([band])[0]
    return dict(next(b for b in S2_BANDS if b["name"] == name))


_AREA_FORMS = "'w,s,e,n', (w, s, e, n), WKT, a shapely geometry, a GeoDataFrame/GeoSeries or a GeoJSON dict"


def to_bbox(area):
    """Returns (w, s, e, n) in EPSG:4326 degrees."""
    numbers = _numbers(area)
    if numbers is None:
        numbers = to_geometry(area).bounds
    return _check_bbox(numbers)


def to_geometry(area):
    """Returns a shapely geometry in EPSG:4326."""
    import shapely
    import shapely.geometry

    numbers = _numbers(area)
    if numbers is not None:
        return shapely.box(*_check_bbox(numbers))
    if hasattr(area, "to_crs"):
        geom = (area.to_crs(4326) if area.crs else area).union_all()
    elif hasattr(area, "geom_type"):
        geom = area
    elif isinstance(area, dict):
        geom = shapely.union_all([shapely.geometry.shape(g) for g in _geojson_geometries(area)])
    elif isinstance(area, str):
        try:
            geom = shapely.from_wkt(area)
        except shapely.errors.GEOSException:
            raise ValueError(f"area must be {_AREA_FORMS}, got {area!r}") from None
    else:
        raise ValueError(f"area must be {_AREA_FORMS}, got {type(area).__name__}")
    if geom.is_empty:
        raise ValueError("area is an empty geometry")
    _check_bbox(geom.bounds)
    return geom


def read_geojson(path_or_url):
    """Returns the GeoJSON dict read from a local file or an http(s) URL."""
    import json

    if str(path_or_url).startswith(("http://", "https://")):
        import requests

        response = requests.get(path_or_url, timeout=60)
        response.raise_for_status()
        return response.json()
    with open(path_or_url) as f:
        return json.load(f)


def geojson_to_wkt(geojson):
    """Returns the WKT string of the union of the GeoJSON geometries."""
    import shapely

    return shapely.to_wkt(to_geometry(geojson), rounding_precision=7)


def get_utm_crs(lon, lat):
    """Returns the UTM CRS ("EPSG:326zz" north, "EPSG:327zz" south) of a point."""
    zone = int((float(lon) + 180) // 6) % 60 + 1
    return f"EPSG:{(32600 if lat >= 0 else 32700) + zone}"


def transform_bbox(bbox, src_crs, dst_crs):
    """Returns the (xmin, ymin, xmax, ymax) envelope of `bbox` reprojected from src_crs to dst_crs."""
    from pyproj import Transformer

    if str(src_crs) == str(dst_crs):
        return tuple(float(v) for v in bbox)
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    return tuple(float(v) for v in transformer.transform_bounds(*bbox, densify_pts=21))


def to_utm_bbox(area):
    """Returns ((xmin, ymin, xmax, ymax) in metres, crs) in the UTM zone of the area centre."""
    w, s, e, n = to_bbox(area)
    crs = get_utm_crs((w + e) / 2, (s + n) / 2)
    return transform_bbox((w, s, e, n), "EPSG:4326", crs), crs


def bbox_to_dimensions(area, resolution):
    """Returns (width, height) in pixels of the area at `resolution` metres, in the UTM zone of its centre."""
    (x0, y0, x1, y1), _ = to_utm_bbox(area)
    rx, ry = _pair(resolution, "resolution")
    return round((x1 - x0) / rx), round((y1 - y0) / ry)


def bbox_to_resolution(area, width, height):
    """Returns (res_x, res_y) in metres of the area drawn at width x height pixels."""
    (x0, y0, x1, y1), _ = to_utm_bbox(area)
    return (x1 - x0) / int(width), (y1 - y0) / int(height)


def area_km2(area):
    """Returns the geodesic area in square kilometres."""
    from pyproj import Geod

    return abs(Geod(ellps="WGS84").geometry_area_perimeter(to_geometry(area))[0]) / 1e6


def make_geobox(area, *, resolution=None, size=None, crs=None):
    """Returns a north-up odc.geo GeoBox over the area (size keeps the exact bounds); crs defaults to UTM of the centre."""
    import math
    from odc.geo import CRS
    from odc.geo.geobox import GeoBox

    if resolution is not None and size is not None:
        raise ValueError("give resolution or size, not both")
    w, s, e, n = to_bbox(area)
    crs = CRS(get_utm_crs((w + e) / 2, (s + n) / 2) if crs in (None, "utm") else crs)
    bounds = transform_bbox((w, s, e, n), "EPSG:4326", str(crs))
    if size is not None:
        width, height = (int(v) for v in _pair(size, "size"))
        return GeoBox.from_bbox(bounds, crs=crs, shape=(height, width), tight=True)
    if crs.geographic or crs.units[0] != "metre":
        raise ValueError(f"resolution is in metres but {crs} uses {crs.units[0]}; give size or a metric crs")
    step = float(resolution or 10)
    if crs.epsg == 3857:
        step /= math.cos(math.radians((s + n) / 2))
    return GeoBox.from_bbox(bounds, crs=crs, resolution=step)


def _numbers(area):
    if isinstance(area, str):
        if "(" in area:
            return None
        area = area.replace("[", "").replace("]", "").split(",")
    elif not isinstance(area, (list, tuple)) and type(area).__name__ != "ndarray":
        return None
    try:
        values = [float(v) for v in area]
    except (TypeError, ValueError):
        values = []
    if len(values) != 4:
        raise ValueError(f"area must be {_AREA_FORMS}, got {area!r}")
    return values


def _check_bbox(values):
    w, s, e, n = (float(v) for v in values)
    if not (-180 <= w <= e <= 180 and -90 <= s <= n <= 90):
        raise ValueError(f"area must be in EPSG:4326 degrees with west <= east and south <= north "
                         f"(split an area that crosses 180), got {[w, s, e, n]}")
    return w, s, e, n


def _geojson_geometries(obj):
    kind = obj.get("type")
    if kind == "FeatureCollection":
        return [g for f in obj["features"] for g in _geojson_geometries(f)]
    if kind == "Feature":
        return [obj["geometry"]] if obj.get("geometry") else []
    if kind in ("Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection"):
        return [obj]
    raise ValueError(f"GeoJSON type must be FeatureCollection, Feature or a geometry type, got {kind!r}")


def _pair(value, name):
    import numbers

    if isinstance(value, numbers.Real) and value > 0:
        return float(value), float(value)
    if isinstance(value, (list, tuple)) and len(value) == 2 and all(v > 0 for v in value):
        return float(value[0]), float(value[1])
    raise ValueError(f"{name} must be a positive number or a (x, y) pair of positive numbers, got {value!r}")


import re


_NOW = re.compile(r"^NOW(?:([+-])(\d+)(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?)?$")
_DAY = re.compile(r"^\d{4}-?\d{2}-?\d{2}$")
_FORMS = "'YYYY-MM-DD', 'YYYYMMDD', an ISO time, 'NOW', 'NOW-30DAYS', 'NOW+2HOURS', a date or a datetime"
_FREQS = {"day": "D", "week": "W-MON", "month": "MS", "quarter": "QS", "season": "QS-DEC", "year": "YS"}


def parse_time(value):
    """Returns a UTC pandas Timestamp for a date, a datetime, an ISO string, 'YYYYMMDD' or 'NOW-30DAYS'."""
    import pandas as pd

    if isinstance(value, str):
        text = value.strip().upper()
        match = _NOW.match(text)
        if match:
            now = pd.Timestamp.now(tz="UTC")
            sign, number, unit = match.groups()
            if not sign:
                return now
            offset = pd.DateOffset(**{unit.lower() + "s": int(number)})
            return now + offset if sign == "+" else now - offset
        if _DAY.match(text):
            text = text.replace("-", "")
            text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
        value = text
    if value is None or isinstance(value, bool):
        raise ValueError(f"time must be {_FORMS}, got {value!r}")
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        raise ValueError(f"time must be {_FORMS}, got {value!r}") from None
    if pd.isna(ts):
        raise ValueError(f"time must be {_FORMS}, got {value!r}")
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def parse_time_interval(date):
    """Returns (start, end) UTC Timestamps; a day covers 00:00 to 23:59:59.999, None means the last 30 days."""
    import pandas as pd

    if date is None:
        today = pd.Timestamp.now(tz="UTC").normalize()
        return today - pd.Timedelta(days=30), _day_end(today)
    if isinstance(date, (tuple, list)):
        if len(date) != 2:
            raise ValueError(f"date must be one time or a (start, end) pair, got {date!r}")
        start, end = parse_time(date[0]), parse_time(date[1])
        if _is_day(date[1]):
            end = _day_end(end)
    else:
        start = parse_time(date)
        end = _day_end(start) if _is_day(date) else start
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    return start, end


def serialize_time(value):
    """Returns 'YYYY-MM-DD' for a day, an ISO 8601 UTC string ('...Z') for a time; a tuple gives a tuple."""
    if isinstance(value, (tuple, list)):
        return tuple(serialize_time(v) for v in value)
    ts = parse_time(value)
    if _is_day(value):
        return ts.strftime("%Y-%m-%d")
    return ts.isoformat().replace("+00:00", "Z")


def filter_times(timestamps, time_difference):
    """Returns the sorted UTC Timestamps, keeping the first of each group closer than time_difference."""
    import pandas as pd

    gap = pd.Timedelta(time_difference)
    kept = []
    for ts in sorted({parse_time(t) for t in timestamps}):
        if not kept or ts - kept[-1] > gap:
            kept.append(ts)
    return kept


def split_time_interval(date, *, n=None, freq=None):
    """Returns [(start, end)] as 'YYYY-MM-DD' strings: n equal parts, or calendar periods of freq."""
    import pandas as pd

    if (n is None) == (freq is None):
        raise ValueError("give exactly one of n or freq")
    start, end = parse_time_interval(date)
    d0, d1 = start.tz_localize(None).normalize(), end.tz_localize(None).normalize()
    days = (d1 - d0).days + 1
    if n is not None:
        if not 1 <= int(n) <= days:
            raise ValueError(f"n must be between 1 and {days} (the number of days), got {n!r}")
        edges = [d0 + pd.Timedelta(days=round(i * days / int(n))) for i in range(int(n) + 1)]
    else:
        try:
            marks = pd.date_range(d0, d1, freq=_FREQS.get(freq, freq))
        except (TypeError, ValueError):
            raise ValueError(f"freq must be one of {list(_FREQS)} or a pandas frequency like '10D', got {freq!r}") from None
        edges = sorted({d0, *marks, d1 + pd.Timedelta(days=1)})
    return [(f"{a:%Y-%m-%d}", f"{b - pd.Timedelta(days=1):%Y-%m-%d}") for a, b in zip(edges[:-1], edges[1:])]


def _is_day(value):
    import datetime as dt

    if isinstance(value, str):
        return bool(_DAY.match(value.strip()))
    return isinstance(value, dt.date) and not isinstance(value, dt.datetime)


def _day_end(ts):
    import pandas as pd

    return ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)


E84_API = "https://earth-search.aws.element84.com/v1"
COOP = "https://data.source.coop/tge-labs/s2-stac-geoparquet"
SOURCES = ("auto", "earth-search", "coop", "legacy")
SOURCE_RANK = {"earth-search": 0, "coop": 0, "legacy": 1}
COLLECTIONS = (SENTINEL2_L2A, DEM)
STAC_PAGE = 200
COLUMNS = {"id": "object", "datetime": "datetime64[ns, UTC]", "date": "object", "tile_id": "object",
           "platform": "object", "cloud_cover": "float64", "nodata_pct": "float64", "crs": "object",
           "source": "object", "offset": "float64", "assets": "object"}
_SQUARE_COLUMNS = "ABCDEFGHJKLMNPQRSTUVWXYZ"
_SQUARE_ROWS = "ABCDEFGHJKLMNPQRSTUV"
_LATITUDE_BANDS = "CDEFGHJKLMNPQRSTUVWX"
_HELD = {}


@fused.udf
def udf(area: str = "-118.62,34.02,-118.50,34.10", date: str = "2024-06-01/2024-06-30",
        cloud_cover: float = 30):
    """Sentinel-2 L2A scenes for the area and dates, without the assets column."""
    return query(area, date, cloud_cover=cloud_cover).drop(columns="assets").reset_index(drop=True)


def query(area, date=None, *, collection=SENTINEL2_L2A, cloud_cover=100, tile_id=None, platform=None,
          limit=None, source="auto"):
    """GeoDataFrame of scenes that touch area in time order, one row per scene, index = scene id."""
    import warnings

    collection = check_choice("collection", collection, COLLECTIONS)
    source = check_choice("source", source, SOURCES)
    bounds = tuple(round(float(v), 6) for v in to_bbox(area))
    shape = to_geometry(area)
    if collection == DEM:
        check_choice("source for DEM", source, ("auto", "earth-search"))
        out = _finish([_search_dem(bounds)], None, None, shape)
    else:
        cloud = _cloud_range(cloud_cover)
        out = _finish(*_search(bounds, date, cloud, source), shape)
        out = out[out["cloud_cover"].between(*cloud)]
    if tile_id is not None:
        tiles = [tile_id] if isinstance(tile_id, str) else list(tile_id)
        out = out[out["tile_id"].isin([str(t).upper().removeprefix("T") for t in tiles])]
    if platform is not None:
        out = out[out["platform"].str.startswith(str(platform).upper().replace("SENTINEL-", "S"))]
    out = out.sort_values(["datetime", "id"])
    if limit is not None and len(out) > int(limit):
        warnings.warn(f"query found {len(out)} scenes; keeping the first {int(limit)}")
        out = out.head(int(limit))
    return out


def count(area, date=None, **filters):
    """Number of scenes that query(area, date, **filters) returns."""
    return len(query(area, date, **filters))


def get_product(product_id):
    """The STAC item (dict) of one scene id from Earth Search."""
    import re
    import requests

    if product_id.startswith("Copernicus_DSM"):
        collection = DEM
    elif re.fullmatch(r"S2[A-D]_\d{1,2}[A-Z]{3}_\d{8}_\d+_L2A", product_id):
        collection = "sentinel-2-l2a"
    else:
        collection = "sentinel-2-c1-l2a"
    r = requests.get(f"{E84_API}/collections/{collection}/items/{product_id}", timeout=60)
    if r.status_code == 404:
        raise NoDataError(f"no product {product_id!r} in Earth Search collection {collection}")
    r.raise_for_status()
    return r.json()


def get_timestamps(products):
    """Sorted unique acquisition times (UTC pandas Timestamps) of the products."""
    return sorted(set(products["datetime"]))


def to_geojson(products):
    """GeoJSON FeatureCollection dict of the products (assets included)."""
    import json

    out = products.assign(datetime=products["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return json.loads(out.reset_index(drop=True).to_json(na="null", drop_id=True))


def to_dataframe(products):
    """pandas DataFrame of the products without geometry and assets."""
    import pandas as pd

    return pd.DataFrame(products.drop(columns=["geometry", "assets"], errors="ignore"))


def get_collections():
    """DataFrame of the data collections: id, title, temporal, provider."""
    import pandas as pd

    rows = [{"id": k, **{c: v.get(c) for c in ("title", "temporal", "provider")}}
            for k, v in DATA_COLLECTIONS.items()]
    return pd.DataFrame(rows, columns=["id", "title", "temporal", "provider"])


def rank(products, area=None, *, date=None, per_tile=None, order="clear"):
    """Products best first with clear_pct, days_off and tile_rank; each tile's best first (order "day": day by day)."""
    import pandas as pd

    order = check_choice("order", order, ("clear", "date", "day"))
    sc = products
    if area is not None and len(sc):
        sc = sc[sc.intersects(to_geometry(area))]
    sc = sc.copy()
    if not len(sc):
        return sc.assign(clear_pct=[], days_off=[], tile_rank=[], **({"day_clear_pct": []} if order == "day" else {}))
    clear = (100 - sc["nodata_pct"].fillna(0)) * (100 - sc["cloud_cover"].fillna(100)) / 100
    sc["clear_pct"] = clear.round(2)
    day = pd.Timestamp(parse_time(date).strftime("%Y-%m-%d")) if date is not None else None
    sc["days_off"] = (pd.to_datetime(sc["date"]) - day).abs().dt.days if day is not None else 0
    sc["_unclear"] = -clear
    keys = {"clear": ["_unclear", "days_off"], "date": ["days_off", "_unclear"],
            "day": ["_day", "cloud_cover", "days_off"]}[order]
    if order == "day":
        days = clearest_days(sc, area if area is not None else sc)
        sc["day_clear_pct"] = sc["date"].map(dict(zip(days["date"], days["clear_pct"])))
        sc["_day"] = sc["date"].map({d: i for i, d in enumerate(days["date"])})
    sc = sc.sort_values(keys + ["datetime", "id"], kind="stable")
    if per_tile:
        sc = sc.groupby("tile_id").head(int(per_tile))
    sc["tile_rank"] = sc.groupby("tile_id").cumcount()
    if order == "day":
        sc = sc.assign(_dup=sc.groupby(["date", "tile_id"]).cumcount()).sort_values(["_day", "_dup"], kind="stable")
    else:
        sc = sc.sort_values("tile_rank", kind="stable")
    return sc.drop(columns=["_unclear", "_day", "_dup"], errors="ignore")


def clearest_days(products, area):
    """DataFrame of the days best first: date, scenes, cover_pct of the area, cloud_cover by area, clear_pct."""
    import geopandas as gpd
    import pandas as pd
    import shapely

    columns = ["date", "scenes", "cover_pct", "cloud_cover", "clear_pct"]
    shape = to_geometry(area)
    sc = products[products.intersects(shape)] if len(products) else products
    if not len(sc):
        return pd.DataFrame(columns=columns)
    crs = to_utm_bbox(shape)[1]
    aoi = gpd.GeoSeries([shape], crs="EPSG:4326").to_crs(crs).iloc[0]
    aoi = aoi if aoi.area > 0 else aoi.buffer(1.0)
    parts = (sc.geometry if sc.crs else sc.geometry.set_crs("EPSG:4326")).to_crs(crs).intersection(aoi).values
    clouds = sc["cloud_cover"].fillna(100).values
    rows = []
    for day, idx in sc.groupby("date").indices.items():
        covered, cloudy = shapely.Polygon(), 0.0
        for k in sorted(idx, key=lambda k: clouds[k]):
            cloudy += parts[k].difference(covered).area * clouds[k]
            covered = covered.union(parts[k])
        cloud = cloudy / covered.area if covered.area > 0 else 100.0
        cover = min(100.0, covered.area / aoi.area * 100)
        rows.append({"date": day, "scenes": len(idx), "cover_pct": round(cover, 2), "cloud_cover": round(cloud, 2),
                     "clear_pct": round(cover * (100 - cloud) / 100, 2)})
    out = pd.DataFrame(rows, columns=columns)
    return out.sort_values(["clear_pct", "date"], ascending=[False, True], kind="stable").reset_index(drop=True)


def _cloud_range(value):
    lo, hi = (0, value) if isinstance(value, (int, float)) else value
    lo, hi = float(lo), float(hi)
    if not 0 <= lo <= hi <= 100:
        raise ValueError(f"cloud_cover must be a max or (min, max) with 0 <= min <= max <= 100, got {value!r}")
    return lo, hi


def _window(date):
    import pandas as pd

    if isinstance(date, str) and "/" in date:
        date = tuple(date.split("/"))
    start, end = (t.strftime("%Y-%m-%d") for t in parse_time_interval(date))
    if end < start:
        raise ValueError(f"date end {end} is before start {start}")
    lo = (pd.Timestamp(start) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    hi = (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    return start, end, lo, hi


def _search(bounds, date, cloud, source):
    import concurrent.futures

    start, end, lo, hi = _window(date)
    key = (bounds, lo, hi, *cloud)
    if source != "auto":
        reader = {"earth-search": _search_c1, "coop": _search_coop, "legacy": _search_legacy}[source]
        return [reader(*key)], start, end
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        c1 = pool.submit(_try, _search_c1, _search_coop, (bounds, lo, hi, 0.0, 100.0))
        legacy = pool.submit(_try, _search_legacy, _search_coop_legacy, key)
        return [c1.result(), legacy.result()], start, end


def _try(first, fallback, key):
    import warnings

    try:
        return first(*key)
    except Exception as e:
        warnings.warn(f"{first.__name__} failed ({str(e)[:80]}); using {fallback.__name__}")
        return fallback(*key)


def _finish(parts, start, end, shape):
    import json
    import geopandas as gpd
    import pandas as pd
    import shapely

    parts = [p for p in parts if p is not None and len(p)]
    if not parts:
        return _empty()
    df = pd.concat(parts, ignore_index=True)
    df = df[shapely.intersects(df["geometry"].values, shape)]
    t = pd.to_datetime(df["datetime"], utc=True, format="ISO8601")
    lon = shapely.get_x(shapely.centroid(df["geometry"].values)) if start else 0
    df = df.assign(datetime=t, date=(t + pd.to_timedelta(lon / 15, unit="h")).dt.strftime("%Y-%m-%d"))
    if start:
        df = df[(df["date"] >= start) & (df["date"] <= end)]
    df = df.assign(_r=df["source"].map(SOURCE_RANK)).sort_values("_r", kind="stable").drop_duplicates("id")
    acquisition = df["tile_id"] + "_" + df["date"] + "_" + df["platform"]
    df = df[df["_r"] == df.groupby(acquisition)["_r"].transform("min")]
    if not len(df):
        return _empty()
    df = df.assign(assets=df["assets"].map(lambda s: {k: v for k, v in json.loads(s).items() if v}))
    out = gpd.GeoDataFrame(df[list(COLUMNS) + ["geometry"]].astype(COLUMNS), geometry="geometry", crs="EPSG:4326")
    out.index = out["id"].values
    return out


def _empty():
    import geopandas as gpd
    import pandas as pd

    cols = {c: pd.Series([], dtype=t) for c, t in COLUMNS.items()}
    return gpd.GeoDataFrame(cols, geometry=gpd.GeoSeries([], crs="EPSG:4326"))


def _search_stac(bounds, start, end, cloud, collection, source, offset, max_pages=50):
    import pandas as pd
    import requests

    body = {"collections": [collection], "bbox": list(bounds), "limit": STAC_PAGE}
    if start:
        body["datetime"] = f"{start}T00:00:00Z/{end}T23:59:59Z"
    if cloud:
        body["query"] = {"eo:cloud_cover": {"gte": cloud[0], "lte": cloud[1]}}
    rows, url = [], f"{E84_API}/search"
    for _ in range(max_pages):
        r = requests.post(url, json=body, timeout=60)
        r.raise_for_status()
        js = r.json()
        rows += [_stac_row(f, source, offset) for f in js["features"]]
        nxt = [link for link in js.get("links", []) if link.get("rel") == "next"]
        if not nxt:
            return pd.DataFrame(rows)
        url, body = nxt[0]["href"], nxt[0].get("body", body)
    raise RuntimeError(f"more than {max_pages * STAC_PAGE} scenes: narrow the area or the dates")


def _stac_row(f, source, offset):
    import json
    import shapely

    p = f["properties"]
    tile = (f"{p['mgrs:utm_zone']:02d}{p['mgrs:latitude_band']}{p['mgrs:grid_square']}" if "mgrs:utm_zone" in p
            else p.get("grid:code", f["id"]).split("-")[-1])
    keys = tuple(f["assets"]) if source == "dem" else _asset_keys()
    return {"id": f["id"], "datetime": p["datetime"], "tile_id": tile,
            "platform": p.get("platform", "").replace("sentinel-", "S").upper(), "cloud_cover": p.get("eo:cloud_cover"),
            "nodata_pct": p.get("s2:nodata_pixel_percentage"), "crs": f"EPSG:{p.get('proj:epsg')}",
            "source": "earth-search" if source == "dem" else source, "offset": offset,
            "assets": json.dumps({k: f["assets"].get(k, {}).get("href") for k in keys}),
            "geometry": shapely.geometry.shape(f["geometry"])}


def _asset_keys():
    return tuple(b["asset"] for b in S2_BANDS if b.get("asset"))


@fused.cache(cache_max_age="30d", cache_verbose=False)
def _search_dem(bounds):
    return _search_stac(bounds, None, None, None, DEM, "dem", 0.0)


@fused.cache(cache_max_age="24h", cache_verbose=False)
def _search_c1(bounds, start, end, cmin, cmax):
    return _search_stac(bounds, start, end, (cmin, cmax), "sentinel-2-c1-l2a", "earth-search", -0.1)


@fused.cache(cache_max_age="24h", cache_verbose=False)
def _search_legacy(bounds, start, end, cmin, cmax):
    return _search_stac(bounds, start, end, (cmin, cmax), "sentinel-2-l2a", "legacy", 0.0)


@fused.cache(cache_max_age="24h", cache_verbose=False)
def _search_coop(bounds, start, end, cmin, cmax):
    return _coop("sentinel-2-c1-l2a", "_tile", "coop", -0.1, bounds, start, end, cmin, cmax)


@fused.cache(cache_max_age="24h", cache_verbose=False)
def _search_coop_legacy(bounds, start, end, cmin, cmax):
    return _coop("sentinel-2-l2a", '"s2:mgrs_tile"', "legacy", 0.0, bounds, start, end, cmin, cmax)


def _coop(collection, tile_column, source, offset, bounds, start, end, cmin, cmax):
    import pandas as pd
    import shapely

    tiles = _tile_candidates(bounds)
    files = _coop_files(f"{COOP}/{collection}", start, end, {int(t[:2]) for t in tiles})
    if not files:
        return pd.DataFrame()
    assets = ", ".join(f"'{a}', json_extract_string(assets, '$.{a}.href')" for a in _asset_keys())
    tile_list = ", ".join(f"'{t}'" for t in sorted(tiles))
    df = _held("duckdb", _connect).cursor().execute(f"""
        SELECT id, strftime(datetime, '%Y-%m-%dT%H:%M:%S.%fZ') AS datetime, {tile_column} AS tile_id,
               upper(replace(platform, 'sentinel-', 'S')) AS platform, "eo:cloud_cover" AS cloud_cover,
               "s2:nodata_pixel_percentage" AS nodata_pct, 'EPSG:' || CAST("proj:epsg" AS VARCHAR) AS crs,
               '{source}' AS source, {offset} AS offset, json_object({assets})::VARCHAR AS assets, geometry
        FROM read_parquet({files}, union_by_name = true)
        WHERE {tile_column} IN ({tile_list})
          AND datetime >= '{start}' AND datetime <= '{end} 23:59:59.999'
          AND "eo:cloud_cover" BETWEEN {cmin} AND {cmax}
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY "s2:generation_time" DESC NULLS LAST) = 1
    """).df()
    df["geometry"] = shapely.from_wkb([bytes(g) for g in df["geometry"]])
    return df[shapely.intersects(df["geometry"].values, shapely.box(*bounds))]


def _coop_files(base, start, end, zones):
    import concurrent.futures
    import re

    def year_files(year):
        assets = _held((base, year), lambda: _year_assets(base, year))
        zone_parts = any(a["href"].startswith("./z") for a in assets)
        keep = []
        for a in assets:
            zone = re.match(r"\./z(\d+)-(\d+)\.", a["href"])
            if zone and not any(int(zone[1]) <= z <= int(zone[2]) for z in zones):
                continue
            if zone_parts and a["href"] == "./items.parquet":
                continue
            if a.get("start_datetime", start) <= f"{end}T23:59:59.999Z" and a.get("end_datetime", end) >= start:
                keep.append(f"{base}/year={year}/{a['href'][2:]}")
        return keep

    years = range(max(int(start[:4]), 2015), int(end[:4]) + 1)
    with concurrent.futures.ThreadPoolExecutor(len(years) or 1) as pool:
        return [f for files in pool.map(year_files, years) for f in files]


def _year_assets(base, year):
    import requests

    r = requests.get(f"{base}/year={year}/{year}.json", timeout=30)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return [a for a in r.json()["assets"].values() if "data" in a.get("roles", ()) and a.get("table:row_count")]


def _connect():
    import duckdb

    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs; SET TimeZone = 'UTC'; SET enable_http_metadata_cache = true; "
            "SET parquet_metadata_cache = true;")
    return con


def _held(key, make, max_age=3600):
    import time as clock

    value, born = _HELD.get(key, (None, 0.0))
    if value is None or clock.time() - born > max_age:
        value, born = make(), clock.time()
        _HELD[key] = (value, born)
    return value


def _tile_candidates(bounds):
    """Set of MGRS tile ids whose Sentinel-2 tile can touch bounds (a superset: false ids do no harm)."""
    import math
    import pyproj

    w, s, e, n = bounds
    lo, hi = (min(max(int((lat + 80) // 8), 0), 19) for lat in (s - 1, n + 1))
    bands = _LATITUDE_BANDS[lo:hi + 1]
    out = set()
    for zone in {(z - 1) % 60 + 1 for z in range(int((w + 180) // 6), int((e + 180) // 6) + 3)}:
        hemispheres = ([32600 + zone] if n + 1 >= 0 else []) + ([32700 + zone] if s - 1 < 0 else [])
        for epsg in hemispheres:
            x0, y0, x1, y1 = pyproj.Transformer.from_crs(4326, epsg, always_xy=True).transform_bounds(
                w, s, e, n, densify_pts=21)
            if not all(map(math.isfinite, (x0, y0, x1, y1))):
                continue
            for col in range(max(int((x0 - 1e4) // 1e5), 1), min(int((x1 + 1e4) // 1e5), 8) + 1):
                for row in range(int((y0 - 1e4) // 1e5), int((y1 + 1e4) // 1e5) + 1):
                    square = _SQUARE_COLUMNS[(zone - 1) % 3 * 8 + col - 1] + _SQUARE_ROWS[(row + 5 * (zone % 2 == 0)) % 20]
                    out.update(f"{zone:02d}{band}{square}" for band in bands)
    return out


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return api_table(globals())
