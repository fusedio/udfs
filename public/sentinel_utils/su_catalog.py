try:
    import su_constants as constants
    import su_geometry as geometry
    import su_time as time
except ImportError:
    constants = fused.load("su_constants")
    geometry = fused.load("su_geometry")
    time = fused.load("su_time")

E84_API = "https://earth-search.aws.element84.com/v1"
COOP = "https://data.source.coop/tge-labs/s2-stac-geoparquet"
SOURCES = ("auto", "earth-search", "coop", "legacy")
SOURCE_RANK = {"earth-search": 0, "coop": 0, "legacy": 1}
COLLECTIONS = (constants.SENTINEL2_L2A, constants.DEM)
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


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def query(area, date=None, *, collection=constants.SENTINEL2_L2A, cloud_cover=100, tile_id=None, platform=None,
          limit=None, source="auto"):
    """GeoDataFrame of scenes that touch area in time order, one row per scene, index = scene id."""
    import warnings

    collection = constants.check_choice("collection", collection, COLLECTIONS)
    source = constants.check_choice("source", source, SOURCES)
    bounds = tuple(round(float(v), 6) for v in geometry.to_bbox(area))
    shape = geometry.to_geometry(area)
    if collection == constants.DEM:
        constants.check_choice("source for DEM", source, ("auto", "earth-search"))
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
        collection = constants.DEM
    elif re.fullmatch(r"S2[A-D]_\d{1,2}[A-Z]{3}_\d{8}_\d+_L2A", product_id):
        collection = "sentinel-2-l2a"
    else:
        collection = "sentinel-2-c1-l2a"
    r = requests.get(f"{E84_API}/collections/{collection}/items/{product_id}", timeout=60)
    if r.status_code == 404:
        raise constants.NoDataError(f"no product {product_id!r} in Earth Search collection {collection}")
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
            for k, v in constants.DATA_COLLECTIONS.items()]
    return pd.DataFrame(rows, columns=["id", "title", "temporal", "provider"])


def rank(products, area=None, *, date=None, per_tile=None, order="clear"):
    """Products best first with clear_pct, days_off and tile_rank; each tile's best first (order "day": day by day)."""
    import pandas as pd

    order = constants.check_choice("order", order, ("clear", "date", "day"))
    sc = products
    if area is not None and len(sc):
        sc = sc[sc.intersects(geometry.to_geometry(area))]
    sc = sc.copy()
    if not len(sc):
        return sc.assign(clear_pct=[], days_off=[], tile_rank=[], **({"day_clear_pct": []} if order == "day" else {}))
    clear = (100 - sc["nodata_pct"].fillna(0)) * (100 - sc["cloud_cover"].fillna(100)) / 100
    sc["clear_pct"] = clear.round(2)
    day = pd.Timestamp(time.parse_time(date).strftime("%Y-%m-%d")) if date is not None else None
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
    shape = geometry.to_geometry(area)
    sc = products[products.intersects(shape)] if len(products) else products
    if not len(sc):
        return pd.DataFrame(columns=columns)
    crs = geometry.to_utm_bbox(shape)[1]
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
    start, end = (t.strftime("%Y-%m-%d") for t in time.parse_time_interval(date))
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
    return tuple(b["asset"] for b in constants.S2_BANDS if b.get("asset"))


@fused.cache(cache_max_age="30d", cache_verbose=False)
def _search_dem(bounds):
    return _search_stac(bounds, None, None, None, constants.DEM, "dem", 0.0)


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
