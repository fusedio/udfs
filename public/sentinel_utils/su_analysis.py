"""Statistical API analog: statistics of bands or an index over an area, one row per time interval and output.

Use `statistical_request(...)` to make a JSON-safe request dict and `run_statistical(request)` to get the table.
The data is read one time for the full date range (orbit stack, one layer per solar day), then cut into intervals.
In each interval the clear pixels of the days are mosaicked: the day with the most valid pixels in the area comes
first (least cloudy for the area), and each pixel takes its first valid value. With "P1D" each acquisition day is
one interval, so there is no mosaic. Pixels outside the area polygon are not counted. When one scene covers the full
area, the other scenes of that day (tile overlap) are not read. An index uses band values clipped at 0
(su_read.compute_index), so NDVI stays in [-1, 1] where the offset makes dark pixels negative.

Earth Engine style collections (earthengine-api ImageCollection, in snake_case): lazy and chainable.

    s2 = collection().filter_bounds(area).filter_date("2024-06-01", "2024-08-31")
    ndvi = s2.filter_metadata("cloud_cover", "less_than", 20).median().normalized_difference(["B08", "B04"])
    ds = ndvi.get_data()
The last line gives the same Dataset as get_data(process_request(area, ("2024-06-01", "2024-08-31"), cloud_cover=20,
reducer="median", index="NDVI")). Each method returns a new object and reads no data. A chain compiles to the JSON
request of su_read (`to_request()`). Only get_info, size, get_data, save_data, reduce_region and reduce_regions read
data. Lists of images and of regions run through su_analysis.download.
filter_date includes the end day (as su_search), not the exclusive end of Earth Engine. cloud_cover limits include the
limit value, so "less_than" and "not_greater_than" give the same limit.
Rule: reduce_region / reduce_regions give the statistics of the composite image itself (its pixels inside the region),
as Earth Engine does; the clearest-day series of the Statistical API is su_analysis.statistical_request.

Download client (sentinelhub-py download/client.py): runs requests in this process, or in parallel on Fused workers.

Through `.map` an xarray Dataset or raw bytes come back as a GeoTIFF, and numpy fails. So the worker sends a
one-row DataFrame (kind, payload bytes, meta JSON) made by `to_payload`, and `download` rebuilds the result with
`from_payload`: the same object that `get_data` gives.
The worker is a UDF made here from a small entry and the source of each module it needs (as headers), so a worker
loads no module by name. This works in the canvas, from a package folder or a GitHub URL, and inside a remote UDF."""


try:
    import su_read
    import su_search
except ImportError:
    su_read = fused.load("su_read")
    su_search = fused.load("su_search")


constants = su_search
geometry = su_search
time = su_search
catalog = su_search
reader = su_read
indices = su_read
process = su_read


import re


_DURATION = re.compile(r"^P(\d+)([DWMY])$")
_DURATION_FORMS = "an ISO 8601 duration 'P<n>D', 'P<n>W', 'P<n>M' or 'P<n>Y' (for example 'P1D', 'P5D', 'P1M', 'P1Y')"
_FIELDS = ("area", "date", "bands", "index", "collection", "aggregation_interval", "resolution", "cloud_cover", "mask",
           "percentiles", "histogram")
_COLUMNS = ["interval_from", "interval_to", "output", "mean", "min", "max", "std", "sample_count", "no_data_count"]


def statistical_request(area, date, bands=None, *, index=None, collection=constants.SENTINEL2_L2A,
                        aggregation_interval="P1D", resolution=10, cloud_cover=100, mask="clear", percentiles=None,
                        histogram=None):
    """Returns a JSON-safe statistical request dict ("type": "statistical"); a bad value raises ValueError."""
    import json
    import numbers
    import shapely

    constants.check_choice("collection", collection, (constants.SENTINEL2_L2A,))
    names = [] if bands is None else [b for b in constants.band_names(bands, collection) if b != "dataMask"]
    if index is not None:
        index = constants.check_choice("index", str(index).upper(), indices.INDICES)
    if not names and index is None:
        raise ValueError("give bands (for example ['B04', 'B08']) or index (one of "
                         f"{list(indices.INDICES)}), or both")
    if mask is not None:
        constants.check_choice("mask", mask, constants.MASKS)
    _duration(aggregation_interval)
    if not isinstance(resolution, numbers.Real) or isinstance(resolution, bool) or resolution <= 0:
        raise ValueError(f"resolution must be a positive number of metres, got {resolution!r}")
    if not isinstance(cloud_cover, numbers.Real) or isinstance(cloud_cover, bool) or not 0 <= cloud_cover <= 100:
        raise ValueError(f"cloud_cover must be a number from 0 to 100 (max scene cloud percent), got {cloud_cover!r}")
    return {
        "type": "statistical",
        "area": json.loads(shapely.to_geojson(geometry.to_geometry(area))),
        "date": _date(date),
        "bands": names,
        "index": index,
        "collection": collection,
        "aggregation_interval": aggregation_interval,
        "resolution": float(resolution),
        "cloud_cover": float(cloud_cover),
        "mask": mask,
        "percentiles": _percentiles(percentiles),
        "histogram": _histogram(histogram),
    }


def run_statistical(request):
    """Returns a long DataFrame: interval_from, interval_to, output, mean, min, max, std, sample_count, no_data_count ..."""
    import numpy as np

    req = check_statistical_request(request)
    outputs = req["bands"] + ([req["index"]] if req["index"] else [])
    empty = _empty(req["percentiles"], req["histogram"])
    geom = geometry.to_geometry(req["area"])
    date = tuple(req["date"]) if isinstance(req["date"], list) else req["date"]
    products = catalog.query(geom, date, collection=req["collection"], cloud_cover=req["cloud_cover"])
    if not len(products):
        return empty
    products = _one_per_day(products, geom)
    load = list(dict.fromkeys(req["bands"] + (indices.required_bands(req["index"]) if req["index"] else [])))
    geobox = geometry.make_geobox(geom, resolution=req["resolution"])
    try:
        ds = reader.read_stack(products, load, geobox, mosaicking="orbit", mask=req["mask"])
    except constants.NoDataError:
        return empty
    inside = _inside(geom, geobox)
    arrays = [indices.compute_index(ds, n) if n == req["index"] else ds[n] for n in outputs]
    values = np.stack([a.transpose("time", "y", "x").values[:, inside] for a in arrays])
    start, end = time.parse_time_interval(date)
    intervals = _intervals(start, end, req["aggregation_interval"])
    return _stats_table(values, ds["time"].values, intervals, outputs, req["percentiles"], req["histogram"])


def check_statistical_request(request):
    """Returns the request dict after it validates it again through statistical_request (only our own dicts run)."""
    if not isinstance(request, dict) or request.get("type") != "statistical":
        raise ValueError("request must be a dict made by statistical_request, with type 'statistical'")
    unknown = sorted(set(request) - set(_FIELDS) - {"type"})
    missing = [k for k in _FIELDS if k not in request]
    if unknown or missing:
        raise ValueError(f"request has unknown keys {unknown} and missing keys {missing}; "
                         f"the keys must be {['type', *_FIELDS]}")
    return statistical_request(**{k: v for k, v in request.items() if k != "type"})


def _one_per_day(products, geom):
    covers = products.geometry.covers(geom)
    best = products[covers].sort_values(["cloud_cover", "id"]).drop_duplicates("date")
    return products[products.index.isin(best.index) | ~products["date"].isin(best["date"])]


def _stats_table(values, days, intervals, names, percentiles=None, histogram=None):
    import numpy as np
    import pandas as pd

    days = pd.DatetimeIndex(days).normalize()
    starts = pd.DatetimeIndex([a for a, _ in intervals])
    slot = starts.searchsorted(days, side="right") - 1
    keep = (slot >= 0) & (days <= intervals[-1][1])
    rows = []
    for k in sorted(set(slot[keep])):
        v = values[:, keep & (slot == k)]
        ok = np.isfinite(v).all(axis=0)
        order = np.argsort(-ok.sum(axis=1), kind="stable")
        v, ok = v[:, order], ok[order]
        has = ok.any(axis=0)
        if not has.any():
            continue
        pick = np.take_along_axis(v, ok.argmax(axis=0)[None, None], axis=1)[:, 0][:, has]
        a, b = intervals[k]
        for name, x in zip(names, pick.astype("float64")):
            row = {"interval_from": f"{a:%Y-%m-%d}", "interval_to": f"{b:%Y-%m-%d}", "output": name,
                   "mean": float(x.mean()), "min": float(x.min()), "max": float(x.max()), "std": float(x.std()),
                   "sample_count": int(v.shape[2]),
                   "no_data_count": int(v.shape[2] - has.sum())}
            for p in percentiles or []:
                row[_pname(p)] = float(np.percentile(x, p))
            if histogram:
                counts, _ = np.histogram(x, bins=histogram["bins"], range=(histogram["low"], histogram["high"]))
                row["histogram"] = [int(c) for c in counts]
            rows.append(row)
    if not rows:
        return _empty(percentiles, histogram)
    return pd.DataFrame(rows, columns=list(_empty(percentiles, histogram).columns))


def _intervals(start, end, duration):
    import pandas as pd

    step = _duration(duration)
    first = pd.Timestamp(start).tz_localize(None).normalize()
    last = pd.Timestamp(end).tz_localize(None).normalize()
    out, k = [], 0
    while first + step * k <= last:
        a, b = first + step * k, first + step * (k + 1) - pd.Timedelta(days=1)
        out.append((a, min(b, last)))
        k += 1
    return out


def _duration(value):
    import pandas as pd

    match = _DURATION.match(str(value).strip().upper()) if isinstance(value, str) else None
    if not match or int(match.group(1)) < 1:
        raise ValueError(f"aggregation_interval must be {_DURATION_FORMS}, got {value!r}")
    n, unit = int(match.group(1)), match.group(2)
    return {"D": pd.DateOffset(days=n), "W": pd.DateOffset(days=7 * n), "M": pd.DateOffset(months=n),
            "Y": pd.DateOffset(years=n)}[unit]


def _inside(geom, geobox):
    from odc.geo.geom import Geometry
    from rasterio.features import geometry_mask

    shape = Geometry(geom, crs="EPSG:4326").to_crs(geobox.crs).geom
    inside = geometry_mask([shape], out_shape=geobox.shape, transform=geobox.affine, invert=True)
    if not inside.any():
        inside = geometry_mask([shape], out_shape=geobox.shape, transform=geobox.affine, invert=True, all_touched=True)
    return inside


def _date(date):
    if isinstance(date, str) and "/" in date:
        date = tuple(date.split("/"))
    if date is None:
        date = tuple(f"{t:%Y-%m-%d}" for t in time.parse_time_interval(None))
    time.parse_time_interval(date)
    value = time.serialize_time(tuple(date) if isinstance(date, list) else date)
    return list(value) if isinstance(value, tuple) else value


def _percentiles(percentiles):
    import numbers

    if percentiles is None:
        return None
    values = [percentiles] if isinstance(percentiles, numbers.Real) else list(percentiles)
    if not values or not all(isinstance(p, numbers.Real) and not isinstance(p, bool) and 0 <= p <= 100 for p in values):
        raise ValueError(f"percentiles must be None or a list of numbers from 0 to 100 (for example [5, 50, 95]), "
                         f"got {percentiles!r}")
    return [int(p) if float(p).is_integer() else float(p) for p in values]


def _histogram(histogram):
    import numbers

    if histogram is None:
        return None
    form = "None or a dict {'bins': 20, 'low': -1, 'high': 1} (bins a positive integer, low < high)"
    if not isinstance(histogram, dict) or set(histogram) != {"bins", "low", "high"}:
        raise ValueError(f"histogram must be {form}, got {histogram!r}")
    bins, low, high = histogram["bins"], histogram["low"], histogram["high"]
    numeric = all(isinstance(v, numbers.Real) and not isinstance(v, bool) for v in (bins, low, high))
    if not numeric or int(bins) != bins or bins < 1 or not low < high:
        raise ValueError(f"histogram must be {form}, got {histogram!r}")
    return {"bins": int(bins), "low": float(low), "high": float(high)}


def _pname(p):
    return f"p{int(p):02d}" if float(p).is_integer() else f"p{p}"


def _empty(percentiles, histogram):
    import pandas as pd

    return pd.DataFrame(columns=_COLUMNS + [_pname(p) for p in percentiles or []] + (["histogram"] if histogram else []))


_MGRS_ROWS = "CDEFGHJKLMNPQRSTUVWX"
_MAX_CELLS = 1_000_000
_MAX_LAT_3857 = 85.0511


def bbox_splitter(area, split_shape=(5, 4), *, reduce_bbox_sizes=False):
    """Returns the area bbox cut into split_shape (columns, rows) equal EPSG:4326 cells that meet the area."""
    import shapely

    geom = geometry.to_geometry(area)
    cols, rows = _count_pair(split_shape, "split_shape")
    w, s, e, n = geom.bounds
    out = _grid((w, s), ((e - w) / cols, (n - s) / rows), (cols, rows), "EPSG:4326", geom)
    if reduce_bbox_sizes:
        bounds = out.geometry.intersection(geom).bounds.to_numpy()
        out["bbox"] = [tuple(float(v) for v in b) for b in bounds]
        out = out.set_geometry(shapely.box(*bounds.T), crs="EPSG:4326")
    return out


def osm_splitter(area, zoom_level):
    """Returns the XYZ web map tiles at zoom_level that meet the area; bbox in EPSG:3857."""
    import geopandas as gpd
    import mercantile
    import shapely

    geom = geometry.to_geometry(area)
    w, s, e, n = geom.bounds
    if max(abs(s), abs(n)) > _MAX_LAT_3857:
        raise ValueError(f"osm_splitter needs latitudes between -{_MAX_LAT_3857} and {_MAX_LAT_3857}, got {s}, {n}")
    if not 0 <= int(zoom_level) <= 24:
        raise ValueError(f"zoom_level must be an integer from 0 to 24, got {zoom_level!r}")
    tiles = list(mercantile.tiles(w, s, e, n, zooms=int(zoom_level)))
    out = gpd.GeoDataFrame(
        {"index_x": [t.x for t in tiles], "index_y": [t.y for t in tiles], "crs": "EPSG:3857",
         "bbox": [tuple(mercantile.xy_bounds(t)) for t in tiles], "zoom": [t.z for t in tiles],
         "x": [t.x for t in tiles], "y": [t.y for t in tiles]},
        geometry=[shapely.box(*mercantile.bounds(t)) for t in tiles], crs="EPSG:4326")
    return out[_meets(out.geometry, geom)].reset_index(drop=True)


def utm_grid_splitter(area, bbox_size):
    """Returns cells of bbox_size metres aligned to the UTM grid, made per MGRS grid zone (6 x 8 degrees)."""
    w, s, e, n = geometry.to_bbox(area)
    zones = []
    for zone in _zones(w, e):
        for k, row in enumerate(_MGRS_ROWS):
            lat0, lat1 = -80 + 8 * k, (84 if row == "X" else -72 + 8 * k)
            if lat1 > s and lat0 < n:
                zones.append((zone, row, row >= "N", (-186 + 6 * zone, lat0, -180 + 6 * zone, lat1)))
    return _utm_split(area, zones, bbox_size, None)


def utm_zone_splitter(area, bbox_size, *, offset=None):
    """Returns cells of bbox_size metres aligned to multiples of bbox_size (+ offset) in each UTM zone."""
    w, s, e, n = geometry.to_bbox(area)
    zones = [(zone, "", north, (-186 + 6 * zone, 0 if north else -80, -180 + 6 * zone, 84 if north else 0))
             for zone in _zones(w, e) for north in (True, False)]
    return _utm_split(area, zones, bbox_size, offset)


def tile_splitter(area, date, *, tile_split_shape=1):
    """Returns the Sentinel-2 MGRS tiles over the area found by su_search.query, each cut into tile_split_shape."""
    import shapely

    geom = geometry.to_geometry(area)
    products = catalog.query(area, date)
    if not len(products):
        raise constants.NoDataError(f"no Sentinel-2 scene over the area for date {date!r}")
    cols, rows = _count_pair(tile_split_shape, "tile_split_shape")
    parts = []
    for tile_id, group in products.groupby("tile_id", sort=True):
        crs = str(group["crs"].iloc[0])
        x0, y0, x1, y1 = _project(shapely.union_all(group.geometry.values), crs).bounds
        cells = _grid((x0, y0), ((x1 - x0) / cols, (y1 - y0) / rows), (cols, rows), crs, _project(geom, crs))
        parts.append(cells.assign(tile_id=tile_id))
    return _concat(parts, ["tile_id"])


def custom_grid_splitter(area, grid):
    """Returns the cells of grid (GeoDataFrame, or list of EPSG:4326 bboxes) that meet the area; index_x = grid position."""
    import geopandas as gpd
    import shapely

    geom = geometry.to_geometry(area)
    if hasattr(grid, "to_crs"):
        cells = gpd.GeoSeries(grid.geometry.values, crs=grid.crs or "EPSG:4326")
    else:
        cells = gpd.GeoSeries([shapely.box(*geometry.to_bbox(b)) for b in grid], crs="EPSG:4326")
    crs = cells.crs.to_string()
    out = gpd.GeoDataFrame(
        {"index_x": range(len(cells)), "index_y": 0, "crs": crs,
         "bbox": [tuple(float(v) for v in b) for b in cells.bounds.to_numpy()]},
        geometry=_to_wgs84(cells).values, crs="EPSG:4326")
    return out[_meets(out.geometry, geom)].reset_index(drop=True)


def _utm_split(area, zones, bbox_size, offset):
    import shapely

    geom = geometry.to_geometry(area)
    sx, sy = _size_pair(bbox_size, "bbox_size")
    ox, oy = (0.0, 0.0) if offset is None else _size_pair(offset, "offset", allow_zero=True)
    parts = []
    for zone, row, north, box in zones:
        part = shapely.box(*box).intersection(geom)
        if part.is_empty or (geom.area > 0 and part.area == 0):
            continue
        crs = f"EPSG:{(32600 if north else 32700) + zone}"
        clip = _project(part, crs)
        x0, y0, x1, y1 = clip.bounds
        ax, ay = (x0 - ox) // sx * sx + ox, (y0 - oy) // sy * sy + oy
        counts = (max(1, int(-((ax - x1) // sx))), max(1, int(-((ay - y1) // sy))))
        if counts[0] * counts[1] > _MAX_CELLS:
            raise ValueError(f"bbox_size {bbox_size!r} gives over {_MAX_CELLS} cells; use a larger bbox_size")
        cells = _grid((ax, ay), (sx, sy), counts, crs, clip).assign(utm_zone=f"{zone:02d}")
        parts.append(cells.assign(utm_row=row) if row else cells)
    return _concat(parts, ["utm_zone", "utm_row"] if any(z[1] for z in zones) else ["utm_zone"])


def _grid(origin, size, counts, crs, clip):
    import geopandas as gpd
    import numpy as np
    import shapely

    (x0, y0), (sx, sy), (nx, ny) = origin, size, counts
    i, j = (a.ravel() for a in np.meshgrid(np.arange(nx), np.arange(ny), indexing="ij"))
    xmin, ymin = x0 + i * sx, y0 + j * sy
    cells = gpd.GeoSeries(shapely.box(xmin, ymin, xmin + sx, ymin + sy), crs=crs)
    keep = _meets(cells, clip)
    cells = cells[keep]
    return gpd.GeoDataFrame(
        {"index_x": i[keep], "index_y": j[keep], "crs": crs,
         "bbox": [tuple(float(v) for v in b) for b in cells.bounds.to_numpy()]},
        geometry=_to_wgs84(cells).values, crs="EPSG:4326")


def _meets(cells, geom):
    import shapely

    boxes = cells.to_numpy()
    hits = shapely.intersects(boxes, geom)
    if geom.area > 0 and hits.any():
        hits[hits] = shapely.area(shapely.intersection(boxes[hits], geom)) > 1e-9 * shapely.area(boxes[hits])
    return hits


def _to_wgs84(cells):
    if cells.crs.to_epsg() == 4326:
        return cells
    width = (cells.bounds["maxx"] - cells.bounds["minx"]).min() if len(cells) else 1.0
    return cells.segmentize(max(width / 8, 1.0)).to_crs("EPSG:4326")


def _project(geom, crs):
    import numpy as np
    import shapely
    from pyproj import Transformer

    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    return shapely.transform(shapely.segmentize(geom, 0.01),
                             lambda xy: np.column_stack(transformer.transform(xy[:, 0], xy[:, 1])))


def _zones(w, e):
    return range(min(int((w + 180) // 6) + 1, 60), min(int((e + 180) // 6) + 1, 60) + 1)


def _concat(parts, extra):
    import geopandas as gpd
    import pandas as pd

    columns = ["index_x", "index_y", "crs", "bbox", *extra, "geometry"]
    if not parts:
        return gpd.GeoDataFrame({c: [] for c in columns}, geometry="geometry", crs="EPSG:4326")
    return gpd.GeoDataFrame(pd.concat(parts, ignore_index=True)[columns], geometry="geometry", crs="EPSG:4326")


def _count_pair(value, name):
    pair = (value, value) if isinstance(value, int) else tuple(value) if isinstance(value, (list, tuple)) else ()
    if len(pair) != 2 or not all(isinstance(v, int) and v >= 1 for v in pair):
        raise ValueError(f"{name} must be a positive integer or a (columns, rows) pair of positive integers, got {value!r}")
    return pair


def _size_pair(value, name, allow_zero=False):
    import numbers

    pair = (value, value) if isinstance(value, numbers.Real) else tuple(value) if isinstance(value, (list, tuple)) else ()
    low = 0 if allow_zero else 1e-9
    if len(pair) != 2 or not all(isinstance(v, numbers.Real) and v >= low for v in pair):
        raise ValueError(f"{name} must be a positive number of metres or a (x, y) pair of them, got {value!r}")
    return float(pair[0]), float(pair[1])


import dataclasses


METADATA = ("cloud_cover",)
OPERATORS = {"equals": ("min", "max"), "less_than": ("max",), "not_greater_than": ("max",),
             "greater_than": ("min",), "not_less_than": ("min",)}
REGION_REDUCERS = ("mean", "median", "min", "max", "std", "count")


def collection(name=constants.SENTINEL2_L2A):
    """Returns a lazy image collection of `name` (one of DATA_COLLECTIONS); chain filters, then a composite."""
    return _Collection(constants.check_choice("collection", name, constants.DATA_COLLECTIONS))


@dataclasses.dataclass(frozen=True, eq=False)
class _Collection:
    name: str
    area: object = None
    date: object = None
    cloud: tuple = (0.0, 100.0)
    bands: tuple = None

    def filter_bounds(self, area):
        return dataclasses.replace(self, area=area)

    def filter_date(self, start, end=None):
        return dataclasses.replace(self, date=start if end is None else (start, end))

    def filter_metadata(self, name, operator, value):
        constants.check_choice("name", name, METADATA)
        limits = OPERATORS[constants.check_choice("operator", operator, OPERATORS)]
        low, high = self.cloud
        low = max(low, float(value)) if "min" in limits else low
        high = min(high, float(value)) if "max" in limits else high
        return dataclasses.replace(self, cloud=(low, high))

    def select(self, bands):
        return dataclasses.replace(self, bands=tuple(constants.band_names(bands, self.name)))

    def get_info(self):
        return catalog.query(self._area(), self.date, collection=self.name, cloud_cover=self._cloud())

    def size(self):
        return len(self.get_info())

    def first(self):
        return self.mosaic("oldest")

    def mosaic(self, order="least_cloudy"):
        return _Image(self, mosaic_order=constants.check_choice("order", order, constants.MOSAIC_ORDERS))

    def median(self):
        return _Image(self, reducer="median")

    def mean(self):
        return _Image(self, reducer="mean")

    def min(self):
        return _Image(self, reducer="min")

    def max(self):
        return _Image(self, reducer="max")

    def count(self):
        return _Image(self, reducer="count")

    def aggregate(self, interval="P1M", reducer="median"):
        constants.check_choice("reducer", reducer, process.REDUCERS)
        start, end = time.parse_time_interval(self.date)
        return _ImageList(_Image(self.filter_date(f"{a:%Y-%m-%d}", f"{b:%Y-%m-%d}"), reducer=reducer)
                          for a, b in _intervals(start, end, interval))

    def _area(self):
        if self.area is None:
            raise ValueError("the request needs an area: call filter_bounds(area) or clip(area) first")
        return self.area

    def _cloud(self):
        low, high = self.cloud
        return high if low == 0 else (low, high)


@dataclasses.dataclass(frozen=True, eq=False)
class _Image:
    source: _Collection
    mosaic_order: str = "least_cloudy"
    reducer: str = None
    index_name: str = None
    mask: str = None
    resolution: float = None

    def select(self, bands):
        return dataclasses.replace(self, source=self.source.select(bands))

    def normalized_difference(self, bands):
        pair = tuple(constants.band_names(bands, self.source.name))
        names = [n for n, b in indices.INDICES.items() if tuple(b) == pair]
        if not names:
            pairs = {n: b for n, b in indices.INDICES.items() if len(b) == 2 and all(isinstance(x, str) for x in b)}
            raise ValueError(f"bands must be the pair of one index in {pairs}, got {list(pair)}")
        return self.index(names[0])

    def index(self, name):
        name = constants.check_choice("index", str(name).upper(), indices.INDICES)
        return dataclasses.replace(self, index_name=name)

    def update_mask(self, mask_name):
        return dataclasses.replace(self, mask=constants.check_choice("mask", mask_name, constants.MASKS))

    def clip(self, area):
        return dataclasses.replace(self, source=self.source.filter_bounds(area))

    def resample(self, resolution):
        return dataclasses.replace(self, resolution=resolution)

    def to_request(self, output="dataset"):
        src = self.source
        return process.process_request(src._area(), src.date, src.bands, collection=src.name,
                                       resolution=self.resolution, mosaic_order=self.mosaic_order,
                                       cloud_cover=src._cloud(), mask=self.mask, index=self.index_name,
                                       reducer=self.reducer, output=output)

    def get_data(self, output="dataset"):
        return get_data(self.to_request(output))

    def save_data(self, path=None):
        return save_data(self.to_request(), path)

    def reduce_region(self, reducer="mean", area=None, resolution=None):
        request = self._region_request(reducer, area, resolution)
        return _region_table(get_data(request), request, self._outputs(request), reducer)

    def reduce_regions(self, features, reducer="mean", resolution=None, max_workers=16, engine="remote"):
        import pandas as pd

        if getattr(features, "crs", None) is not None:
            features = features.to_crs(4326)
        shapes, ids = ((list(features.geometry), list(features.index)) if hasattr(features, "geometry")
                       else (list(features), list(range(len(features)))))
        requests = [self._region_request(reducer, shape, resolution) for shape in shapes]
        images = download(requests, max_workers=max_workers, engine=engine)
        tables = [_region_table(ds, r, self._outputs(r), reducer).assign(feature=i)
                  for i, ds, r in zip(ids, images, requests)]
        return pd.concat(tables, ignore_index=True)[["feature", "interval_from", "interval_to", "output", reducer]]

    def _region_request(self, reducer, area, resolution):
        constants.check_choice("reducer", reducer, REGION_REDUCERS)
        image = self if area is None else self.clip(area)
        return image.resample(resolution or self.resolution).to_request()

    def _outputs(self, request):
        bands = [b for b in request["bands"] if b != "dataMask"]
        if not self.index_name:
            return bands
        return (bands if self.source.bands else []) + [self.index_name]


class _ImageList(tuple):
    def map(self, function):
        return _ImageList(function(image) for image in self)

    def to_request(self, output="dataset"):
        return [image.to_request(output) for image in self]

    def get_data(self, output="dataset", max_workers=16, engine="remote", ignore_errors=False):
        return download(self.to_request(output), max_workers=max_workers, engine=engine,
                                 ignore_errors=ignore_errors)

    def reduce_region(self, reducer="mean", area=None, resolution=None, max_workers=16, engine="remote"):
        import pandas as pd

        requests = [image._region_request(reducer, area, resolution) for image in self]
        outcomes = _collect(requests, max_workers, engine)
        failed = [(i, err) for i, (_, err) in enumerate(outcomes) if err is not None and not _no_data(err)]
        if failed:
            raise RuntimeError(_failures(failed, len(requests)))
        tables = [_region_table(ds, r, image._outputs(r), reducer)
                  for (ds, err), r, image in zip(outcomes, requests, self) if err is None]
        if not tables:
            raise constants.NoDataError("no interval has data in the area")
        return pd.concat(tables, ignore_index=True)


def _region_table(ds, request, names, reducer):
    import numpy as np

    inside = _inside(geometry.to_geometry(request["area"]), process._geobox(request))
    valid = ds["dataMask"].values > 0
    values = np.stack([np.where(valid, ds[n].values, np.nan)[inside] for n in names]).astype("float64")[:, None]
    date = tuple(request["date"]) if isinstance(request["date"], list) else request["date"]
    first, last = (t.tz_localize(None).normalize() for t in time.parse_time_interval(date))
    table = _stats_table(values, [first], [(first, last)], names, [50] if reducer == "median" else None)
    column = (table["sample_count"] - table["no_data_count"] if reducer == "count"
              else table["p50" if reducer == "median" else reducer])
    return table[["interval_from", "interval_to", "output"]].assign(**{reducer: column})


ENGINES = ("remote", "local")
REQUEST_TYPES = ("process", "statistical")
SAVE_DIR = "fd://sentinel_utils/data"
REMOTE_TIMEOUT = 120
POLL_SECONDS = 0.05
FINAL_ERRORS = ("NoDataError", "ValueError")
WORKER_CODE = '''@fused.udf(cache_max_age="0s")
def udf(request: str = ""):
    import json
    import su_analysis

    return su_analysis.to_payload(su_analysis.get_data(json.loads(request)))
'''


def get_data(request):
    """Returns the result of one request (from process_request or statistical_request), run in this process."""
    request = check_request(request)
    if request["type"] == "process":
        return process.run_process(request)
    return run_statistical(request)


def check_request(request):
    """Returns the request after it validates it again through its constructor; other dicts raise ValueError."""
    if not isinstance(request, dict):
        raise ValueError(f"a request must be a dict made by process_request or statistical_request, "
                         f"got {type(request).__name__}")
    constants.check_choice("request type", request.get("type"), REQUEST_TYPES)
    if request["type"] == "process":
        return process.check_request(request)
    return check_statistical_request(request)


def download(requests, *, max_workers=16, engine="remote", ignore_errors=False):
    """Returns the results in request order; engine remote runs each on a Fused worker (worker()), local in threads."""
    import warnings

    outcomes = _collect(requests, max_workers, engine)
    failed = [(i, err) for i, (_, err) in enumerate(outcomes) if err is not None]
    if failed:
        msg = _failures(failed, len(outcomes))
        if not ignore_errors:
            raise RuntimeError(msg)
        warnings.warn(msg)
    return [result for result, _ in outcomes]


def iter_download(requests, *, max_workers=16, engine="remote", ignore_errors=False):
    """Yields (index, result) in the order the requests finish; index is the position of the request in requests."""
    checked, workers = _prepare(requests, max_workers, engine)
    return _progressive(checked, workers, engine, ignore_errors)


def save_data(request, path=None):
    """Returns the path of the saved result: a COG (PNG for output png) or Parquet; default folder SAVE_DIR."""
    import hashlib
    import json
    import os
    import shutil
    import tempfile

    request = check_request(request)
    if request["type"] == "process":
        ext = "png" if request["output"] == "png" else "tif"
        request = check_request({**request, "output": "png" if ext == "png" else "geotiff"})
    else:
        ext = "parquet"
    key = hashlib.sha1(json.dumps(request, sort_keys=True).encode()).hexdigest()[:12]
    path = str(path or f"{SAVE_DIR}/{request['type']}_{key}.{ext}")
    data = get_data(request)
    local = os.path.join(tempfile.mkdtemp(), os.path.basename(path))
    if ext == "parquet":
        data.to_parquet(local)
    else:
        with open(local, "wb") as f:
            f.write(data)
    if path.startswith(("fd://", "s3://")):
        fused.api.upload(local, path)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        shutil.copy(local, path)
    return path


def worker():
    """Returns the worker Udf: WORKER_CODE plus the source of each module it needs as a header (no load by name)."""
    import linecache

    modules = {"su_search": su_search, "su_read": su_read, "su_analysis": globals()}
    headers = []
    for name, module in modules.items():
        loader = module.get("__loader__") if isinstance(module, dict) else getattr(module, "__loader__", None)
        source = (getattr(module, "code", None) or getattr(loader, "source_code", None)
                  or "".join(linecache.getlines(f"<module {name}>")))
        if not source:
            raise RuntimeError(f"no source found for {name}; load sentinel_utils with fused.load first")
        headers.append({"module_name": name, "source_code": source})
    return fused.models.Udf.model_validate(
        {"name": "su_worker", "type": "geopandas_v2", "entrypoint": "udf", "code": WORKER_CODE, "headers": headers,
         "metadata": {}}, context={"import_globals": False, "load_parameter_list": True})


def to_payload(result):
    """Returns a one-row DataFrame (kind, payload, meta) that carries a result of get_data through `.map` unchanged."""
    import io
    import json
    import numpy as np
    import pandas as pd
    import xarray as xr

    meta = {}
    if isinstance(result, xr.Dataset):
        ds = result.drop_vars("spatial_ref", errors="ignore")
        names = [*ds.data_vars, *ds.coords]
        meta = {"layers": [[k, list(ds[k].dims), dict(ds[k].attrs), k in ds.coords] for k in names],
                "attrs": dict(result.attrs), "crs": str(_odc(result).crs) if _odc(result).crs is not None else None}
        kind, data = "dataset", _npz([ds[k].values for k in names])
    elif isinstance(result, np.ndarray):
        kind, data = "array", _npz([result])
    elif isinstance(result, (bytes, bytearray)):
        kind, data = "bytes", bytes(result)
    elif isinstance(result, pd.DataFrame):
        buf = io.BytesIO()
        result.to_parquet(buf)
        kind, data, meta = "frame", buf.getvalue(), {"geo": hasattr(result, "geometry") and "geometry" in result}
    else:
        kind, data = "json", json.dumps(result).encode()
    return pd.DataFrame({"kind": [kind], "payload": [data], "meta": [json.dumps(meta, default=_plain)]})


def from_payload(frame):
    """Returns the result that to_payload packed: xarray Dataset, numpy array, bytes, DataFrame or JSON value."""
    import io
    import json
    import numpy as np
    import pandas as pd

    kind, data, meta = frame["kind"].iloc[0], frame["payload"].iloc[0], json.loads(frame["meta"].iloc[0])
    if kind == "dataset":
        import xarray as xr

        with np.load(io.BytesIO(data), allow_pickle=False) as z:
            arrays = [z[f"arr_{i}"] for i in range(len(z.files))]
        layers = [(name, xr.Variable(dims, arr, attrs), coord)
                  for (name, dims, attrs, coord), arr in zip(meta["layers"], arrays)]
        ds = xr.Dataset({n: v for n, v, coord in layers if not coord}, coords={n: v for n, v, coord in layers if coord},
                        attrs=meta["attrs"])
        return _odc(ds).assign_crs(meta["crs"]) if meta["crs"] else ds
    if kind == "array":
        with np.load(io.BytesIO(data), allow_pickle=False) as z:
            return z["arr_0"]
    if kind == "bytes":
        return bytes(data)
    if kind == "frame":
        if meta.get("geo"):
            import geopandas as gpd

            return gpd.read_parquet(io.BytesIO(data))
        return pd.read_parquet(io.BytesIO(data))
    return json.loads(data)


def _prepare(requests, max_workers, engine):
    constants.check_choice("engine", engine, ENGINES)
    if isinstance(requests, dict):
        raise ValueError("requests must be a list of request dicts; put one request in a list")
    checked = [check_request(r) for r in requests]
    return checked, max(1, min(int(max_workers), len(checked) or 1))


def _collect(requests, max_workers, engine):
    checked, workers = _prepare(requests, max_workers, engine)
    outcomes = [(None, None)] * len(checked)
    stream = _stream(checked, workers, engine)
    try:
        for index, result, err in stream:
            outcomes[index] = (result, err)
    finally:
        stream.close()
    return outcomes


def _no_data(err):
    return isinstance(err, constants.NoDataError) or "NoDataError" in str(err)


def _progressive(requests, workers, engine, ignore_errors):
    import warnings

    failed = []
    stream = _stream(requests, workers, engine)
    try:
        for index, result, err in stream:
            if err is not None:
                if not ignore_errors:
                    raise RuntimeError(f"request {index} of {len(requests)} failed: {_message(err)}") from err
                failed.append((index, err))
            yield index, result
    finally:
        stream.close()
    if failed:
        warnings.warn(_failures(sorted(failed, key=lambda f: f[0]), len(requests)))


def _stream(requests, workers, engine):
    if requests:
        yield from _iter_remote(requests, workers) if engine == "remote" else _iter_local(requests, workers)


def _iter_local(requests, workers):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    executor = ThreadPoolExecutor(workers)
    try:
        futures = {executor.submit(get_data, r): i for i, r in enumerate(requests)}
        for future in as_completed(futures):
            err = future.exception()
            yield futures[future], None if err else future.result(), err
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _iter_remote(requests, workers):
    import asyncio
    import numbers

    timeout = fused.options.request_timeout
    if isinstance(timeout, numbers.Real) and timeout < REMOTE_TIMEOUT:
        fused.options.request_timeout = REMOTE_TIMEOUT
    pools = []
    try:
        pools.append(_submit(requests, list(range(len(requests))), workers))
        left, retried = set(range(len(requests))), set()
        while left:
            again = []
            for pool, indices, seen in pools:
                for j, got in sorted(pool.results_now(return_exceptions=True).items()):
                    index = indices[j]
                    if j in seen or index not in left:
                        continue
                    seen.add(j)
                    if isinstance(got, BaseException) and index not in retried and _retryable(got):
                        retried.add(index)
                        again.append(index)
                        continue
                    left.discard(index)
                    yield _landed(index, got)
            if again:
                pools.append(_submit(requests, again, workers))
            if left:
                asyncio.run(asyncio.sleep(POLL_SECONDS))
    finally:
        for pool, _, _ in pools:
            pool.cancel(wait=True)
        fused.options.request_timeout = timeout


def _submit(requests, indices, workers):
    import json

    args = [{"request": json.dumps(requests[i])} for i in indices]
    pool = worker().map(args, max_workers=min(workers, len(args)), max_retry=0, cache_max_age="0s")
    return pool, indices, set()


def _retryable(err):
    return not any(name in str(err) for name in FINAL_ERRORS)


def _landed(index, got):
    if isinstance(got, BaseException):
        return index, None, got
    try:
        return index, from_payload(got), None
    except Exception as e:
        return index, None, e


def _failures(failed, n):
    first, err = failed[0]
    return f"{len(failed)} of {n} requests failed; first: request {first}: {_message(err)}"


def _message(err):
    lines = [s.strip() for s in str(err).splitlines() if s.strip()]
    return f"{type(err).__name__}: {lines[-1] if lines else repr(err)}"[:300]


def _npz(arrays):
    import io
    import numpy as np

    buf = io.BytesIO()
    np.savez_compressed(buf, *[a.astype(str) if a.dtype == object else a for a in arrays])
    return buf.getvalue()


def _plain(value):
    return value.item() if hasattr(value, "item") else str(value)


def _odc(obj):
    import odc.geo.xr

    return obj.odc


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return su_search.api_table(globals())
