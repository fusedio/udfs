"""Statistical API analog: statistics of bands or an index over an area, one row per time interval and output.

Use `statistical_request(...)` to make a JSON-safe request dict and `run_statistical(request)` to get the table.
The data is read one time for the full date range (orbit stack, one layer per solar day), then cut into intervals.
In each interval the clear pixels of the days are mosaicked: the day with the most valid pixels in the area comes
first (least cloudy for the area), and each pixel takes its first valid value. With "P1D" each acquisition day is
one interval, so there is no mosaic. Pixels outside the area polygon are not counted. When one scene covers the full
area, the other scenes of that day (tile overlap) are not read. An index uses band values clipped at 0
(su_indices.compute_index), so NDVI stays in [-1, 1] where the offset makes dark pixels negative.
"""
import re

try:
    import su_constants as constants
    import su_geometry as geometry
    import su_time as time
    import su_catalog as catalog
    import su_reader as reader
    import su_indices as indices
except ImportError:
    constants = fused.load("su_constants")
    geometry = fused.load("su_geometry")
    time = fused.load("su_time")
    catalog = fused.load("su_catalog")
    reader = fused.load("su_reader")
    indices = fused.load("su_indices")

_DURATION = re.compile(r"^P(\d+)([DWMY])$")
_DURATION_FORMS = "an ISO 8601 duration 'P<n>D', 'P<n>W', 'P<n>M' or 'P<n>Y' (for example 'P1D', 'P5D', 'P1M', 'P1Y')"
_FIELDS = ("area", "date", "bands", "index", "collection", "aggregation_interval", "resolution", "cloud_cover", "mask",
           "percentiles", "histogram")
_COLUMNS = ["interval_from", "interval_to", "output", "mean", "min", "max", "std", "sample_count", "no_data_count"]


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


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

    req = check_request(request)
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


def check_request(request):
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
