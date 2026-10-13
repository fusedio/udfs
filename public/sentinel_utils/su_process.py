"""Process API analog (sentinelhub-py api/process.py): make a JSON request for one image of an area, then run it."""

try:
    import su_constants as constants
    import su_geometry as geometry
    import su_time as time
    import su_catalog as catalog
    import su_reader as reader
    import su_indices as indices
    import su_embeddings as embeddings
except ImportError:
    constants = fused.load("su_constants")
    geometry = fused.load("su_geometry")
    time = fused.load("su_time")
    catalog = fused.load("su_catalog")
    reader = fused.load("su_reader")
    indices = fused.load("su_indices")
    embeddings = fused.load("su_embeddings")

REDUCERS = ("median", "mean", "min", "max", "count")
SIMPLE_STOP = 0.995
SIMPLE_BATCH = 16
DEFAULT_BANDS = {constants.SENTINEL2_L2A: ("B04", "B03", "B02"), constants.DEM: ("DEM",),
                 constants.AEF: constants.DATA_COLLECTIONS[constants.AEF]["bands"]}
DEM_ENV = {**reader.GDAL_ENV, "AWS_REGION": "eu-central-1"}
_FIELDS = ("area", "date", "bands", "collection", "resolution", "size", "crs", "mosaicking", "mosaic_order",
           "cloud_cover", "units", "resampling", "mask", "index", "reducer", "output")


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def process_request(area, date, bands=None, *, collection=constants.SENTINEL2_L2A, resolution=None, size=None,
                    crs=None, mosaicking="simple", mosaic_order="least_cloudy", cloud_cover=100, units="reflectance",
                    resampling="nearest", mask=None, index=None, reducer=None, output="dataset"):
    """Returns a validated JSON-safe request dict ("type": "process"); bands None gives B04, B03, B02 (DEM: DEM)."""
    import json

    constants.check_choice("collection", collection, constants.DATA_COLLECTIONS)
    for name, value, allowed in (("mosaicking", mosaicking, constants.MOSAICKING),
                                 ("mosaic_order", mosaic_order, constants.MOSAIC_ORDERS),
                                 ("units", units, constants.UNITS), ("resampling", resampling, constants.RESAMPLING),
                                 ("output", output, constants.OUTPUTS)):
        constants.check_choice(name, value, allowed)
    if mask is not None:
        constants.check_choice("mask", mask, constants.MASKS)
    if reducer is not None:
        constants.check_choice("reducer", reducer, REDUCERS)
    if index is not None:
        index = constants.check_choice("index", str(index).upper(), indices.INDICES)
    dem = collection == constants.DEM
    s2 = collection == constants.SENTINEL2_L2A
    if not s2 and (mask or index or reducer):
        raise ValueError(f"a {collection} request takes no mask, index or reducer")
    if bands is None:
        bands = indices.required_bands(index) if index else DEFAULT_BANDS[collection]
    bands = constants.band_names(bands, collection)
    if reducer == "count" and index:
        raise ValueError("index needs reducer median, mean, min or max, not count")
    if resolution is not None and size is not None:
        raise ValueError("give resolution or size, not both")
    if resolution is None and size is None:
        resolution = (min(constants.band_info(b)["resolution"] for b in bands if b != "dataMask") if s2
                      else constants.DATA_COLLECTIONS[collection]["resolution"])
    stack = mosaicking == "orbit" and reducer is None and s2
    layers = len(_output_names({"bands": bands, "index": index}))
    if stack and output in ("geotiff", "png"):
        raise ValueError(f"output {output} holds one image; use mosaicking 'simple' or a reducer {list(REDUCERS)}")
    if output == "png" and layers not in (1, 3):
        raise ValueError(f"output png holds 1 or 3 layers, the request has {layers}; use output geotiff")
    date = str(embeddings.embedding_year(date)) if collection == constants.AEF else _date_json(date, dem)
    request = {"type": "process", "area": _area_json(area), "date": date, "bands": bands,
               "collection": collection, "resolution": resolution, "size": size,
               "crs": None if crs is None else str(crs),
               "mosaicking": mosaicking, "mosaic_order": mosaic_order, "cloud_cover": _cloud(cloud_cover),
               "units": units, "resampling": resampling, "mask": mask, "index": index, "reducer": reducer,
               "output": output}
    request = json.loads(json.dumps(request))
    gb = _geobox(request)
    n_values = gb.shape[0] * gb.shape[1] * (len(bands) + 1)
    if n_values > constants.MAX_PIXELS:
        raise ValueError(f"{gb.shape[0]}x{gb.shape[1]} px x {len(bands) + 1} layers is more than MAX_PIXELS "
                         f"{constants.MAX_PIXELS:,}; use a coarser resolution or split the area (su_areas)")
    return request


def run_process(request):
    """Returns the result of a process request: xarray Dataset, numpy (H, W, B), or COG / PNG bytes, per its output."""
    import numpy as np

    req = check_request(request)
    gb = _geobox(req)
    names = [b for b in req["bands"] if b != "dataMask"]
    if req["collection"] == constants.DEM:
        ds = _read_dem(gb, req["area"], req["resampling"])
    elif req["collection"] == constants.AEF:
        ds = embeddings.load_embeddings(req["area"], req["date"], geobox=gb).to_dataset("band")[names]
        ds["dataMask"] = ds.to_array().notnull().all("variable").astype("uint8")
    else:
        load = names + [b for b in (indices.required_bands(req["index"]) if req["index"] else []) if b not in names]
        products = catalog.query(req["area"], _tuple(req["date"]), cloud_cover=_tuple(req["cloud_cover"]))
        if not len(products):
            raise constants.NoDataError(f"no Sentinel-2 scene for area {req['area']} and date {req['date']}")
        if req["reducer"]:
            ds = reader.read_stack(_clearest(products, load, gb, req), load, gb, resampling=req["resampling"],
                                   mosaic_order=req["mosaic_order"], mask=req["mask"])
            ds = _reduce(ds, load, req["reducer"], req["units"])
        elif req["mosaicking"] == "orbit":
            ds = reader.read_stack(products, load, gb, units=req["units"], resampling=req["resampling"],
                                   mosaic_order=req["mosaic_order"], mask=req["mask"])
        else:
            ds = _simple(products, load, gb, req)
        if req["index"]:
            ds[req["index"]] = _index(ds, req["index"], req["units"])
    ds = _clip(ds, req["area"], gb)
    ds = _odc(ds).assign_crs(gb.crs)
    ds.attrs.update({"collection": req["collection"], "date": str(req["date"])})
    if req["reducer"]:
        ds.attrs["reducer"] = req["reducer"]
    out = _output_names(req)
    if req["output"] == "dataset":
        return ds
    if req["output"] == "array":
        return np.stack([ds[n].values for n in out], axis=-1)
    if req["output"] == "geotiff":
        return _geotiff(ds[out])
    return _png(ds, out, req["units"])


def check_request(request):
    """Returns the request dict after it validates it again through process_request (only our own dicts run)."""
    if not isinstance(request, dict) or request.get("type") != "process":
        raise ValueError("request must be a dict made by process_request, with type 'process'")
    unknown = sorted(set(request) - set(_FIELDS) - {"type"})
    if unknown:
        raise ValueError(f"request has unknown keys {unknown}; allowed keys are {['type', *_FIELDS]}")
    fields = {k: request.get(k) for k in _FIELDS}
    fields["date"] = _tuple(fields["date"])
    fields["cloud_cover"] = _tuple(fields["cloud_cover"])
    return process_request(fields.pop("area"), fields.pop("date"), fields.pop("bands"), **fields)


def _output_names(req):
    return list(req["bands"]) + ([req["index"]] if req["index"] else [])


def _area_json(area):
    import shapely

    geom = geometry.to_geometry(area)
    bbox = geometry.to_bbox(area)
    if geom.equals(shapely.box(*bbox)):
        return list(bbox)
    return shapely.geometry.mapping(geom)


def _date_json(date, dem):
    if date is None:
        if dem:
            return None
        raise ValueError("date is required for Sentinel-2: one day 'YYYY-MM-DD' or a (start, end) pair")
    if isinstance(date, str) and "/" in date:
        date = tuple(date.split("/"))
    time.parse_time_interval(date)
    return time.serialize_time(date)


def _tuple(value):
    return tuple(value) if isinstance(value, list) else value


def _cloud(value):
    import numbers

    pair = value if isinstance(value, (list, tuple)) else (0, value)
    if len(pair) != 2 or not all(isinstance(v, numbers.Real) and 0 <= v <= 100 for v in pair) or pair[0] > pair[1]:
        raise ValueError(f"cloud_cover must be a max percent 0-100 or a (min, max) pair, got {value!r}")
    return [float(v) for v in pair] if isinstance(value, (list, tuple)) else float(value)


def _geobox(req):
    size = tuple(req["size"]) if req["size"] is not None else None
    return geometry.make_geobox(req["area"], resolution=req["resolution"], size=size, crs=req["crs"])


def _layers(names, mask):
    return len(names) + int(bool(mask or {"CLD", "SNW"} & set(names)) and "SCL" not in names)


def _max_scenes(names, gb, mask):
    return max(1, constants.MAX_PIXELS // (gb.shape[0] * gb.shape[1] * _layers(names, mask)))


def _clearest(products, names, gb, req):
    import warnings

    n = _max_scenes(names, gb, req["mask"])
    if len(products) <= n:
        return products
    warnings.warn(f"{len(products)} scenes on {gb.shape[0]}x{gb.shape[1]} px pass MAX_PIXELS "
                  f"{constants.MAX_PIXELS:,}; the reducer uses the {n} clearest scenes (su_catalog.rank)")
    return catalog.rank(products, req["area"], order="clear").head(n)


def _order(products, req):
    if req["mosaic_order"] != "least_cloudy":
        return products.sort_values(["datetime", "id"], ascending=req["mosaic_order"] == "oldest", kind="stable")
    return catalog.rank(products, req["area"], order="clear")


def _simple(products, names, gb, req):
    import warnings

    scenes = _order(products, req)
    last = min(len(scenes), _max_scenes(names, gb, req["mask"]))
    stops, size = [0], scenes["tile_id"].nunique()
    while stops[-1] < last:
        stops.append(min(stops[-1] + size, last))
        size = max(size, min(2 * size, SIMPLE_BATCH))
    groups = [scenes.iloc[a:b] for a, b in zip(stops, stops[1:])] + ([scenes.iloc[last:]] if last < len(scenes) else [])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        later = reader.reach(groups, gb)
    out, used = None, 0
    for k in range(len(stops) - 1):
        try:
            part = reader.read_stack(groups[k], names, gb, units=req["units"], resampling=req["resampling"],
                                     mosaicking="simple", mosaic_order=req["mosaic_order"], mask=req["mask"])
        except constants.NoDataError:
            continue
        used += part.attrs["scenes"]
        if out is None:
            out = part
        else:
            take = (part["dataMask"].values > 0) & (out["dataMask"].values == 0)
            for n in out.data_vars:
                out[n].values[take] = part[n].values[take]
        if k + 1 < len(later) and (later[k + 1] & (out["dataMask"].values == 0)).mean() <= 1 - SIMPLE_STOP:
            break
    if out is None:
        raise constants.NoDataError(f"no scene has every band of {names}")
    empty = float((later[-1] & (out["dataMask"].values == 0)).mean())
    if last < len(scenes) and empty > 1 - SIMPLE_STOP:
        warnings.warn(f"the simple mosaic read the {last} best of {len(scenes)} scenes (MAX_PIXELS); "
                      f"{empty:.1%} of the pixels have no value yet")
    out.attrs["scenes"] = used
    return out


def _reduce(stack, names, reducer, units):
    import warnings
    import xarray as xr

    data = stack[names]
    if reducer == "count":
        out = data.count("time").astype("uint16")
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            out = getattr(data, reducer)("time", skipna=True).astype("float32")
        if units == "dn":
            out = out.assign({n: (out[n] / constants.SCALE).round().clip(1, 65535).fillna(0).astype("uint16")
                              for n in names if n in reader.SPECTRAL})
    out["dataMask"] = xr.concat([data[n].notnull().any("time") for n in names], "band").all("band").astype("uint8")
    out.attrs.update(stack.attrs)
    return out


def _index(ds, name, units):
    src = ds
    if units == "dn":
        src = ds.assign({b: ds[b].where(ds[b] > 0) * constants.SCALE for b in indices.required_bands(name)
                         if b in reader.SPECTRAL})
    return indices.compute_index(src, name).astype("float32").where(ds["dataMask"] > 0)


def _read_dem(gb, area, resampling):
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np
    import odc.geo.xr
    import rasterio
    from rasterio.crs import CRS
    from rio_tiler.errors import TileOutsideBounds
    from rio_tiler.io import Reader

    products = catalog.query(area, collection=constants.DEM)
    if not len(products):
        raise constants.NoDataError(f"no Copernicus DEM tile for area {area}")
    crs = CRS.from_wkt(gb.crs.to_wkt())
    bb = gb.extent.boundingbox

    def read(href):
        try:
            with rasterio.Env(**DEM_ENV), Reader(href) as r:
                img = r.part((bb.left, bb.bottom, bb.right, bb.top), dst_crs=crs, bounds_crs=crs, width=gb.shape[1],
                             height=gb.shape[0], max_size=None, resampling_method=resampling,
                             reproject_method=resampling)
        except TileOutsideBounds:
            return None
        return np.where(img.mask > 0, img.data[0], np.nan).astype("float32")

    dem = np.full(gb.shape, np.nan, dtype="float32")
    with ThreadPoolExecutor(min(16, len(products))) as ex:
        for got in ex.map(read, [a["data"] for a in products["assets"]]):
            if got is not None:
                dem = np.where(np.isnan(dem), got, dem)
    ds = odc.geo.xr.xr_zeros(gb, dtype="float32").to_dataset(name="DEM")
    ds = ds.rename({d: n for d, n in zip(gb.dims, ("y", "x")) if d != n})
    ds["DEM"] = (("y", "x"), dem)
    ds["dataMask"] = (("y", "x"), np.isfinite(dem).astype("uint8"))
    ds.attrs.update({"resolution": abs(gb.resolution.x), "crs": str(gb.crs), "scenes": len(products)})
    return ds


def _clip(ds, area, gb):
    import shapely
    import xarray as xr
    from odc.geo.geom import Geometry
    from rasterio.features import geometry_mask

    if not isinstance(area, dict):
        return ds
    geom = Geometry(shapely.geometry.shape(area), crs="EPSG:4326").to_crs(gb.crs).geom
    inside = xr.DataArray(geometry_mask([geom], out_shape=gb.shape, transform=gb.affine, invert=True), dims=("y", "x"))
    return ds.assign({n: ds[n].where(inside, 0 if ds[n].dtype.kind in "ui" else float("nan")).astype(ds[n].dtype)
                      for n in ds.data_vars})


def _geotiff(ds):
    import os
    import tempfile

    ints = all(ds[n].dtype.kind in "ui" for n in ds.data_vars)
    path = reader.to_cog(ds if ints else ds.astype("float32"), os.path.join(tempfile.mkdtemp(), "process.tif"),
                         nodata=0 if ints else float("nan"))
    with open(path, "rb") as f:
        return f.read()


def _png(ds, names, units):
    import io
    import numpy as np
    from PIL import Image

    layers = [_uint8(ds[n].values.astype("float32"), n, units) for n in names]
    alpha = np.where(ds["dataMask"].values > 0, 255, 0).astype("uint8")
    buf = io.BytesIO()
    Image.fromarray(np.stack(layers + [alpha], axis=-1)).save(buf, format="PNG")
    return buf.getvalue()


def _uint8(values, name, units):
    import numpy as np

    if name in reader.SPECTRAL:
        lo, hi = 0.0, 10000.0 if units == "dn" else 1.0
    elif name in indices.INDICES:
        lo, hi = -1.0, 1.0
    elif name == "dataMask":
        lo, hi = 0.0, 1.0
    else:
        finite = values[np.isfinite(values)]
        lo, hi = (float(finite.min()), float(finite.max())) if finite.size else (0.0, 1.0)
    with np.errstate(invalid="ignore"):
        scaled = np.clip((values - lo) / ((hi - lo) or 1.0), 0, 1) * 255
    return np.nan_to_num(scaled).astype("uint8")


def _odc(obj):
    import odc.geo.xr

    return obj.odc
