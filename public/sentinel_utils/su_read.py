"""Pixel engine: map tiles, best-pixel mosaics, COG export and read_stack, on the products table of su_search.

Process API analog (sentinelhub-py api/process.py): make a JSON request for one image of an area, then run it.

AlphaEarth Foundations (AEF) annual embeddings (collection AEF): 64 features (A00..A63) per 10 m pixel, 2017-2025.

Source: int8 COGs on source.coop (tge-labs/aef/v1/annual), nodata -128, de-quantised as sign(q) * (q / 127.5) ** 2.
Each pixel is a vector of about unit length, so a dot product of two vectors is their cosine similarity."""


try:
    import su_search
except ImportError:
    su_search = fused.load("su_search")


constants = su_search
geometry = su_search
catalog = su_search
time = su_search


INDICES = {
    "NDVI": ("B08", "B04"),
    "NDWI": ("B03", "B08"),
    "MNDWI": ("B03", "B11"),
    "NDMI": ("B08", "B11"),
    "NBR": ("B08", "B12"),
    "NDSI": ("B03", "B11"),
    "NDRE": ("B08", "B05"),
    "EVI": ("B08", "B04", "B02"),
    "SAVI": ("B08", "B04"),
    "BSI": ("B11", "B04", "B08", "B02"),
}


def normalized_difference(a, b):
    """Returns (a - b) / (a + b), NaN where a + b == 0; same type as a (numpy array or xarray)."""
    a, b = _float(a), _float(b)
    return _ratio(a - b, a + b)


def compute_index(ds, name):
    """Returns the index `name` (a key of INDICES) of a Dataset of reflectance bands; inputs are clipped at 0 first."""
    key = constants.check_choice("index", str(name).upper(), INDICES)
    bands = [_band(ds, b) for b in INDICES[key]]
    missing = [b for b, v in zip(INDICES[key], bands) if v is None]
    if missing:
        raise ValueError(f"{key} needs bands {list(INDICES[key])}; missing {missing}, the Dataset has {list(ds.data_vars)}")
    bands = [_float(v).clip(min=0) for v in bands]
    if key == "EVI":
        nir, red, blue = bands
        out = 2.5 * _ratio(nir - red, nir + 6 * red - 7.5 * blue + 1)
    elif key == "SAVI":
        nir, red = bands
        out = 1.5 * _ratio(nir - red, nir + red + 0.5)
    elif key == "BSI":
        swir, red, nir, blue = bands
        out = normalized_difference(swir + red, nir + blue)
    else:
        out = normalized_difference(*bands)
    return out.rename(key)


def required_bands(name):
    """Returns the band names the index `name` needs."""
    return list(INDICES[constants.check_choice("index", str(name).upper(), INDICES)])


def _band(ds, name):
    info = constants.band_info(name)
    return next((ds[k] for k in (info["name"], info["asset"], info["alias"]) if k in ds), None)


def _float(x):
    import numpy as np

    if not hasattr(x, "dtype"):
        return np.asarray(x, dtype="float64")
    return x.astype("float32") if x.dtype.kind in "iub" else x


def _ratio(num, den):
    import numpy as np

    with np.errstate(divide="ignore", invalid="ignore"):
        out = num / den
    if hasattr(out, "where"):
        return out.where(den != 0)
    return np.where(den != 0, out, np.nan)


SCALE = constants.SCALE
CLOUD_COVER = 80
DAYS = 30
CLOUD_CLASSES = constants.MASKS["cloud"]
ASSETS = tuple(b["asset"] for b in constants.S2_BANDS if b["asset"])
SPECTRAL = tuple(b["name"] for b in constants.S2_BANDS if b["wavelength"])
_M_PER_DEG_LAT = 110_540
_M_PER_DEG_LON = 111_320

PRESETS = {
    "true_color": (["red", "green", "blue"], 0.0, 0.3, 1.3),
    "false_color": (["nir", "red", "green"], 0.0, 0.5, 1.2),
    "false_color_urban": (["swir22", "swir16", "red"], 0.0, 0.4, 1.2),
    "swir": (["swir22", "nir08", "red"], 0.0, 0.5, 1.2),
    "agriculture": (["swir16", "nir", "blue"], 0.0, 0.5, 1.2),
    "geology": (["swir22", "swir16", "blue"], 0.0, 0.5, 1.2),
    "bathymetric": (["red", "green", "coastal"], 0.0, 0.2, 1.3),
}
INDEX_STYLE = {
    "NDVI": ("RdYlGn", -0.2, 0.9), "NDMI": ("RdYlGn", -0.2, 0.9), "NDRE": ("RdYlGn", -0.2, 0.9),
    "EVI": ("RdYlGn", -0.2, 0.9), "SAVI": ("RdYlGn", -0.2, 0.9), "NDWI": ("RdBu", -0.6, 0.6),
    "MNDWI": ("RdBu", -0.6, 0.6), "NBR": ("RdYlGn", -0.5, 0.8), "NDSI": ("Blues", -0.2, 1.0),
    "BSI": ("YlOrBr", -0.3, 0.3),
}

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

TILE_REGION_ZOOM = 8
TILE_LOW_ZOOM = 9
TILE_STOP_SHARE = {"high": 0.995, "low": 0.97}
TILE_MAX_ROUNDS = {"high": 6, "low": 3}
TILE_MAX_SCENES = 48
TILE_PER_TILE = 6
MOSAIC_MAX_VALUES = 50_000_000
EXPORT_MAX_PX = 4096
EXPORT_MAX_VALUES = 120_000_000
EXPORT_DIR = "fd://sentinel_utils/exports"


def read_stack(products, bands, geobox, *, units="reflectance", resampling="nearest", mosaicking="orbit",
               mosaic_order="least_cloudy", mask=None):
    """Returns an xarray Dataset of the bands (B04 ...) and dataMask on geobox: (time, y, x) per solar day for orbit, (y, x) for simple."""
    import numpy as np
    import odc.stac
    import pandas as pd
    import rasterio
    import xarray as xr

    constants.check_choice("units", units, constants.UNITS)
    constants.check_choice("resampling", resampling, constants.RESAMPLING)
    constants.check_choice("mosaicking", mosaicking, constants.MOSAICKING)
    constants.check_choice("mosaic_order", mosaic_order, constants.MOSAIC_ORDERS)
    if mask is not None:
        constants.check_choice("mask", mask, constants.MASKS)
    names = [b for b in constants.band_names(bands) if b != "dataMask"]
    if not names:
        raise ValueError("bands must name at least one band other than dataMask")
    load = names + (["SCL"] if (mask or {"CLD", "SNW"} & set(names)) and "SCL" not in names else [])
    asset = {n: constants.band_info(n)["asset"] for n in load}
    scenes = _scene_table(products)
    if len(scenes):
        scenes = scenes[scenes[[f"href_{asset[n]}" for n in load]].notna().all(axis=1)]
    if not len(scenes):
        raise constants.NoDataError(f"no scene has every band of {load}")
    scenes = scenes.sort_values(["datetime", "id"], kind="stable")
    n_values = geobox.shape[0] * geobox.shape[1] * len(scenes) * len(load)
    if n_values > constants.MAX_PIXELS:
        raise ValueError(f"{geobox.shape[0]}x{geobox.shape[1]} px x {len(scenes)} scenes x {len(load)} bands = "
                         f"{n_values:,} > MAX_PIXELS {constants.MAX_PIXELS:,}; use a coarser resolution, fewer scenes "
                         f"or a smaller area")
    spectral = [n for n in load if n in SPECTRAL]
    other = [n for n in load if n not in SPECTRAL]
    parts = []
    with rasterio.Env(**GDAL_ENV):
        for offset, group in scenes.groupby("offset"):
            items = _items(group, [asset[n] for n in load])
            got = []
            if spectral:
                ds = odc.stac.load(items, bands=[asset[n] for n in spectral], geobox=geobox, dtype="float32", nodata=0,
                                   resampling=resampling, groupby="id", pool=16, fail_on_error=False)
                got.append(ds.where(ds > 0) * SCALE + float(offset))
            if other:
                dm = odc.stac.load(items, bands=[asset[n] for n in other], geobox=geobox, dtype="uint16", nodata=0,
                                   resampling="nearest", groupby="id", pool=16, fail_on_error=False)
                got.append(dm.where(dm > 0).astype("float32"))
            part = xr.merge(got, compat="override").rename({asset[n]: n for n in load})
            part = part.rename({d: n for d, n in zip(geobox.dims, ("y", "x")) if d != n})
            if part.sizes["time"] != len(group):
                raise RuntimeError(f"odc-stac returned {part.sizes['time']} time steps for {len(group)} scenes")
            times = pd.to_datetime(group["datetime"], utc=True).dt.tz_localize(None).values
            part = part.assign_coords(time=times, scene=("time", list(group["id"])))
            for b in ("CLD", "SNW"):
                if b in part:
                    part[b] = part[b].fillna(0).where(part["SCL"].notnull())
            parts.append(part)
    ds = xr.concat(parts, dim="time") if len(parts) > 1 else parts[0]
    if mask:
        ok = ds["SCL"].isin(constants.MASKS[mask])
        ds = ds.assign({n: ds[n].where(ok) for n in names})
    ordered = _ordered(scenes, mosaic_order, geobox)
    ds = ds[names].isel(time=np.argsort([ordered.get(s, len(ordered)) for s in ds["scene"].values], kind="stable"))
    if mosaicking == "simple":
        valid = np.all([ds[n].notnull().values for n in names], axis=0)
        first = valid.argmax(axis=0)[None]
        has = valid.any(axis=0)
        out = xr.Dataset({n: (("y", "x"), np.where(has, np.take_along_axis(ds[n].values, first, 0)[0], np.nan))
                          for n in names}, coords={"y": ds["y"], "x": ds["x"]})
    else:
        bb = geobox.extent.to_crs("EPSG:4326").boundingbox
        out = _by_day(ds, lon=(bb.left + bb.right) / 2).sortby("time")
    out = out.astype("float32")
    out["dataMask"] = xr.concat([out[n].notnull() for n in names], "band").all("band").astype("uint8")
    if units == "dn":
        out = out.assign({n: (out[n] / SCALE).round().clip(1, 65535).fillna(0).astype("uint16")
                          for n in names if n in SPECTRAL})
    out = _odc(out).assign_crs(geobox.crs)
    out.attrs.update({"resolution": abs(geobox.resolution.x), "crs": str(geobox.crs), "scenes": len(scenes),
                      "units": units, "mosaicking": mosaicking})
    return out


def tile(z, x, y, date, *, days=DAYS, what="true_color", method="best", cloud_cover=CLOUD_COVER,
         per_tile=TILE_PER_TILE, order="clear", cloud_mask=True, size=256, cmap=None, vmin=None, vmax=None, min_zoom=6):
    """Returns one uint8 RGBA (4, size, size) map tile of `what` (a preset or an index) around `date` (+- days)."""
    import numpy as np
    import xarray as xr

    empty = np.zeros((4, int(size), int(size)), dtype="uint8")
    if int(z) < int(min_zoom):
        return empty
    bands = _needs(what)
    scenes = tile_scenes(z, x, y, date, days=days, cloud_cover=cloud_cover, per_tile=per_tile, order=order)
    if not len(scenes):
        return empty
    level = "low" if int(z) < TILE_LOW_ZOOM else "high"
    data, _ = mosaic(scenes, bands, tile_geobox(z, x, y, size), cloud_mask=cloud_mask, method=method,
                     stop=TILE_STOP_SHARE[level], max_rounds=TILE_MAX_ROUNDS[level], max_scenes=TILE_MAX_SCENES)
    if np.isnan(data).all():
        return empty
    return render(xr.Dataset({n: (("y", "x"), data[i]) for i, n in enumerate(bands)}), what, cmap=cmap, vmin=vmin, vmax=vmax)


def tile_scenes(z, x, y, date, *, days=DAYS, cloud_cover=CLOUD_COVER, per_tile=TILE_PER_TILE, order="clear"):
    """Returns the products that tile z/x/y reads, best first (ranked per zoom-8 region)."""
    import mercantile
    import shapely

    t = mercantile.Tile(int(x), int(y), int(z))
    region = mercantile.bounds(mercantile.parent(t, zoom=TILE_REGION_ZOOM) if t.z > TILE_REGION_ZOOM else t)
    rbox = [round(v, 6) for v in region]
    products = catalog.query(rbox, _date_window(date, days), cloud_cover=float(cloud_cover))
    ranked = catalog.rank(products, rbox, date=date, per_tile=per_tile, order=order)
    return ranked[ranked.intersects(shapely.box(*mercantile.bounds(t)))] if len(ranked) else ranked


def tile_geobox(z, x, y, size=256):
    """Returns the Web Mercator GeoBox of map tile z/x/y with size x size pixels."""
    import mercantile
    from odc.geo.geobox import GeoBox

    b = mercantile.xy_bounds(int(x), int(y), int(z))
    return GeoBox.from_bbox((b.left, b.bottom, b.right, b.top), crs="EPSG:3857", shape=(int(size), int(size)))


def bounds_to_tile(bounds):
    """Returns the Web Mercator tile (z, x, y) of tile bounds [w, s, e, n], as a Fused tile UDF gets them."""
    import math
    import mercantile

    w, s, e, n = [float(v) for v in bounds]
    t = mercantile.tile((w + e) / 2, (s + n) / 2, max(0, int(round(math.log2(360.0 / max(e - w, 1e-9))))))
    return t.z, t.x, t.y


def mosaic(scenes, bands, gb, *, cloud_mask=True, method="best", stop=0.995, max_rounds=6, max_scenes=TILE_MAX_SCENES,
           keep_obs=True, key_band=0, threads=32):
    """Returns (reflectance float32 (n, H, W), report) of ranked products on the north-up grid gb; the first clear pixel wins."""
    from concurrent.futures import ThreadPoolExecutor
    import warnings
    import numpy as np

    if gb.affine.e > 0 or gb.affine.b or gb.affine.d:
        raise ValueError("mosaic needs a north-up grid without rotation")
    scenes = _scene_table(scenes)
    bands = list(bands)
    stat = method in ("mean", "median") or (method[:1] == "p" and method[1:].isdigit() and int(method[1:]) <= 100)
    if method != "best" and not stat:
        raise ValueError("method must be best, median, mean or pNN")
    if method == "best":
        if "tile_rank" not in scenes:
            raise ValueError("method best needs ranked products: pass them through catalog.rank() first")
        groups = [g for _, g in scenes.groupby("tile_rank", sort=True)][:int(max_rounds)]
    else:
        groups = [scenes]
    kept, n = [], 0
    for g in groups:
        g = g.head(max(0, int(max_scenes) - n))
        if len(g):
            kept.append(g)
            n += len(g)
    fill = _BestFill(len(bands), gb.shape, keep_obs=keep_obs, key_band=key_band)
    later = reach(kept, gb) if method == "best" else None
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
            if later is not None and k + 1 < len(kept) and (later[k + 1] & ~fill.filled).mean() <= 1 - float(stop):
                break
    data, alpha = fill.result()
    if stat and stack:
        arr = np.stack(stack)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            out = np.nanmean(arr, axis=0).astype("float32") if method == "mean" else _nanquantile(arr, 0.5 if method == "median" else int(method[1:]) / 100)
        holes = np.isnan(out).any(0)
        out[:, holes] = data[:, holes]
        data = out
    data[:, ~alpha] = np.nan
    return data, {"scenes": used, "clear_pct": round(fill.clear_share() * 100, 1), "rounds": rounds}


def read_scenes(scenes, bands, gb, *, cloud_mask=True, pool=None):
    """Returns one (scene id, (reflectance, valid, clear) or None) per product, read on the grid gb."""
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np

    scenes = _scene_table(scenes)
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
            clear = valid & ~np.isin(scl, CLOUD_CLASSES) if scl is not None else np.zeros_like(valid)
        else:
            clear = valid
        out.append((r["id"], (np.where(valid[None], raw * SCALE + float(r["offset"]), np.nan).astype("float32"), valid, clear)))
    return out


def export(area, date, *, days=DAYS, kind="rgb", what="true_color", bands="blue,green,red,nir", method="best",
           cloud_mask=True, cloud_cover=CLOUD_COVER, per_tile=TILE_PER_TILE, max_px=EXPORT_MAX_PX, resolution=None,
           crs="utm", cmap=None, vmin=None, vmax=None, out_dir=EXPORT_DIR):
    """Returns a dict (url, path, width, height, res_m, scenes, clear_pct, seconds) of one COG of `area` around `date` (+- days)."""
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
        raise ValueError(f"max_px is limited to {EXPORT_MAX_PX}; for larger areas split the area (su_analysis)")
    if kind == "rgb":
        names = _needs(what)
    else:
        canonical = constants.band_names(bands)
        if any(n not in SPECTRAL for n in canonical):
            raise ValueError(f"kind bands writes reflectance; use only bands of {list(SPECTRAL)}")
        names = [constants.band_info(n)["asset"] for n in canonical]
    bounds = list(geometry.to_bbox(area))
    res, gb = _export_grid(bounds, resolution, crs, max_px)
    if gb.shape[0] * gb.shape[1] * (len(names) + 1) > EXPORT_MAX_VALUES:
        raise ValueError(f"{gb.shape[0]}x{gb.shape[1]} px x {len(names) + 1} bands is too large; raise resolution or lower max_px")
    key = hashlib.sha1(json.dumps([bounds, _day(date), days, kind, what, names, method, cloud_mask, cloud_cover, per_tile,
                                   res, str(crs), cmap, vmin, vmax], default=str).encode()).hexdigest()[:12]
    products = catalog.query(bounds, _date_window(date, days), cloud_cover=float(cloud_cover))
    scenes = catalog.rank(products, bounds, date=date, per_tile=per_tile)
    if not len(scenes):
        raise constants.NoDataError("no scenes for this box and window")
    px = gb.shape[0] * gb.shape[1]
    opts = dict(cloud_mask=cloud_mask, keep_obs=False, key_band=names.index("blue") if "blue" in names else 0,
                threads=max(4, min(32, EXPORT_MAX_VALUES // px)))
    if method == "best":
        data, rep = mosaic(scenes, names, gb, stop=0.97 if res >= 100 else 0.995, max_rounds=per_tile,
                           max_scenes=len(scenes), **opts)
    else:
        data, rep = mosaic(scenes, names, gb, method=method, max_scenes=max(1, EXPORT_MAX_VALUES // (px * len(names))), **opts)
    if np.isnan(data).all():
        raise constants.NoDataError("no scene has data in this box")
    used, clear = rep["scenes"], rep["clear_pct"]
    local = os.path.join(tempfile.mkdtemp(), f"s2_{kind}_{key}.tif")
    if kind == "rgb":
        rgba = render(xr.Dataset({n: (("y", "x"), data[i]) for i, n in enumerate(names)}), what, cmap=cmap, vmin=vmin, vmax=vmax)
        _write_tif(local, gb, rgba, colorinterp="rgba")
    else:
        dn = np.where(np.isnan(data), 0, np.clip(np.round(data / SCALE + 1000), 1, 65535)).astype("uint16")
        _write_tif(local, gb, dn, nodata=0, names=canonical, scale=SCALE, offset=-0.1)
    path = _save_file(local, f"{str(out_dir).rstrip('/')}/{os.path.basename(local)}")
    url = fused.api.sign_url(fused.api.resolve(path)) if path.startswith(("fd://", "s3://")) else None
    return {"url": url, "path": path, "kind": kind, "what": what if kind == "rgb" else ",".join(canonical),
            "width": int(gb.shape[1]), "height": int(gb.shape[0]), "res_m": res,
            "crs": f"EPSG:{gb.crs.epsg}" if gb.crs.epsg else str(gb.crs), "mb": round(os.path.getsize(local) / 1e6, 2),
            "scenes": ",".join(used), "clear_pct": clear, "seconds": round(time.time() - t0, 1)}


def to_cog(data, path, *, nodata=None):
    """Returns `path` after it writes a 2-D DataArray, or a Dataset of 2-D variables, as a COG (fd:// and s3:// upload)."""
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
        raise ValueError("data has no CRS; set one with .odc.assign_crs")
    arr = np.stack([np.asarray(a.transpose("y", "x").values) for a in arrs])
    local = os.path.join(tempfile.mkdtemp(), os.path.basename(path))
    _write_tif(local, _odc(data).geobox, arr, nodata=nodata, names=names)
    return _save_file(local, path)


def render(ds, what="true_color", *, cmap=None, vmin=None, vmax=None, gamma=None):
    """Returns uint8 RGBA (4, H, W) of a preset or an index of a 2-D Dataset; alpha is 0 where there is no data."""
    import numpy as np

    if "time" in ds.dims:
        raise ValueError("render needs one image: reduce the time axis first")
    if what in PRESETS:
        names, lo, hi, gm = PRESETS[what]
        layers = [_var(ds, n) for n in names]
        missing = [n for n, v in zip(names, layers) if v is None]
        if missing:
            raise ValueError(f"{what} needs bands {missing}")
        lo, hi = lo if vmin is None else vmin, hi if vmax is None else vmax
        rgb = np.stack([stretch(v, lo, hi, gamma or gm).values for v in layers])
        alpha = np.all([v.notnull().values for v in layers], axis=0)
        return np.concatenate([rgb, (alpha * 255).astype("uint8")[None]])
    _needs(what)
    s_cmap, s_min, s_max = render_style(what)
    return colorize(compute_index(ds, what), cmap or s_cmap, s_min if vmin is None else vmin,
                    s_max if vmax is None else vmax)


def render_style(what):
    """Returns (cmap, vmin, vmax) of an index from INDEX_STYLE; (viridis, None, None) for other names."""
    return INDEX_STYLE.get(str(what).upper(), ("viridis", None, None))


def stretch(da, lo, hi, gamma=1.0):
    """Returns uint8 values after a linear stretch from lo to hi and a gamma; NaN gives 0."""
    if hi == lo:
        raise ValueError("stretch needs lo != hi")
    x = ((da - lo) / (hi - lo)).clip(0, 1)
    if gamma != 1.0:
        x = x ** (1.0 / gamma)
    return (x * 255).fillna(0).astype("uint8")


def colorize(da, cmap="viridis", vmin=None, vmax=None):
    """Returns uint8 RGBA (4, H, W) of 2-D values through a matplotlib colour map; vmin and vmax default to the 2nd and 98th percentile."""
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


def css_gradient(cmap, n=6):
    """Returns the CSS linear-gradient of a matplotlib colour map (for legends in pages)."""
    import matplotlib

    n = max(int(n), 2)
    stops = ",".join(matplotlib.colors.to_hex(matplotlib.colormaps[cmap](i / (n - 1))) for i in range(n))
    return f"linear-gradient(90deg,{stops})"


def _scene_table(products):
    import pandas as pd

    if "assets" not in products.columns:
        return products
    hrefs = [a if isinstance(a, dict) else {} for a in products["assets"]]
    out = products.drop(columns="assets").reset_index(drop=True)
    return out.assign(epsg=pd.to_numeric(out["crs"].astype(str).str.split(":").str[-1], errors="coerce"),
                      **{f"href_{a}": [h.get(a) for h in hrefs] for a in ASSETS})


def _needs(what):
    allowed = list(PRESETS) + list(INDICES)
    if what in PRESETS:
        return list(PRESETS[what][0])
    name = constants.check_choice("what", str(what).upper(), allowed)
    return [constants.band_info(b)["asset"] for b in required_bands(name)]


def _var(ds, asset):
    info = constants.band_info(asset)
    return next((ds[k] for k in (asset, info["name"]) if k in ds), None)


def _ordered(scenes, mosaic_order, geobox):
    if mosaic_order == "clearest_day":
        if "day_clear_pct" not in scenes:
            scenes = catalog.rank(scenes, geobox.extent.to_crs("EPSG:4326").geom, order="day")
        sc = scenes.sort_values(["day_clear_pct", "date", "cloud_cover", "datetime", "id"],
                                ascending=[False, True, True, True, True], kind="stable")
    elif mosaic_order == "least_cloudy":
        sc = scenes.sort_values(["cloud_cover", "datetime", "id"], kind="stable")
    else:
        sc = scenes.sort_values(["datetime", "id"], ascending=mosaic_order == "oldest", kind="stable")
    return {sid: i for i, sid in enumerate(sc["id"])}


def _date_window(date, days=DAYS):
    import pandas as pd

    d = pd.Timestamp(_day(date))
    half = pd.Timedelta(days=int(days))
    return (d - half).strftime("%Y-%m-%d"), (d + half).strftime("%Y-%m-%d")


def _day(value):
    import pandas as pd

    try:
        return pd.Timestamp(str(value)[:10]).strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError(f"dates must be YYYY-MM-DD, got {value!r}") from None


def _size_m(bounds):
    import math

    lat = math.radians((bounds[1] + bounds[3]) / 2)
    return (bounds[2] - bounds[0]) * _M_PER_DEG_LON * math.cos(lat), (bounds[3] - bounds[1]) * _M_PER_DEG_LAT


def _auto_res(bounds, max_px, min_res=10):
    import math

    return max(float(min_res), math.ceil(max(_size_m(bounds)) / int(max_px) / 10) * 10)


def _export_grid(bounds, res, crs, max_px):
    auto = not res
    res = _auto_res(bounds, max_px) if auto else float(res)
    gb = geometry.make_geobox(bounds, resolution=res, crs=crs)
    while auto and max(gb.shape) > int(max_px):
        res += 10
        gb = geometry.make_geobox(bounds, resolution=res, crs=crs)
    if max(gb.shape) > int(max_px):
        raise ValueError(f"{gb.shape[0]}x{gb.shape[1]} px is larger than max_px {max_px}; raise resolution")
    return res, gb


def _odc(obj):
    import odc.geo.xr

    return obj.odc


def _save_file(local, path):
    import shutil

    if str(path).startswith(("fd://", "s3://")):
        fused.api.upload(local, path)
    elif local != path:
        shutil.copy(local, path)
    return path


def _nanquantile(a, q, axis=0):
    import numpy as np

    a = np.moveaxis(np.asarray(a, dtype="float32"), axis, 0)
    n = (~np.isnan(a)).sum(0)
    srt = np.sort(a, axis=0)
    pos = float(q) * np.clip(n - 1, 0, None)
    lo, hi = np.floor(pos).astype("int64"), np.ceil(pos).astype("int64")
    vlo = np.take_along_axis(srt, lo[None], 0)[0]
    vhi = np.take_along_axis(srt, hi[None], 0)[0]
    return np.where(n > 0, vlo + (vhi - vlo) * (pos - lo), np.nan).astype("float32")


class _BestFill:
    """Best-first pixel fill of tiles and exports: the first clear value of a pixel wins."""

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
        """Returns the share of the pixels with data that are clear (0.0 when no pixel has data)."""
        return float(self.filled[self.seen].mean()) if self.seen.any() else 0.0

    def result(self):
        """Returns (data float32 (nb, H, W), alpha bool (H, W)); data is NaN where no observation has data."""
        import numpy as np

        holes = ~self.filled & self.seen
        if holes.any():
            if self.keep_obs:
                self.out[:, holes] = _nanquantile(np.stack([o[:, holes] for o in self.obs]), 0.5)
            else:
                self.out[:, holes] = self.dark_val[:, holes]
        return self.out, self.filled | self.seen


def reach(groups, gb):
    """Returns one bool (H, W) mask per group of products: pixels that group k or a later group covers (footprint - 2 px)."""
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
    return out.rename(day="time")


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


REDUCERS = ("median", "mean", "min", "max", "count")
SIMPLE_STOP = 0.995
SIMPLE_BATCH = 16
DEFAULT_BANDS = {constants.SENTINEL2_L2A: ("B04", "B03", "B02"), constants.DEM: ("DEM",),
                 constants.AEF: constants.DATA_COLLECTIONS[constants.AEF]["bands"]}
DEM_ENV = {**GDAL_ENV, "AWS_REGION": "eu-central-1"}
_FIELDS = ("area", "date", "bands", "collection", "resolution", "size", "crs", "mosaicking", "mosaic_order",
           "cloud_cover", "units", "resampling", "mask", "index", "reducer", "output")


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
        index = constants.check_choice("index", str(index).upper(), INDICES)
    dem = collection == constants.DEM
    s2 = collection == constants.SENTINEL2_L2A
    if not s2 and (mask or index or reducer):
        raise ValueError(f"a {collection} request takes no mask, index or reducer")
    if bands is None:
        bands = required_bands(index) if index else DEFAULT_BANDS[collection]
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
    date = str(embedding_year(date)) if collection == constants.AEF else _date_json(date, dem)
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
                         f"{constants.MAX_PIXELS:,}; use a coarser resolution or split the area (su_analysis)")
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
        ds = load_embeddings(req["area"], req["date"], geobox=gb).to_dataset("band")[names]
        ds["dataMask"] = ds.to_array().notnull().all("variable").astype("uint8")
    else:
        load = names + [b for b in (required_bands(req["index"]) if req["index"] else []) if b not in names]
        products = catalog.query(req["area"], _tuple(req["date"]), cloud_cover=_tuple(req["cloud_cover"]))
        if not len(products):
            raise constants.NoDataError(f"no Sentinel-2 scene for area {req['area']} and date {req['date']}")
        if req["reducer"]:
            ds = read_stack(_clearest(products, load, gb, req), load, gb, resampling=req["resampling"],
                                   mosaic_order=req["mosaic_order"], mask=req["mask"])
            ds = _reduce(ds, load, req["reducer"], req["units"])
        elif req["mosaicking"] == "orbit":
            ds = read_stack(products, load, gb, units=req["units"], resampling=req["resampling"],
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
                  f"{constants.MAX_PIXELS:,}; the reducer uses the {n} clearest scenes (su_search.rank)")
    return catalog.rank(products, req["area"], order="clear").head(n)


def _order(products, req):
    if req["mosaic_order"] in ("newest", "oldest"):
        return products.sort_values(["datetime", "id"], ascending=req["mosaic_order"] == "oldest", kind="stable")
    return catalog.rank(products, req["area"], order="day" if req["mosaic_order"] == "clearest_day" else "clear")


def _simple(products, names, gb, req):
    import warnings

    scenes = _order(products, req)
    last = min(len(scenes), _max_scenes(names, gb, req["mask"]))
    stops, size = [0], (scenes["date"].isin(scenes["date"].head(1)).sum() if req["mosaic_order"] == "clearest_day"
                        else scenes["tile_id"].nunique())
    while stops[-1] < last:
        stops.append(min(stops[-1] + size, last))
        size = max(size, min(2 * size, SIMPLE_BATCH))
    groups = [scenes.iloc[a:b] for a, b in zip(stops, stops[1:])] + ([scenes.iloc[last:]] if last < len(scenes) else [])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        later = reach(groups, gb)
    out, used = None, 0
    for k in range(len(stops) - 1):
        try:
            part = read_stack(groups[k], names, gb, units=req["units"], resampling=req["resampling"],
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
                              for n in names if n in SPECTRAL})
    out["dataMask"] = xr.concat([data[n].notnull().any("time") for n in names], "band").all("band").astype("uint8")
    out.attrs.update(stack.attrs)
    return out


def _index(ds, name, units):
    src = ds
    if units == "dn":
        src = ds.assign({b: ds[b].where(ds[b] > 0) * constants.SCALE for b in required_bands(name)
                         if b in SPECTRAL})
    return compute_index(src, name).astype("float32").where(ds["dataMask"] > 0)


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
    path = to_cog(ds if ints else ds.astype("float32"), os.path.join(tempfile.mkdtemp(), "process.tif"),
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

    if name in SPECTRAL:
        lo, hi = 0.0, 10000.0 if units == "dn" else 1.0
    elif name in INDICES:
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


AEF_INDEX = "https://data.source.coop/tge-labs/aef/v1/annual/aef_index.parquet"
AEF_YEARS = range(2017, 2026)
AEF_FEATURES = constants.DATA_COLLECTIONS[constants.AEF]["bands"]
AEF_NODATA = -128
AEF_ENV = dict(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
               GDAL_HTTP_MULTIPLEX="YES", GDAL_INGESTED_BYTES_AT_OPEN="32768", VSI_CACHE="TRUE",
               VSI_CACHE_SIZE="50000000", AWS_NO_SIGN_REQUEST="YES", AWS_REGION="us-west-2")


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


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return su_search.api_table(globals())
