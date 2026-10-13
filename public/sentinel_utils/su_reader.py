"""Pixel engine: map tiles, best-pixel mosaics, COG export and read_stack, on the products table of su_catalog."""

try:
    import su_constants as constants
    import su_geometry as geometry
    import su_catalog as catalog
    import su_indices as indices
except ImportError:
    constants = fused.load("su_constants")
    geometry = fused.load("su_geometry")
    catalog = fused.load("su_catalog")
    indices = fused.load("su_indices")

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


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


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
    ordered = _ordered(scenes, mosaic_order)
    ds = ds[names].isel(time=np.argsort([ordered[s] for s in ds["scene"].values], kind="stable"))
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
        raise ValueError(f"max_px is limited to {EXPORT_MAX_PX}; for larger areas split the area (su_areas)")
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
    return colorize(indices.compute_index(ds, what), cmap or s_cmap, s_min if vmin is None else vmin,
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
    allowed = list(PRESETS) + list(indices.INDICES)
    if what in PRESETS:
        return list(PRESETS[what][0])
    name = constants.check_choice("what", str(what).upper(), allowed)
    return [constants.band_info(b)["asset"] for b in indices.required_bands(name)]


def _var(ds, asset):
    info = constants.band_info(asset)
    return next((ds[k] for k in (asset, info["name"]) if k in ds), None)


def _ordered(scenes, mosaic_order):
    if mosaic_order == "least_cloudy":
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
