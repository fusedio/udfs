"""Tests for public/sentinel_utils. Each test asserts a known result.

    pytest tests/test_sentinel_utils.py                          fast tests: synthetic data, no Sentinel-2 reads
    SENTINEL_UTILS_LIVE=1 pytest tests/test_sentinel_utils.py    also the live tests: real Sentinel-2 data

Fast tests need numpy, pandas, xarray, odc-geo and scipy. They download the spectral index catalog once.
Live tests also need the packages of the remote engine (odc-stac, pystac, rasterio, rio-tiler, duckdb, geopandas,
mercantile, matplotlib, pillow) and a Fused login: the export writes to your Fused disk and the fan-out test runs
workers with fused.submit.
"""

import os
import warnings

import fused
import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")
pytest.importorskip("odc.geo.xr")
pytest.importorskip("scipy")

UDF_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "public", "sentinel_utils")
BOX = [-118.62, 34.02, -118.50, 34.10]
live = pytest.mark.skipif(os.environ.get("SENTINEL_UTILS_LIVE") != "1", reason="set SENTINEL_UTILS_LIVE=1 to read real data")


@pytest.fixture(scope="module")
def s2():
    return fused.load(UDF_PATH)


def synth(values, times=None, crs="EPSG:32611"):
    """A small (time, y, x) Dataset on a 10 m UTM grid from {band: array}."""
    first = np.asarray(next(iter(values.values())))
    ny, nx = first.shape[-2:]
    coords = {"y": 3_770_000.0 - 10 * np.arange(ny) - 5, "x": 360_000.0 + 10 * np.arange(nx) + 5}
    dims = ("y", "x")
    if first.ndim == 3:
        coords["time"] = pd.to_datetime(times if times is not None else pd.date_range("2024-01-01", periods=first.shape[0], freq="10D"))
        dims = ("time", "y", "x")
    return xr.Dataset({k: (dims, np.asarray(v, dtype="float32")) for k, v in values.items()}, coords=coords).odc.assign_crs(crs)


def test_bands_and_places(s2):
    assert s2.band_name("B04") == "red" and s2.band_name("b8a") == "nir08" and s2.band_name("S2") == "swir22"
    assert s2.band_name("SCL") == "scl" and s2.band_names("red,B04,nir") == ["red", "nir"]
    assert s2.bbox("1,2,3,4") == [1.0, 2.0, 3.0, 4.0]
    assert s2.date_window("2024-07-01", 30) == ("2024-06-01", "2024-07-31")
    assert s2.bounds_to_tile([-73.9893, 40.7638, -73.9453, 40.7971]) == (13, 2412, 3078)
    with pytest.raises(ValueError):
        s2.bbox("3,2,1,4")


def test_formulas(s2):
    for bad in ["__import__('os')", "N.__class__", "(lambda: 1)()", "[x for x in N]", "open('x')", "'a'", "N if True else R"]:
        with pytest.raises((ValueError, SyntaxError)):
            s2.parse_formula(bad)
    rng = np.random.default_rng(0)
    ds = synth({b[0]: rng.uniform(0.02, 0.5, (6, 6)) for b in s2.BANDS})
    letters = {b[2]: b[0] for b in s2.BANDS}
    catalog, const = s2.index_catalog()
    env = {L: ds[n].values.astype("float64") for L, n in letters.items()}
    env.update({k: v for k, v in const.items() if v is not None})
    env.update({f"lambda{L}": float(s2.band_info(n)["wavelength"]) for L, n in letters.items()})
    checked = 0
    for name, entry in catalog.items():
        try:
            mine = s2.index(ds, name).values
        except ValueError:
            continue
        with np.errstate(all="ignore"):
            ref = eval(entry["formula"], {}, env)
        assert np.allclose(mine, ref, rtol=1e-3, atol=1e-5, equal_nan=True), name
        checked += 1
    assert checked >= 230, checked
    assert s2.needs("true_color") == ["red", "green", "blue"] and s2.needs("NDVI") == ["nir", "red"]
    wet = s2.index(ds.assign(green=ds["green"].where(ds["green"] > 0.1)), "(G - S1) / (G + S1) > 0")
    assert int(wet.isnull().sum()) == int((ds["green"] <= 0.1).sum()), "a comparison must stay NaN where inputs are NaN"


def test_composite_methods(s2):
    rng = np.random.default_rng(1)
    red = rng.uniform(0.05, 0.2, (5, 4, 4))
    nir = rng.uniform(0.2, 0.5, (5, 4, 4))
    red[2, 0, 0] = np.nan
    ds = synth({"red": red, "nir": nir})
    v = {m: float(s2.composite(ds, m)["red"].mean()) for m in ("min", "p25", "median", "max")}
    assert v["min"] <= v["p25"] <= v["median"] <= v["max"], v
    assert np.allclose(s2.composite(ds, "p50")["red"].values, s2.composite(ds, "median")["red"].values, equal_nan=True)
    first = s2.composite(ds, "first")["red"].values
    assert first[0, 0] == np.float32(red[0, 0, 0]) and first[1, 1] == np.float32(red[0, 1, 1])
    best = s2.composite(ds, "max:NDVI")
    ndvi = (nir - red) / (nir + red)
    assert np.allclose(best["nir"].values, np.take_along_axis(nir, np.nanargmax(ndvi, 0)[None], 0)[0])
    assert int(s2.clear_count(ds).values[0, 0]) == 4


def test_seasons_and_periods(s2):
    times = pd.to_datetime(["2023-12-10", "2024-01-15", "2024-02-20", "2024-03-10"])
    out = s2.composite(synth({"red": np.ones((4, 2, 2))}, times=times), "count", "season")
    assert list(pd.to_datetime(out["time"].values).strftime("%Y-%m")) == ["2023-12", "2024-03"]
    labels = [p[0] for p in s2.period_ranges("2023-12-01", "2024-11-30", "season")]
    assert labels == ["2023 DJF", "2024 MAM", "2024 JJA", "2024 SON"]


def test_time_functions(s2):
    s = pd.Series([1.0, np.nan, np.nan, np.nan, np.nan, 2.0, np.nan, 3.0], index=pd.date_range("2024-01-01", periods=8, freq="10D"))
    f = s2.fill_gaps(s, max_gap="30D")
    assert f.iloc[1:5].isna().all() and f.iloc[6] == 2.5, f.tolist()
    d = pd.date_range("2023-01-03", "2023-12-28", freq="5D")
    doy = d.dayofyear.values
    curve = 0.2 + 0.6 * np.exp(-((doy - 90) / 25) ** 2) + 0.55 * np.exp(-((doy - 220) / 30) ** 2)
    ph = s2.phenology(pd.Series(curve, index=d))
    assert len(ph) == 2 and abs(ph["peak_doy"].iloc[0] - 90) <= 10 and abs(ph["peak_doy"].iloc[1] - 220) <= 10
    weekly = pd.date_range("2021-01-03", "2024-12-28", freq="7D")
    _, coef = s2.harmonic(pd.Series(0.5 + 0.2 * np.cos(2 * np.pi * (weekly.dayofyear - 200) / 365.25), index=weekly), n=1)
    assert abs(coef["amplitude_1"] - 0.2) < 0.01 and abs(coef["phase_1"] - 200) < 5
    months = pd.date_range("2019-01-01", "2023-12-01", freq="MS")
    vals = np.stack([np.full((2, 2), 0.5 + 0.1 * np.sin(m.month)) for m in months])
    vals[[i for i, m in enumerate(months) if m.month == 1 and m.year != 2022]] = np.nan
    z = s2.anomaly(synth({"v": vals}, times=months)["v"], [2019, 2020, 2021, 2023], target_years=[2022])
    assert z.sel(time="2022-01").isnull().all() and z.sizes["time"] == 12


def test_mask_dilation(s2):
    scl = np.full((1, 5, 5), 4.0)
    scl[0, 2, 2] = 9
    scl[0, :, 0] = 6
    ok = s2.clear_mask(synth({"red": np.ones((1, 5, 5)), "scl": scl}), "land", dilate=1, rescue=False).values[0]
    assert not ok[1:4, 1:4].any() and ok[0, 1] and ok[4, 4] and not ok[:, 0].any() and ok[2, 4]


def test_feature_names(s2):
    ds = synth({"red": np.ones((2, 2, 2)), "nir": np.ones((2, 2, 2))}, times=pd.to_datetime(["2024-06-01", "2024-06-15"]))
    names = list(s2.features(ds)["feature"].values)
    assert len(set(names)) == 4 and names[0] == "red_2024-06-01", names


def test_worker_udf(s2):
    for name, age in [("block_worker", 0), ("tile_worker", 0), ("frame_worker", 7 * 86400)]:
        w = s2.worker_udf(name)
        assert w.entrypoint == name and w.cache_max_age == age and "def worker_udf(" in w.code


@live
def test_search_sources_agree(s2):
    a = s2.search(BOX, "2023-06-01", "2023-08-31", max_cloud=20, source="c1")
    b = s2.search(BOX, "2023-06-01", "2023-08-31", max_cloud=20, source="coop")
    assert len(a) and sorted(a["id"]) == sorted(b["id"]), (len(a), len(b))


@live
def test_gap_fill(s2):
    sc = s2.search([67.6, 26.3, 67.9, 26.6], "2022-07-01", "2022-11-30", max_cloud=30)
    assert len(sc) and (sc["source"] == "legacy").mean() > 0.9, sc["source"].value_counts().to_dict()


@live
def test_offset(s2):
    vals = {}
    for src in ("c1", "legacy"):
        sc = s2.search(BOX, "2023-06-22", "2023-06-22", max_cloud=100, source=src)
        vals[src] = float(s2.load(sc, "red,nir", BOX, res=60)["nir"].median())
    assert abs(vals["c1"] - vals["legacy"]) < 0.002, vals


@live
def test_json_roundtrip(s2):
    sc = s2.rank(s2.search(BOX, "2024-06-01", "2024-06-30"), per_mgrs=2)
    back = s2.from_json(s2.to_json(sc, "red,nir,scl"))
    assert list(back["id"]) == list(sc["id"]) and back["href_red"].notna().all()
    assert back["offset"].tolist() == sc["offset"].tolist()
    assert len(s2.from_json(s2.to_json(sc.iloc[:0], "red"))) == 0


@live
def test_fanout_reports_errors(s2):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        df = s2.run_fanout("block_worker", [{"name": "empty", "scenes_json": "[]"}, {"name": "bad", "scenes_json": "{"}], max_workers=2)
    assert len(df) == 1 and df["name"].iloc[0] == "empty"
    assert any("1 of 2 failed" in str(x.message) for x in w), [str(x.message) for x in w]


@live
def test_tile(s2):
    rgba = s2.tile(13, 2412, 3078, "2024-07-01")
    assert rgba.shape == (4, 256, 256) and (rgba[3] > 0).mean() > 0.99


@live
def test_tile_layer_remote():
    rgba = fused.run(fused.load(UDF_PATH), x=2412, y=3078, z=13, engine="remote")
    assert np.asarray(rgba).shape[-2:] == (256, 256)


@live
def test_export_bands(s2):
    import rasterio

    info = s2.export([-74.00, 40.75, -73.98, 40.77], "2024-07-01", kind="bands", bands="red,nir")
    with rasterio.Env(**s2.GDAL_ENV), rasterio.open(info["url"]) as src:
        nir = src.read(2)
        assert src.scales == (0.0001, 0.0001) and src.offsets == (-0.1, -0.1) and src.nodata == 0
        assert src.res == (10.0, 10.0) and nir[nir > 0].mean() > 1000


@live
def test_embeddings(s2):
    e = s2.load_embeddings([-93.56, 41.97, -93.54, 41.99], 2024, res=20)
    norm = np.sqrt(np.nansum(e.values ** 2, axis=0))
    assert (norm > 0).any() and abs(float(np.median(norm[norm > 0])) - 1) < 0.02
