"""Tests for public/sentinel_utils. Each test asserts a known result.

    pytest tests/test_sentinel_utils.py                          fast tests: synthetic data, no Sentinel-2 reads
    SENTINEL_UTILS_LIVE=1 pytest tests/test_sentinel_utils.py    also the live tests: real Sentinel-2 data

Fast tests need numpy, pandas, xarray, odc-geo, scipy, scikit-learn, matplotlib, pillow and pyproj. They download
the spectral index catalog once.
Live tests also need the packages of the remote engine (odc-stac, pystac, rasterio, rio-tiler, duckdb, geopandas,
mercantile) and a Fused login: the export writes to your Fused disk and the fan-out test runs workers with
fused.submit.
"""

import os
import shutil
import tempfile
import warnings

import fused
import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")
pytest.importorskip("odc.geo.xr")
pytest.importorskip("scipy")
gpd = pytest.importorskip("geopandas")
shapely = pytest.importorskip("shapely")

UDF_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "public", "sentinel_utils")
BOX = [-118.62, 34.02, -118.50, 34.10]
live = pytest.mark.skipif(os.environ.get("SENTINEL_UTILS_LIVE") != "1", reason="set SENTINEL_UTILS_LIVE=1 to read real data")


@pytest.fixture(scope="module")
def s2():
    return fused.load(UDF_PATH)


def synth(values, times=None, crs="EPSG:32611", res=10.0):
    """A small (time, y, x) or (y, x) Dataset on a regular grid from {band: array}."""
    first = np.asarray(next(iter(values.values())))
    ny, nx = first.shape[-2:]
    y0, x0 = (3_770_000.0, 360_000.0) if crs != "EPSG:4326" else (34.1, -118.6)
    coords = {"y": y0 - res * np.arange(ny) - res / 2, "x": x0 + res * np.arange(nx) + res / 2}
    dims = ("y", "x")
    if first.ndim == 3:
        coords["time"] = pd.to_datetime(times if times is not None else pd.date_range("2024-01-01", periods=first.shape[0], freq="10D"))
        dims = ("time", "y", "x")
    return xr.Dataset({k: (dims, np.asarray(v, dtype="float32")) for k, v in values.items()}, coords=coords).odc.assign_crs(crs)


def scene_table(rows):
    """A ranked scene table like rank() returns, from (id, mgrs_rank, footprint box) rows."""
    return gpd.GeoDataFrame({"id": [r[0] for r in rows], "mgrs": "11SLT", "mgrs_rank": [r[1] for r in rows], "offset": -0.1,
                             **{f"href_{b}": [f"mem://{r[0]}/{b}" for r in rows] for b in ("red", "green", "blue", "scl")}},
                            geometry=[shapely.box(*r[2]) for r in rows], crs="EPSG:4326")


def test_bands_and_places(s2):
    assert s2.band_name("B04") == "red" and s2.band_name("b8a") == "nir08" and s2.band_name("S2") == "swir22"
    assert s2.band_name("SCL") == "scl" and s2.band_names("red,B04,nir") == ["red", "nir"]
    assert s2.bbox("1,2,3,4") == [1.0, 2.0, 3.0, 4.0]
    multi = shapely.MultiPolygon([shapely.box(0, 0, 1, 1), shapely.box(2, 2, 3, 3)])
    assert s2.bbox(multi) == [0.0, 0.0, 3.0, 3.0]
    for bad in ("3,2,1,4", "179.9,0,180.1,1", "a,b,c,d", [1, 2, 3]):
        with pytest.raises(ValueError):
            s2.bbox(bad)
    assert s2.date_window("2024-07-01", 30) == ("2024-06-01", "2024-07-31")
    assert s2.bounds_to_tile([-73.9893, 40.7638, -73.9453, 40.7971]) == (13, 2412, 3078)
    assert s2.utm_crs([179.5, -17, 179.9, -16]) == "EPSG:32760" and s2.utm_crs([-180, 10, -179.5, 11]) == "EPSG:32601"
    with pytest.raises(ValueError):
        s2.scl_classes("4,x")


def test_grid_resolution_is_metres(s2):
    for crs in ("EPSG:4326", "EPSG:2263"):
        with pytest.raises(ValueError):
            s2.geobox([-74.0, 40.7, -73.9, 40.8], 10, crs)
    merc = s2.geobox([10, 59.9, 10.2, 60.0], 20, "EPSG:3857")
    assert abs(abs(merc.resolution.x) - 40) < 0.5
    assert s2.geobox([10, 59.9, 10.2, 60.0], 20, "EPSG:4326", shape=(10, 20)).shape == (10, 20)


def test_formulas(s2):
    for bad in ["__import__('os')", "N.__class__", "(lambda: 1)()", "[x for x in N]", "open('x')", "'a'", "N if True else R",
                "N +", "max(R, G, B)", "sqrt(N, R)", "log(N, 10)", "1 / 0 + R", "(n - r) / (n + r)", "N[0]"]:
        with pytest.raises(ValueError):
            s2.index(synth({"nir": np.ones((2, 2)), "red": np.ones((2, 2))}), bad)
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
    assert set(s2.needs("NDVI, EVI")) == {"nir", "red", "blue"} and s2.needs("clip(N, 0, 1)") == ["nir"]
    both = s2.indices(ds, "NDVI, clip(N, 0, 1)")
    assert list(both.data_vars) == ["NDVI", "clip(N, 0, 1)"]
    votes = s2.index(ds, "(R > 0.1) + (N > 0.1) + (G > 0.1)").values
    expect = (ds["red"] > 0.1).values.astype(int) + (ds["nir"] > 0.1).values + (ds["green"] > 0.1).values
    assert np.array_equal(votes, expect.astype("float32"))
    assert np.array_equal(s2.index(ds, "not (R > 0.1)").values, (ds["red"] <= 0.1).values.astype("float32"))
    wet = s2.index(ds.assign(green=ds["green"].where(ds["green"] > 0.1)), "(G - S1) / (G + S1) > 0")
    assert int(wet.isnull().sum()) == int((ds["green"] <= 0.1).sum()), "a comparison must stay NaN where inputs are NaN"
    with pytest.raises(ValueError, match="BaI"):
        s2.index_info("bai")


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
    with pytest.raises(ValueError):
        s2.composite(ds, "p150")


def test_seasons_and_periods(s2):
    times = pd.to_datetime(["2023-12-10", "2024-01-15", "2024-02-20", "2024-03-10"])
    out = s2.composite(synth({"red": np.ones((4, 2, 2))}, times=times), "count", "season")
    assert list(pd.to_datetime(out["time"].values).strftime("%Y-%m")) == ["2023-12", "2024-03"]
    seasons = s2.period_ranges("2023-12-01", "2024-11-30", "season")
    assert [p[0] for p in seasons] == ["2023 DJF", "2024 MAM", "2024 JJA", "2024 SON"]
    assert seasons[0][1:] == ("2023-12-01", "2024-02-29")
    months = s2.period_ranges("2024-01-15", "2024-03-10", "month")
    assert months == [("2024-01", "2024-01-15", "2024-01-31"), ("2024-02", "2024-02-01", "2024-02-29"), ("2024-03", "2024-03-01", "2024-03-10")]
    days = s2.period_ranges("2024-01-01", "2024-01-25", "10D")
    assert [p[1:] for p in days] == [("2024-01-01", "2024-01-10"), ("2024-01-11", "2024-01-20"), ("2024-01-21", "2024-01-25")]
    for bad in ("ME", "2MS", "week"):
        with pytest.raises(ValueError):
            s2.period_ranges("2024-01-01", "2024-03-01", bad)


def test_time_functions(s2):
    s = pd.Series([1.0, np.nan, np.nan, np.nan, np.nan, 2.0, np.nan, 3.0], index=pd.date_range("2024-01-01", periods=8, freq="10D"))
    f = s2.fill_gaps(s, max_gap="30D")
    assert f.iloc[1:5].isna().all() and f.iloc[6] == 2.5, f.tolist()
    cube = synth({"v": np.broadcast_to(s.values[:, None, None], (8, 2, 2)).copy()}, times=s.index)["v"]
    fc = s2.fill_gaps(cube, max_gap="30D")
    assert np.allclose(fc.values[:, 0, 0], f.values, equal_nan=True)
    d = pd.date_range("2023-01-03", "2023-12-28", freq="5D")
    doy = d.dayofyear.values
    curve = 0.2 + 0.6 * np.exp(-((doy - 90) / 25) ** 2) + 0.55 * np.exp(-((doy - 220) / 30) ** 2)
    ph = s2.phenology(pd.Series(curve, index=d))
    assert len(ph) == 2 and abs(ph["peak_doy"].iloc[0] - 90) <= 10 and abs(ph["peak_doy"].iloc[1] - 220) <= 10
    weekly = pd.date_range("2021-01-03", "2024-12-28", freq="7D")
    wave = 0.5 + 0.2 * np.cos(2 * np.pi * (np.asarray(weekly.dayofyear) - 200) / 365.25)
    _, coef = s2.harmonic(pd.Series(wave, index=weekly), n=1)
    assert abs(coef["amplitude_1"] - 0.2) < 0.01 and abs(coef["phase_1"] - 200) < 5
    wcube = synth({"v": np.broadcast_to(wave[:, None, None], (len(weekly), 2, 3)).copy()}, times=weekly)["v"]
    _, c2 = s2.harmonic(wcube.transpose("y", "x", "time"), n=1)
    assert abs(float(c2["amplitude_1"][0, 0]) - 0.2) < 0.01 and abs(float(c2["phase_1"][0, 0]) - 200) < 5


def test_anomaly_and_climatology(s2):
    months = pd.date_range("2019-01-01", "2024-12-01", freq="MS")
    rng = np.random.default_rng(3)
    vals = 0.5 + 0.1 * np.sin(months.month.values) + rng.normal(0, 0.05, len(months))
    series = pd.Series(vals, index=months)
    cube = synth({"v": np.broadcast_to(vals[:, None, None], (len(months), 2, 2)).copy()}, times=months)["v"]
    a_series = s2.anomaly(series, [2019, 2020, 2021])
    a_cube = s2.anomaly(cube, [2019, 2020, 2021])
    assert len(a_series) == 36 and a_cube.sizes["time"] == 36
    assert np.allclose(a_series["anomaly"].values, a_cube.values[:, 0, 0], atol=1e-5)
    jan = vals[[i for i, m in enumerate(months) if m.month == 1 and m.year <= 2021]]
    assert abs(float(a_series["anomaly"].iloc[0]) - (vals[36] - jan.mean()) / jan.std(ddof=1)) < 1e-5
    holes = vals.copy()
    holes[[i for i, m in enumerate(months) if m.month == 1 and m.year != 2022]] = np.nan
    z = s2.anomaly(synth({"v": np.broadcast_to(holes[:, None, None], (len(months), 2, 2)).copy()}, times=months)["v"],
                   [2019, 2020, 2021, 2023], target_years=[2022])
    assert z.sel(time="2022-01").isnull().all() and z.sizes["time"] == 12


def test_phenology_map_quiet_on_empty_pixels(s2):
    t = pd.date_range("2024-01-01", periods=36, freq="10D")
    v = np.broadcast_to((0.2 + 0.6 * np.exp(-((np.arange(36) - 18) / 5) ** 2))[:, None, None], (36, 2, 2)).copy()
    v[:, 0, 0] = np.nan
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        out = s2.phenology_map(synth({"v": v}, times=t)["v"])
    assert np.isnan(out["peak"].values[0, 0]) and abs(float(out["peak"].values[1, 1]) - 0.8) < 1e-5


def test_mask(s2):
    scl = np.full((1, 5, 5), 4.0)
    scl[0, 2, 2] = 9
    scl[0, :, 0] = 6
    ok = s2.clear_mask(synth({"red": np.ones((1, 5, 5)), "scl": scl}), "land", dilate=1, rescue=False).values[0]
    assert not ok[1:4, 1:4].any() and ok[0, 1] and ok[4, 4] and not ok[:, 0].any() and ok[2, 4]
    scl = np.array([[[6.0, 2.0]], [[3.0, 2.0]], [[6.0, 9.0]]])
    land = s2.clear_mask(synth({"red": np.ones((3, 1, 2)), "scl": scl}), "land").values
    assert not land[:, 0, 0].any(), "water marked as shadow must not become land"
    clear = s2.clear_mask(synth({"red": np.ones((3, 1, 2)), "scl": scl}), "clear").values
    assert clear[:2, 0, 1].all() and not clear[2, 0, 1], "a never-clear pixel gets back its dark-area dates, not its cloud date"
    glacier = np.array([[[11.0]], [[11.0]], [[3.0]], [[2.0]]])
    snow = s2.clear_mask(synth({"red": np.ones((4, 1, 1)), "scl": glacier}), "clear_snow").values[:, 0, 0]
    assert snow.tolist() == [True, True, False, False], "a pixel that is valid on snow dates is not rescued"


def test_rank_uses_solar_days(s2):
    sc = scene_table([(f"s{i}", 0, (0, 0, 1, 1)) for i in range(5)])
    sc["datetime"] = [f"2024-07-{d}T10:30:00Z" for d in (13, 14, 15, 16, 17)]
    sc["date"] = [f"2024-07-{d}" for d in (13, 14, 15, 16, 17)]
    sc["cloud_cover"], sc["nodata_pct"] = 10.0, 0.0
    out = s2.rank(sc, date="2024-07-15", order="date")
    assert out["days_off"].tolist() == [0, 1, 1, 2, 2] and out["date"].iloc[0] == "2024-07-15"


def test_zones_and_stats(s2):
    ds = synth({"red": np.arange(25, dtype="float32").reshape(5, 5)})
    big = shapely.box(360_000, 3_769_950, 360_050, 3_770_000)
    small = shapely.box(360_012, 3_769_988, 360_014, 3_769_986)
    zones = gpd.GeoDataFrame({"id": ["big", "small"]}, geometry=[big, small], crs="EPSG:32611")
    lab = s2.zone_labels(ds, zones)
    assert (lab == 2).sum() == 1 and (lab == 1).sum() == 24
    twins = gpd.GeoDataFrame({"id": ["2023", "2024"]}, geometry=[big, big], crs="EPSG:32611")
    assert (s2.zone_labels(ds, twins) == 2).all(), "the later of two equal zones keeps the pixels"
    dry = ds.assign(red=ds["red"].where(lab != 2))
    z = s2.zonal_stats(dry, zones, "mean,count", id_col="id")
    assert set(z["zone"]) == {"big", "small"} and z.loc[z["zone"] == "small", "valid_pct"].iloc[0] == 0
    two = synth({"red": np.ones((2, 5, 5))}, times=pd.to_datetime(["2024-06-01", "2024-06-01"]))
    assert len(s2.zonal_stats(two, zones, "mean")) == 4
    pts = pd.DataFrame({"lat": [34.07, 50.0], "lng": [-118.57, 10.0]})
    smp = s2.sample(synth({"red": np.ones((5, 5))}, crs="EPSG:4326", res=0.01), pts)
    assert smp["inside"].tolist() == [True, False] and smp["red"].isna().tolist() == [False, True]


def test_change_and_area(s2):
    before = synth({"nir": np.full((1, 4, 4), 0.4), "swir22": np.full((1, 4, 4), 0.1)})
    after = synth({"nir": np.full((1, 4, 4), 0.2), "swir22": np.full((1, 4, 4), 0.3)})
    ch = s2.change(before, after, "burn")
    assert int(ch["class"].notnull().sum()) == 16 and ch.attrs["labels"][int(ch["class"].values[0, 0])] == "high"
    utm = s2.class_area(ch["class"])
    assert utm["pixels"].sum() == 16 and abs(utm["km2"].sum() - 0.0016) < 0.0005
    deg = synth({"c": np.zeros((10, 10))}, crs="EPSG:4326", res=0.01)["c"]
    area = s2.class_area(deg)["km2"].sum()
    assert 101 < area < 103.5, area
    from odc.geo.xr import xr_zeros

    merc = xr_zeros(s2.geobox([10, 59.95, 10.1, 60.0], 100, "EPSG:3857"), dtype="float32")
    expect = merc.size * 0.01
    assert abs(s2.class_area(merc)["km2"].sum() - expect) < 0.03 * expect
    world_merc = xr_zeros(s2.geobox([10, 59.95, 10.1, 60.0], 100, "EPSG:3395"), dtype="float32")
    true_km2 = s2.class_area(xr_zeros(s2.geobox([10, 59.95, 10.1, 60.0], 10, "utm"), dtype="float32"))["km2"].sum()
    assert abs(s2.class_area(world_merc)["km2"].sum() - true_km2) < 0.05 * true_km2


def test_classify(s2):
    rng = np.random.default_rng(0)
    feat = synth({"a": rng.normal(0, 1, (20, 20)), "b": rng.normal(0, 1, (20, 20))})
    label = np.where(feat["a"].values > 0, 1.0, 0.0)
    labels = s2.on_grid(label.astype("float32"), feat, "label", {"labels": ["neg", "pos"]})
    groups = np.full((20, 20), np.nan, dtype="float32")
    groups[:10] = np.repeat(np.arange(10), 20).reshape(10, 20)
    _, rep = s2.train(s2.features(feat), labels, groups=s2.on_grid(groups, feat, "zone"), per_class=150, seed=2)
    assert rep["test_px"] > 0 and rep["accuracy"] > 0.8 and rep["importance"] is not None
    with pytest.raises(ValueError):
        s2.train(s2.features(feat), labels, groups=s2.on_grid(np.zeros((20, 20), dtype="float32"), feat, "zone"))


def test_feature_names(s2):
    ds = synth({"red": np.ones((2, 2, 2)), "nir": np.ones((2, 2, 2))}, times=pd.to_datetime(["2024-06-01", "2024-06-15"]))
    names = list(s2.features(ds)["feature"].values)
    assert len(set(names)) == 4 and names[0] == "red_2024-06-01", names


def test_render(s2):
    da = synth({"v": np.linspace(0, 1, 16).reshape(4, 4)})["v"]
    up, down = s2.colorize(da, "viridis", 0, 1), s2.colorize(da, "viridis", 1, 0)
    assert np.array_equal(up[:3, 0, 0], down[:3, -1, -1]) and not np.array_equal(up[:3, 0, 0], up[:3, -1, -1])
    classes = synth({"c": np.arange(16, dtype="float32").reshape(4, 4) % 12})["c"]
    rgba = s2.colorize_classes(classes)
    assert not np.array_equal(rgba[:3, 0, 0], rgba[:3, 2, 2])
    assert s2.colorize_classes(classes.where(classes < 2), ["red", "#00f"])[:3, 0, 1].tolist() == [0, 0, 255]
    assert s2.css_gradient("viridis", 1).startswith("linear-gradient")


def test_mosaic_reads_until_covered(s2, monkeypatch):
    gb = s2.tile_geobox(10, 300, 400, 64)
    w, s, e, n = gb.extent.to_crs("EPSG:4326").boundingbox
    mid = (w + e) / 2
    sc = scene_table([("west_half", 0, (w - 1, s - 1, mid, n + 1)), ("full", 1, (w - 1, s - 1, e + 1, n + 1)),
                      ("broken", 2, (w - 1, s - 1, e + 1, n + 1))])
    cols = (np.arange(64) + 0.5) / 64 * (e - w) + w

    def fake(href, grid, nearest):
        sid, band = href.split("/")[2:4]
        if sid == "broken":
            return None, None
        inside = np.broadcast_to(cols < mid if sid == "west_half" else np.ones(64, bool), (64, 64))
        value = {"red": 1000, "green": 2000, "blue": 3000, "scl": 4}[band]
        return np.where(inside, value if sid == "west_half" or band == "scl" else value + 500, 0).astype("uint16"), inside

    monkeypatch.setitem(s2.mosaic.__globals__, "_read_band", fake)
    data, rep = s2.mosaic(sc, ["red", "green", "blue"], gb)
    assert not np.isnan(data).any(), "the east half must be filled from the second round"
    assert rep["scenes"] == ["west_half", "full"] and rep["rounds"] == 2 and rep["clear_pct"] == 100.0
    assert np.allclose(data[0][:, cols < mid], 0.0) and np.allclose(data[0][:, cols >= mid], 0.05)
    one, rep1 = s2.mosaic(sc[sc["id"] != "west_half"].assign(mgrs_rank=[0, 1]), ["red"], gb)
    assert rep1["rounds"] == 1 and rep1["scenes"] == ["full"] and not np.isnan(one).any()

    def no_scl(href, grid, nearest):
        return (None, None) if href.endswith("/scl") and "west_half" in href else fake(href, grid, nearest)

    monkeypatch.setitem(s2.mosaic.__globals__, "_read_band", no_scl)
    data2, rep2 = s2.mosaic(sc, ["red"], gb)
    assert rep2["rounds"] == 2 and np.allclose(data2[0], 0.05), "a scene without scl is never taken as clear"
    south_up = gb.__class__(gb.shape, gb.affine * gb.affine.__class__.scale(1, -1), gb.crs)
    with pytest.raises(ValueError):
        s2.mosaic(sc, ["red"], south_up)


def test_input_checks(s2):
    with pytest.raises(ValueError):
        s2.export(BOX, "2024-07-01", kind="bands", bands="red,scl")
    with pytest.raises(ValueError):
        s2.to_h3(synth({"v": np.ones((2, 2))})["v"], method="avg(v); DROP")
    with pytest.raises(ValueError):
        s2.to_cog(synth({"v": np.ones((2, 2, 2))}), "x.tif")
    with pytest.raises(ValueError):
        s2.animate([np.zeros((4, 2, 2), "uint8"), np.zeros((4, 3, 3), "uint8")])
    assert len(s2.animate([np.zeros((4, 8, 8), "uint8")], labels=[2024])) > 0
    with pytest.raises(ValueError):
        s2.save_tiles(BOX, (12, 13), "2024-07-01", "fd://x", fmt="jpg")


def test_to_cog_writes_band_names(s2, tmp_path):
    rasterio = pytest.importorskip("rasterio")
    path = s2.to_cog(synth({"red": np.ones((4, 4)), "nir": np.full((4, 4), 2.0)}), str(tmp_path / "x.tif"))
    with rasterio.open(path) as src:
        assert src.descriptions == ("red", "nir") and src.crs.to_epsg() == 32611 and src.read(2)[0, 0] == 2.0
    count = s2.clear_count(synth({"red": np.ones((3, 4, 4))}))
    with rasterio.open(s2.to_cog(count, str(tmp_path / "count.tif"))) as src:
        assert src.read(1)[0, 0] == 3
    with rasterio.open(s2.to_cog(count > 1, str(tmp_path / "mask.tif"))) as src:
        assert src.dtypes[0] == "uint8" and src.read(1)[0, 0] == 1


def test_export_grid_fits_max_px(s2):
    rng = np.random.default_rng(5)
    for _ in range(40):
        lon, lat = rng.uniform(-170, 170), rng.uniform(-70, 70)
        w, h = rng.uniform(0.05, 0.6, 2)
        res, gb = s2._export_grid([lon, lat, lon + w, lat + h], None, "utm", 4096)
        assert max(gb.shape) <= 4096 and res % 10 == 0
    with pytest.raises(ValueError):
        s2._export_grid([0.0, 45.0, 1.0, 46.0], 10, "utm", 4096)


def test_worker_udf_uses_its_own_copy(s2, tmp_path):
    other = tmp_path / "sentinel_utils"
    shutil.copytree(UDF_PATH, other)
    code = (other / "sentinel_utils.py").read_text() + "\n\ndef marker_v2():\n    return 2\n"
    (other / "sentinel_utils.py").write_text(code)
    copy = fused.load(str(other))
    assert "def marker_v2(" in copy.worker_udf("block_worker").code
    for name, age in [("block_worker", 0), ("tile_worker", 0), ("frame_worker", 7 * 86400)]:
        w = s2.worker_udf(name)
        assert w.entrypoint == name and w.cache_max_age == age and "def marker_v2(" not in w.code


@live
def test_search_sources_agree(s2):
    a = s2.search(BOX, "2023-06-01", "2023-08-31", max_cloud=20, source="c1")
    b = s2.search(BOX, "2023-06-01", "2023-08-31", max_cloud=20, source="coop")
    assert len(a) and sorted(a["id"]) == sorted(b["id"]), (len(a), len(b))


@live
def test_search_large_and_edges(s2):
    big = s2.search([-118.8, 33.9, -118.3, 34.3], "2023-01-01", "2024-12-31", source="c1")
    assert len(big) > 340
    nz = s2.search([172.60, -43.55, 172.65, -43.50], "2024-01-14", "2024-01-14", max_cloud=100)
    assert len(nz) and set(nz["date"]) == {"2024-01-14"}
    year = pd.Timestamp.now(tz="UTC").year
    s2.search(BOX, f"{year}-12-20", f"{year + 1}-01-10", source="coop")


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
def test_load_masks_and_scene_ids(s2):
    sc = s2.search(BOX, "2023-06-01", "2023-06-30", max_cloud=100, source="c1")
    ds = s2.load(sc, "red,cloud", BOX, res=60, merge=None)
    assert list(ds["scene"].values) == list(sc.sort_values(["datetime", "id"])["id"])
    cloud = ds["cloud"].values
    assert (cloud == 0).any() and np.array_equal(np.isnan(cloud), np.isnan(ds["scl"].values))


@live
def test_pixel_series_mask_bands(s2):
    px = s2.pixel_series(34.06, -118.56, "2024-06-01", "2024-06-30", "scl,cloud")
    seen = px[px["scl"].notna()]
    assert len(seen) and list(px.columns)[-2:] == ["scl", "cloud"] and seen["cloud"].between(0, 100).all()
    legacy = s2.pixel_series(34.06, -118.56, "2022-06-01", "2022-06-20", "cloud")
    assert len(legacy) and legacy["cloud"].isna().all(), "legacy scenes have no cloud band"



@live
def test_aoi_cloud_in_batches(s2, monkeypatch):
    sc = s2.search(BOX, "2024-06-01", "2024-07-31", max_cloud=100)
    whole = s2.aoi_cloud(sc, BOX)
    monkeypatch.setitem(s2.aoi_cloud.__globals__, "MAX_PIXELS", 60_000)
    batched = s2.aoi_cloud(sc, BOX)
    pd.testing.assert_frame_equal(whole, batched)


@live
def test_json_roundtrip(s2):
    sc = s2.rank(s2.search(BOX, "2024-06-01", "2024-06-30"), per_mgrs=2)
    back = s2.from_json(s2.to_json(sc, "red,nir,scl"))
    assert list(back["id"]) == list(sc["id"]) and back["href_red"].notna().all()
    assert back["offset"].tolist() == sc["offset"].tolist() and back.geometry.geom_equals_exact(sc.geometry.reset_index(drop=True), tolerance=1e-5).all()
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
        assert src.res == (10.0, 10.0) and nir[nir > 0].mean() > 1000 and (nir > 0).mean() > 0.99


@live
def test_embeddings(s2):
    e = s2.load_embeddings([-93.56, 41.97, -93.54, 41.99], 2024, res=20)
    norm = np.sqrt(np.nansum(e.values ** 2, axis=0))
    assert (norm > 0).any() and abs(float(np.median(norm[norm > 0])) - 1) < 0.02
