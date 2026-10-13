"""Tests for public/sentinel_utils. Each test checks a known value.

Run from the root of the repo:

    pytest tests/test_sentinel_utils.py                          fast tests only
    SENTINEL_UTILS_LIVE=1 pytest tests/test_sentinel_utils.py    fast tests and live tests

The fast tests use synthetic data. They do not use the network and they do not need a Fused login.
The live tests read real Sentinel-2 data and run remote Fused workers. They need a Fused login.
The tests load the package folder with fused.load, as a user does.
"""

import datetime as dt
import json
import math
import os
import time
import warnings

import fused
import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")
pytest.importorskip("odc.geo.xr")
gpd = pytest.importorskip("geopandas")
shapely = pytest.importorskip("shapely")

PACKAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "public", "sentinel_utils")
LIVE = os.environ.get("SENTINEL_UTILS_LIVE") == "1"
live = pytest.mark.skipif(not LIVE, reason="set SENTINEL_UTILS_LIVE=1 to read real data and run remote workers")

LA = [-118.62, 34.02, -118.50, 34.10]
OCEAN = [-140.0, 0.0, -139.99, 0.01]
JUNE = ("2023-06-01", "2023-06-30")
FIELD = {"type": "Polygon", "coordinates": [[[-119.27, 36.05], [-119.26, 36.05], [-119.26, 36.058], [-119.27, 36.058],
                                             [-119.27, 36.05]]]}
TRIANGLE = shapely.Polygon([(-118.6, 34.0), (-118.2, 34.0), (-118.6, 34.3)])
MODULES = ["su_constants", "su_geometry", "su_time", "su_catalog", "su_reader", "su_process", "su_statistical",
           "su_areas", "su_indices", "su_embeddings", "su_download", "su_collection"]
SCENE_COLUMNS = ["id", "datetime", "date", "tile_id", "platform", "cloud_cover", "nodata_pct", "crs", "source",
                 "offset", "assets", "geometry"]
TIMES = {}


@pytest.fixture(scope="module")
def su():
    fused.options.show.enable_tqdm = False
    return fused.load(PACKAGE)


@pytest.fixture(scope="module")
def mod(su):
    return {name: su.module(name) for name in MODULES}


def teardown_module():
    if TIMES:
        print("\nTIMES", json.dumps({k: round(v, 1) for k, v in TIMES.items()}))


def _timed(name, fn):
    t0 = time.time()
    out = fn()
    TIMES[name] = time.time() - t0
    return out


def test_meta_lists_the_twelve_modules_as_headers():
    meta = json.load(open(os.path.join(PACKAGE, "meta.json")))
    udf = meta["job_config"]["steps"][0]["udf"]
    assert udf["source"] == "sentinel_utils.py" and udf["entrypoint"] == "udf"
    assert [h["module_name"] for h in udf["headers"]] == MODULES
    assert [h["source_file"] for h in udf["headers"]] == [f"{m}.py" for m in MODULES]
    assert all(os.path.exists(os.path.join(PACKAGE, f"{m}.py")) for m in MODULES)


def test_api_lists_76_functions(su):
    table = su.api()
    assert list(table.columns) == ["module", "name", "signature", "doc"] and len(table) == 76
    assert table.groupby("module").size().to_dict() == {
        "su_areas": 6, "su_catalog": 8, "su_collection": 1, "su_constants": 6, "su_download": 8, "su_embeddings": 7,
        "su_geometry": 11, "su_indices": 3, "su_process": 3, "su_reader": 15, "su_statistical": 3, "su_time": 5}
    assert not table["name"].str.startswith("_").any() and table["doc"].str.len().gt(0).all()
    listing = fused.run(su, engine="local")
    assert list(listing["name"]) == list(table["name"])


def test_module_names_are_checked(su):
    with pytest.raises(ValueError, match="su_collection"):
        su.module("su_worker")


def test_lone_main_file_asks_for_the_package(tmp_path):
    (tmp_path / "sentinel_utils.py").write_bytes(open(os.path.join(PACKAGE, "sentinel_utils.py"), "rb").read())
    lone = fused.load(str(tmp_path / "sentinel_utils.py"))
    assert lone._HELD == {}
    with pytest.raises(ValueError, match="load the package folder or GitHub URL"):
        lone.module("su_constants")


def test_band_names_and_collections(su, mod):
    c = mod["su_constants"]
    assert su.band_names("B4,red,nir,B8A") == ["B04", "B08", "B8A"]
    assert su.band_names(["b2", "swir1", "swir22", "scl", "CLD", "dataMask", "B01"]) == [
        "B02", "B11", "B12", "SCL", "CLD", "dataMask", "B01"]
    assert su.band_names("dem", collection=c.DEM) == ["DEM"]
    assert su.band_names(["a00", "A63"], collection=c.AEF) == ["A00", "A63"]
    with pytest.raises(ValueError, match="B8A"):
        su.band_names("B13")
    assert su.band_info("nir08") == {"name": "B8A", "asset": "nir08", "alias": "nir08", "resolution": 20,
                                     "wavelength": 865}
    assert su.band_info("B11")["alias"] == "swir1"
    assert su.get_available_collections() == ["sentinel-2-l2a", "cop-dem-glo-30", "alphaearth-embeddings"]
    assert [su.SENTINEL2_L2A, su.DEM, su.AEF] == su.get_available_collections()
    assert su.collection_info(c.DEM)["resolution"] == 30 and len(su.collection_info(c.AEF)["bands"]) == 64


def test_constants_and_allowed_values(mod):
    c = mod["su_constants"]
    assert c.MASKS["clear"] == (4, 5, 6) and c.MASKS["water"] == (6,) and c.SCL_CLASSES[10] == "thin_cirrus"
    assert c.MOSAIC_ORDERS == ("least_cloudy", "newest", "oldest")
    assert c.OUTPUTS == ("dataset", "array", "geotiff", "png")
    assert issubclass(c.NoDataError, ValueError)
    assert c.check_choice("units", "dn", c.UNITS) == "dn"
    with pytest.raises(ValueError, match=r"\['reflectance', 'dn'\]"):
        c.check_choice("units", "toa", c.UNITS)


def test_to_bbox_accepts_every_area_form(su, tmp_path):
    box = (-118.5, 34.0, -118.4, 34.1)
    feature = {"type": "Feature", "properties": {}, "geometry": shapely.geometry.mapping(shapely.box(*box))}
    forms = ["-118.5,34.0,-118.4,34.1", "[-118.5, 34.0, -118.4, 34.1]", list(box), box, np.array(box),
             shapely.box(*box), shapely.box(*box).wkt, feature, {"type": "FeatureCollection", "features": [feature]},
             gpd.GeoDataFrame(geometry=[shapely.box(*box)], crs=4326).to_crs(3857)]
    for form in forms:
        assert su.to_bbox(form) == pytest.approx(box, abs=1e-9), form
    path = tmp_path / "a.geojson"
    path.write_text(json.dumps(feature))
    assert su.read_geojson(str(path)) == json.loads(json.dumps(feature))
    assert su.geojson_to_wkt(feature).startswith("POLYGON ((-118.4 34")
    assert su.to_geometry(feature).equals(shapely.box(*box))
    for bad in ["1,2,3", [1, 2, 3, 4, 5], 42, "10,0,5,1", (170, 0, 190, 1)]:
        with pytest.raises(ValueError):
            su.to_bbox(bad)


def test_utm_zones_and_transforms(su):
    assert su.get_utm_crs(-118.5, 34.05) == "EPSG:32611"
    assert su.get_utm_crs(151.2, -33.9) == "EPSG:32756"
    assert su.get_utm_crs(180, 0) == "EPSG:32601"
    bbox, crs = su.to_utm_bbox((-117.05, 34.0, -116.95, 34.1))
    assert crs == "EPSG:32611" and bbox[0] == pytest.approx(495380, abs=30) and bbox[2] == pytest.approx(504620, abs=30)
    x0, y0, x1, _ = su.transform_bbox((0, 0, 1, 1), "EPSG:4326", "EPSG:3857")
    assert (x0, y0) == pytest.approx((0, 0), abs=1e-6) and x1 == pytest.approx(111319.49, abs=0.1)


def test_dimensions_geobox_and_area(su):
    box = (-117.05, 34.0, -116.95, 34.1)
    assert su.bbox_to_dimensions(box, 10) == (923, 1109)
    assert su.bbox_to_dimensions(box, (20, 40)) == (round(923 / 2), round(1109 / 4))
    rx, ry = su.bbox_to_resolution(box, 923, 1109)
    assert rx == pytest.approx(10, abs=0.02) and ry == pytest.approx(10, abs=0.02)
    gb = su.make_geobox(box, resolution=10)
    assert str(gb.crs) == "EPSG:32611" and gb.resolution.x == 10 and abs(gb.shape[1] - 923) <= 1
    assert su.make_geobox(box, size=(256, 128)).shape == (128, 256)
    merc = su.make_geobox(box, resolution=10, crs="EPSG:3857")
    assert merc.resolution.x == pytest.approx(10 / math.cos(math.radians(34.05)))
    with pytest.raises(ValueError, match="size"):
        su.make_geobox(box, resolution=10, crs="EPSG:4326")
    assert su.area_km2((0, 0, 1, 1)) == pytest.approx(12308.8, rel=1e-3)


def test_parse_time_forms(su):
    now = pd.Timestamp.now(tz="UTC")
    assert abs(su.parse_time("NOW-30DAYS") - (now - pd.Timedelta(days=30))) < pd.Timedelta(minutes=1)
    assert abs(su.parse_time("NOW+2HOURS") - (now + pd.Timedelta(hours=2))) < pd.Timedelta(minutes=1)
    day = pd.Timestamp("2024-01-05", tz="UTC")
    for value in ["20240105", "2024-01-05", dt.date(2024, 1, 5), dt.datetime(2024, 1, 5), np.datetime64("2024-01-05"),
                  "2024-01-05T00:00:00Z", pd.Timestamp("2024-01-04 19:00", tz="US/Eastern")]:
        assert su.parse_time(value) == day, value
    for bad in ["yesterday", "NOW-3FORTNIGHTS", None, ""]:
        with pytest.raises(ValueError, match="NOW-30DAYS"):
            su.parse_time(bad)


def test_time_intervals_and_serialize(su):
    start, end = su.parse_time_interval("2024-01-05")
    assert start == pd.Timestamp("2024-01-05", tz="UTC") and end == pd.Timestamp("2024-01-05 23:59:59.999", tz="UTC")
    start, end = su.parse_time_interval(("2024-01-01T06:00", "2024-01-01T08:00"))
    assert end - start == pd.Timedelta(hours=2)
    with pytest.raises(ValueError):
        su.parse_time_interval(("2024-02-01", "2024-01-01"))
    assert su.serialize_time("20240105") == "2024-01-05"
    assert su.serialize_time(pd.Timestamp("2024-01-05 10:30", tz="UTC")) == "2024-01-05T10:30:00Z"
    assert su.serialize_time(su.parse_time_interval("2024-01-05")) == ("2024-01-05T00:00:00Z",
                                                                       "2024-01-05T23:59:59.999000Z")


def test_filter_and_split_times(su):
    t0 = pd.Timestamp("2024-01-01 10:00", tz="UTC")
    stamps = [t0 + pd.Timedelta(minutes=m) for m in (130, 0, 10, 50, 120, 70, 0)]
    assert su.filter_times(stamps, dt.timedelta(hours=1)) == [t0, t0 + pd.Timedelta(minutes=70)]
    assert su.filter_times([], "1h") == []
    assert su.split_time_interval(("2024-01-15", "2024-04-10"), freq="month") == [
        ("2024-01-15", "2024-01-31"), ("2024-02-01", "2024-02-29"), ("2024-03-01", "2024-03-31"),
        ("2024-04-01", "2024-04-10")]
    assert su.split_time_interval(("2024-01-01", "2024-01-30"), n=3) == [
        ("2024-01-01", "2024-01-10"), ("2024-01-11", "2024-01-20"), ("2024-01-21", "2024-01-30")]
    with pytest.raises(ValueError, match="month"):
        su.split_time_interval(("2024-01-01", "2024-02-01"), freq="fortnight")


def test_process_request_defaults_and_json_round_trip(su):
    r = su.process_request(LA, "2023-06-22")
    assert r == {"type": "process", "area": LA, "date": "2023-06-22", "bands": ["B04", "B03", "B02"],
                 "collection": "sentinel-2-l2a", "resolution": 10, "size": None, "crs": None, "mosaicking": "simple",
                 "mosaic_order": "least_cloudy", "cloud_cover": 100.0, "units": "reflectance", "resampling": "nearest",
                 "mask": None, "index": None, "reducer": None, "output": "dataset"}
    assert su.check_request(json.loads(json.dumps(r))) == r
    full = su.process_request(LA, JUNE, "red,nir,SCL", resolution=20, mask="clear", index="ndvi", reducer="median",
                              cloud_cover=(0, 30), units="dn", output="geotiff")
    assert full["date"] == list(JUNE) and full["bands"] == ["B04", "B08", "SCL"] and full["index"] == "NDVI"
    assert full["cloud_cover"] == [0.0, 30.0] and su.check_request(json.loads(json.dumps(full))) == full
    assert su.process_request(LA, "2023-06-22", index="NDVI")["bands"] == ["B08", "B04"]
    assert su.process_request(LA, "2023-06-22", "B01")["resolution"] == 60
    dem = su.process_request(LA, None, collection="cop-dem-glo-30")
    assert dem["bands"] == ["DEM"] and dem["resolution"] == 30 and dem["date"] is None
    aef = su.process_request(LA, "2024-06-01", collection="alphaearth-embeddings", resolution=20)
    assert aef["date"] == "2024" and len(aef["bands"]) == 64 and aef["bands"][0] == "A00"


@pytest.mark.parametrize("kwargs, allowed", [
    ({"output": "tiff"}, "['dataset', 'array', 'geotiff', 'png']"),
    ({"mosaic_order": "leastCC"}, "['least_cloudy', 'newest', 'oldest']"),
    ({"mosaicking": "tile"}, "['simple', 'orbit']"),
    ({"units": "DN"}, "['reflectance', 'dn']"),
    ({"resampling": "lanczos"}, "['nearest', 'bilinear', 'cubic']"),
    ({"reducer": "sum"}, "['median', 'mean', 'min', 'max', 'count']"),
    ({"mask": "clouds"}, "'clear'"),
    ({"index": "XYZ"}, "'NDVI'"),
    ({"bands": "B10"}, "'B8A'"),
    ({"collection": "landsat"}, "'cop-dem-glo-30'"),
])
def test_bad_request_values_list_the_allowed_values(su, kwargs, allowed):
    with pytest.raises(ValueError) as e:
        su.process_request(LA, "2023-06-22", **kwargs)
    assert allowed in str(e.value), str(e.value)


def test_other_request_errors(su):
    cases = [({"resolution": 10, "size": (100, 100)}, "not both"),
             ({"collection": "alphaearth-embeddings", "mask": "clear"}, "mask, index or reducer"),
             ({"mosaicking": "orbit", "output": "geotiff"}, "one image"),
             ({"bands": "B04,B03", "output": "png"}, "1 or 3 layers"),
             ({"cloud_cover": 120}, "0-100"),
             ({"reducer": "count", "index": "NDVI"}, "not count"),
             ({"resolution": 1, "bands": "B04,B03,B02,B08"}, "MAX_PIXELS")]
    for kwargs, text in cases:
        with pytest.raises(ValueError, match=text):
            su.process_request(LA, "2023-06-22", **kwargs)
    with pytest.raises(ValueError, match="date is required"):
        su.process_request(LA, None)


def test_workers_refuse_other_dicts(su):
    r = su.process_request(LA, "2023-06-22")
    for bad, text in (({**r, "type": "x"}, "request type"), ({**r, "code": "print(1)"}, "unknown keys"),
                      ({**r, "output": "pickle"}, "output must be one of"), ([r], "must be a dict")):
        with pytest.raises(ValueError, match=text):
            su.check_request(bad)


def test_statistical_request_round_trip_and_validation(su):
    req = su.statistical_request(TRIANGLE, ("2024-01-01", "2024-12-31"), "red,nir", index="ndvi",
                                 aggregation_interval="P1M", percentiles=(5, 50, 95.5),
                                 histogram={"bins": 20, "low": -1, "high": 1})
    again = json.loads(json.dumps(req))
    assert again == req and req["type"] == "statistical" and req["area"]["type"] == "Polygon"
    assert req["bands"] == ["B04", "B08"] and req["index"] == "NDVI" and req["date"] == ["2024-01-01", "2024-12-31"]
    assert req["percentiles"] == [5, 50, 95.5] and req["histogram"] == {"bins": 20, "low": -1.0, "high": 1.0}
    assert su.statistical_request(**{k: v for k, v in again.items() if k != "type"}) == req
    for kwargs, text in [({}, "NDVI"), ({"index": "NDVI", "aggregation_interval": "1D"}, "P1M"),
                         ({"index": "NDVI", "collection": "cop-dem-glo-30"}, "sentinel-2-l2a"),
                         ({"index": "NDVI", "percentiles": [50, 101]}, "0 to 100"),
                         ({"index": "NDVI", "histogram": {"bins": 10, "low": 1, "high": 1}}, "low < high")]:
        with pytest.raises(ValueError, match=text):
            su.statistical_request(TRIANGLE, "2024-06-01", **kwargs)
    with pytest.raises(ValueError, match="unknown keys \\['evalscript'\\]"):
        su.check_request({**req, "evalscript": "return [1]"})


def test_statistics_table_math(mod):
    stat = mod["su_statistical"]
    values = np.full((1, 2, 4), np.nan)
    values[0, 0, :3] = [1.0, 2.0, 3.0]
    values[0, 1, 1:3] = [9.0, 9.0]
    days = pd.to_datetime(["2024-03-02", "2024-03-09"]).values
    month = stat._intervals(pd.Timestamp("2024-03-01"), pd.Timestamp("2024-03-31"), "P1M")
    row = stat._stats_table(values, days, month, ["B04"]).iloc[0]
    assert row["mean"] == 2.0 and row["sample_count"] == 4 and row["no_data_count"] == 1
    daily = stat._stats_table(values, days, stat._intervals(pd.Timestamp("2024-03-01"), pd.Timestamp("2024-03-31"),
                                                            "P1D"), ["B04"])
    assert list(daily["interval_from"]) == ["2024-03-02", "2024-03-09"] and list(daily["mean"]) == [2.0, 9.0]
    year = stat._intervals(pd.Timestamp("2024-01-01", tz="UTC"), pd.Timestamp("2024-12-31 23:59", tz="UTC"), "P1M")
    assert len(year) == 12 and f"{year[1][1]:%Y-%m-%d}" == "2024-02-29"


def _bands():
    values = {"B02": 0.05, "B03": 0.08, "B04": 0.1, "B05": 0.2, "B08": 0.5, "B11": 0.3, "B12": 0.2}
    return xr.Dataset({k: (("y", "x"), np.full((2, 3), v, "float32")) for k, v in values.items()}), values


def test_normalized_difference(su):
    out = su.normalized_difference(np.array([1.0, 0.0, 3.0]), np.array([1.0, 0.0, 1.0]))
    assert out[0] == 0 and np.isnan(out[1]) and out[2] == 0.5
    assert su.normalized_difference(np.array([1], "uint16"), np.array([3], "uint16"))[0] == pytest.approx(-0.5)
    da = su.normalized_difference(xr.DataArray([2.0, 0.0]), xr.DataArray([1.0, 0.0]))
    assert isinstance(da, xr.DataArray) and da[0] == pytest.approx(1 / 3) and np.isnan(da[1])


def test_index_values(su, mod):
    ds, v = _bands()
    nd = lambda a, b: (a - b) / (a + b)
    expected = {
        "NDVI": nd(v["B08"], v["B04"]), "NDWI": nd(v["B03"], v["B08"]), "MNDWI": nd(v["B03"], v["B11"]),
        "NDMI": nd(v["B08"], v["B11"]), "NBR": nd(v["B08"], v["B12"]), "NDSI": nd(v["B03"], v["B11"]),
        "NDRE": nd(v["B08"], v["B05"]), "EVI": 2.5 * (0.5 - 0.1) / (0.5 + 0.6 - 0.375 + 1),
        "SAVI": 1.5 * (0.5 - 0.1) / (0.5 + 0.1 + 0.5), "BSI": nd(v["B11"] + v["B04"], v["B08"] + v["B02"]),
    }
    assert set(expected) == set(mod["su_indices"].INDICES)
    for name, value in expected.items():
        da = su.compute_index(ds, name)
        assert da.name == name and da.dims == ("y", "x") and float(da[0, 0]) == pytest.approx(value, rel=1e-5), name
    assert float(su.compute_index(ds.rename({"B08": "nir", "B04": "red"}), "ndvi")[0, 0]) == pytest.approx(
        expected["NDVI"], rel=1e-5)
    assert su.required_bands("EVI") == ["B08", "B04", "B02"]
    with pytest.raises(ValueError, match="NDVI"):
        su.compute_index(ds, "XYZ")
    with pytest.raises(ValueError, match="B05"):
        su.compute_index(ds.drop_vars("B05"), "NDRE")


def test_index_clips_negative_reflectance_at_zero(su):
    ds = xr.Dataset({"B08": (("y", "x"), np.array([[-0.02, 0.4]], "float32")),
                     "B04": (("y", "x"), np.array([[0.01, -0.05]], "float32"))})
    ndvi = su.compute_index(ds, "NDVI").values
    assert ndvi[0, 0] == pytest.approx(-1.0) and ndvi[0, 1] == pytest.approx(1.0)
    raw = su.normalized_difference(ds["B08"], ds["B04"]).values
    assert raw[0, 0] == pytest.approx(3.0) and raw[0, 1] == pytest.approx(0.45 / 0.35, rel=1e-5)


def _check_cells(cells, geom, extra=()):
    assert list(cells.columns) == ["index_x", "index_y", "crs", "bbox", *extra, "geometry"]
    assert cells.crs.to_epsg() == 4326 and cells.intersects(geom).all()
    assert cells.index.tolist() == list(range(len(cells)))


def test_bbox_splitter(su):
    cells = su.bbox_splitter(TRIANGLE, (5, 4))
    _check_cells(cells, TRIANGLE)
    assert len(cells) == 14
    assert sorted(zip(cells.index_x, cells.index_y))[:5] == [(0, 0), (0, 1), (0, 2), (0, 3), (1, 0)]
    assert cells.bbox.iloc[0] == pytest.approx((-118.6, 34.0, -118.52, 34.075))
    assert len(su.bbox_splitter(TRIANGLE, 2, reduce_bbox_sizes=True)) == 3
    with pytest.raises(ValueError, match="split_shape"):
        su.bbox_splitter(TRIANGLE, (0, 2))


def test_osm_splitter(su):
    mercantile = pytest.importorskip("mercantile")
    tile = mercantile.tile(-118.4, 34.05, 10)
    b = mercantile.bounds(tile)
    corner = shapely.Point(b.east, b.south).buffer(0.01)
    four = su.osm_splitter(corner, 10)
    _check_cells(four, corner, ["zoom", "x", "y"])
    assert sorted(zip(four.x, four.y)) == [(tile.x, tile.y), (tile.x, tile.y + 1), (tile.x + 1, tile.y),
                                          (tile.x + 1, tile.y + 1)]
    with pytest.raises(ValueError, match="85.0511"):
        su.osm_splitter((0, 80, 1, 89), 3)


def test_utm_splitters(su):
    box = shapely.box(-117.05, 34.0, -116.95, 34.1)
    cells = su.utm_zone_splitter(box, 10_000)
    _check_cells(cells, box, ["utm_zone"])
    assert len(cells) == 4 and set(cells["crs"]) == {"EPSG:32611"} and set(cells.utm_zone) == {"11"}
    assert sorted(cells.bbox)[0] == (490000.0, 3760000.0, 500000.0, 3770000.0)
    across = shapely.box(-114.2, 34.0, -113.8, 34.1)
    two = su.utm_zone_splitter(across, 10_000)
    assert set(two["crs"]) == {"EPSG:32611", "EPSG:32612"} and two.union_all().covers(across)
    grid = su.utm_grid_splitter(shapely.box(-117.05, 31.95, -116.95, 32.05), 10_000)
    assert len(grid) == 6 and grid.groupby("utm_row").size().to_dict() == {"R": 4, "S": 2}
    with pytest.raises(ValueError, match="bbox_size"):
        su.utm_zone_splitter(box, 0)


def test_custom_grid_splitter(su):
    grid = [(-118.7, 33.9, -118.5, 34.1), (-118.1, 33.9, -117.9, 34.1), "-118.5,34.1,-118.3,34.3"]
    cells = su.custom_grid_splitter(TRIANGLE, grid)
    _check_cells(cells, TRIANGLE)
    assert cells.index_x.tolist() == [0, 2]


def test_tile_splitter_with_a_fake_catalog(su, mod, monkeypatch):
    products = gpd.GeoDataFrame(
        {"tile_id": ["11SLT", "11SLT", "11SMT"], "crs": ["EPSG:32611"] * 3},
        geometry=[shapely.box(-119.0, 33.8, -118.4, 34.4), shapely.box(-119.0, 33.8, -118.4, 34.4),
                  shapely.box(-118.45, 33.8, -117.8, 34.4)], crs=4326)
    fake = type("FakeCatalog", (), {"query": staticmethod(lambda area, date: products)})
    monkeypatch.setitem(mod["su_areas"].tile_splitter.__globals__, "catalog", fake)
    cells = su.tile_splitter(TRIANGLE, ("2024-06-01", "2024-06-30"), tile_split_shape=2)
    _check_cells(cells, TRIANGLE, ["tile_id"])
    assert cells.groupby("tile_id").size().to_dict() == {"11SLT": 2, "11SMT": 2}


def _scene(**over):
    row = {"id": "S2A_T11SLT_20230601T183921_L2A", "datetime": "2023-06-01T18:39:21Z", "tile_id": "11SLT",
           "platform": "S2A", "cloud_cover": 3.0, "nodata_pct": 0.0, "crs": "EPSG:32611", "source": "coop",
           "offset": -0.1, "assets": '{"red": "https://x/B04.tif", "cloud": null}',
           "geometry": shapely.box(-119, 34, -118, 35)}
    row.update(over)
    return pd.DataFrame([row])


def test_scene_table_merge_and_rank(su, mod):
    cat = mod["su_catalog"]
    legacy = _scene(id="S2A_11SLT_20230601_0_L2A", source="legacy", offset=0.0)
    out = cat._finish([legacy, _scene(), _scene()], "2023-06-01", "2023-06-01", shapely.box(*LA))
    assert list(out.index) == ["S2A_T11SLT_20230601T183921_L2A"] and list(out.columns) == SCENE_COLUMNS
    assert out.iloc[0]["offset"] == -0.1 and out.iloc[0]["assets"] == {"red": "https://x/B04.tif"}
    rows = [_scene(id=f"a{i}", tile_id=t, cloud_cover=c, datetime=f"2023-06-0{i + 1}T18:00:00Z")
            for i, (t, c) in enumerate([("11SLT", 50), ("11SLT", 5), ("11SKT", 20)])]
    products = cat._finish(rows, "2023-06-01", "2023-06-09", shapely.box(*LA))
    ranked = su.rank(products, per_tile=1)
    assert list(ranked["id"]) == ["a1", "a2"] and list(ranked["clear_pct"]) == [95.0, 80.0]
    by_date = su.rank(products, date="2023-06-01", order="date")
    assert list(by_date["id"]) == ["a0", "a2", "a1"] and list(by_date["days_off"]) == [0, 2, 1]


def test_payload_round_trip(su):
    from odc.geo.geobox import GeoBox

    gb = GeoBox.from_bbox((0, 0, 300, 200), crs="EPSG:32611", resolution=10)
    ds = xr.Dataset({"B04": (("time", "y", "x"), np.random.default_rng(0).random((2, 20, 30), dtype="float32")),
                     "dataMask": (("time", "y", "x"), np.ones((2, 20, 30), "uint8"))},
                    coords={"time": pd.to_datetime(["2023-06-01", "2023-06-06"]).values,
                            "y": gb.coords["y"].values, "x": gb.coords["x"].values},
                    attrs={"resolution": 10.0, "scenes": 2}).odc.assign_crs("EPSG:32611")
    back = su.from_payload(su.to_payload(ds))
    xr.testing.assert_identical(back, ds)
    assert back.odc.crs.epsg == 32611
    arr = np.arange(24, dtype="uint16").reshape(2, 3, 4)
    assert np.array_equal(su.from_payload(su.to_payload(arr)), arr)
    assert su.from_payload(su.to_payload(b"\x89PNG")) == b"\x89PNG"
    frame = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    pd.testing.assert_frame_equal(su.from_payload(su.to_payload(frame)), frame)
    assert list(su.to_payload(frame).columns) == ["kind", "payload", "meta"]


def test_collection_chain_compiles_to_requests(su):
    summer = ("2024-06-01", "2024-08-31")
    chain = (su.collection().filter_bounds(LA).filter_date(*summer).filter_metadata("cloud_cover", "less_than", 20)
             .median().normalized_difference(["B08", "B04"]))
    assert chain.to_request() == su.process_request(LA, summer, cloud_cover=20, reducer="median", index="NDVI")
    dem = su.collection("cop-dem-glo-30").filter_bounds(LA).mosaic()
    assert dem.to_request() == su.process_request(LA, None, collection="cop-dem-glo-30")
    months = su.collection().filter_bounds(LA).filter_date("2024-01-01", "2024-12-31").aggregate("P1M")
    assert len(months) == 12 and months.to_request()[1]["date"] == ["2024-02-01", "2024-02-29"]
    with pytest.raises(ValueError, match="sentinel-2-l2a"):
        su.collection("landsat")


def test_embedding_year(su):
    assert su.embedding_year("2020-07-15") == 2020 and su.embedding_year(("2023-01-01", "2023-12-31")) == 2023
    with pytest.raises(ValueError, match="2017"):
        su.embedding_year(2016)


def _fake_get_data(mod, monkeypatch, delays):
    download = mod["su_download"]

    def fake(request):
        delay = delays[request["date"]]
        if delay is None:
            raise download.constants.NoDataError(f"no scene on {request['date']}")
        time.sleep(delay)
        return request["date"]

    monkeypatch.setitem(download.download.__globals__, "get_data", fake)
    return [download.process.process_request(LA, day, resolution=60) for day in delays]


def test_iter_download_gives_results_as_they_finish(su, mod, monkeypatch):
    requests = _fake_get_data(mod, monkeypatch, {"2023-06-01": 0.3, "2023-06-02": 0.0, "2023-06-03": 0.1})
    got = list(su.iter_download(requests, engine="local", max_workers=3))
    assert [i for i, _ in got] == [1, 2, 0] and dict(got) == {0: "2023-06-01", 1: "2023-06-02", 2: "2023-06-03"}
    assert su.download(requests, engine="local") == ["2023-06-01", "2023-06-02", "2023-06-03"]
    assert list(su.iter_download([], engine="local")) == []
    with pytest.raises(ValueError, match="'remote', 'local'"):
        su.iter_download(requests, engine="threads")


def test_iter_download_errors(su, mod, monkeypatch):
    requests = _fake_get_data(mod, monkeypatch, {"2023-06-01": 0.0, "2023-06-02": None})
    with pytest.raises(RuntimeError, match="request 1 of 2 failed: NoDataError: no scene on 2023-06-02"):
        list(su.iter_download(requests, engine="local", max_workers=1))
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        got = dict(su.iter_download(requests, engine="local", max_workers=1, ignore_errors=True))
    assert got == {0: "2023-06-01", 1: None}
    assert [str(w.message) for w in seen if "failed" in str(w.message)] == [
        "1 of 2 requests failed; first: request 1: NoDataError: no scene on 2023-06-02"]
    with pytest.raises(RuntimeError, match="1 of 2 requests failed; first: request 1: NoDataError"):
        su.download(requests, engine="local")


def test_iter_download_stops_fast(su, mod, monkeypatch):
    requests = _fake_get_data(mod, monkeypatch, {"2023-06-01": 0.0, "2023-06-02": 0.5, "2023-06-03": 0.5})
    stream = su.iter_download(requests, engine="local", max_workers=1)
    assert next(stream) == (0, "2023-06-01")
    t0 = time.time()
    stream.close()
    assert time.time() - t0 < 0.1


def _fake_pool(mod, monkeypatch, outcomes):
    download = mod["su_download"]
    calls = []

    class Pool:
        def __init__(self, args):
            self.results = {j: outcomes[json.loads(a["request"])["date"]].pop(0) for j, a in enumerate(args)}

        def results_now(self, return_exceptions=True):
            return dict(self.results)

        def cancel(self, wait=True):
            pass

    class Worker:
        def map(self, args, **kwargs):
            calls.append((len(args), kwargs["max_retry"]))
            return Pool(args)

    monkeypatch.setitem(download.download.__globals__, "worker", lambda: Worker())
    monkeypatch.setitem(download.download.__globals__, "from_payload", lambda got: got)
    return calls


def test_remote_job_runs_one_more_time_after_a_failure(su, mod, monkeypatch):
    requests = _fake_get_data(mod, monkeypatch, {"2023-06-01": 0.0, "2023-06-02": 0.0, "2023-06-03": 0.0})
    calls = _fake_pool(mod, monkeypatch, {
        "2023-06-01": ["a"],
        "2023-06-02": [TimeoutError("worker timed out"), "b"],
        "2023-06-03": [RuntimeError("NoDataError: no scene"), "never"],
    })
    assert dict(su.iter_download(requests, ignore_errors=True)) == {0: "a", 1: "b", 2: None}
    assert calls == [(3, 0), (1, 0)]


def test_remote_second_failure_is_final(su, mod, monkeypatch):
    requests = _fake_get_data(mod, monkeypatch, {"2023-06-01": 0.0, "2023-06-02": 0.0})
    calls = _fake_pool(mod, monkeypatch, {
        "2023-06-01": ["a"],
        "2023-06-02": [TimeoutError("worker timed out"), TimeoutError("worker timed out again")],
    })
    with pytest.raises(RuntimeError, match="request 1: TimeoutError: worker timed out again"):
        su.download(requests)
    assert calls == [(2, 0), (1, 0)]


@live
def test_live_query_los_angeles(su):
    products = _timed("query LA June 2024", lambda: su.query(LA, ("2024-06-01", "2024-06-30")))
    assert len(products) > 5 and set(products["tile_id"]) == {"11SLT"} and list(products.columns) == SCENE_COLUMNS
    assert products["datetime"].is_monotonic_increasing and products["crs"].eq("EPSG:32611").all()
    summer = su.query(LA, ("2023-06-01", "2023-08-31"), cloud_cover=20)
    assert list(summer.index) == ["S2A_T11SLT_20230622T184217_L2A", "S2B_T11SLT_20230627T183917_L2A",
                                  "S2A_T11SLT_20230712T183436_L2A", "S2B_T11SLT_20230727T183842_L2A",
                                  "S2B_T11SLT_20230826T184452_L2A"]


@live
def test_live_get_data_one_day(su):
    ds = _timed("get_data 60 m local", lambda: su.get_data(su.process_request(LA, "2023-06-22", resolution=60)))
    assert list(ds.data_vars) == ["B04", "B03", "B02", "dataMask"] and dict(ds.sizes) == {"y": 152, "x": 188}
    assert ds.odc.crs.epsg == 32611 and float(ds["B04"].mean()) > 0.03


@live
def test_live_no_data_over_the_ocean(su, mod):
    with pytest.raises(mod["su_constants"].NoDataError):
        su.get_data(su.process_request(OCEAN, "2023-06-10", resolution=60))


@live
def test_live_remote_run_equals_local(su):
    request = su.process_request(LA, "2023-06-22", resolution=60)
    payload = _timed("fused.run remote, 1 request", lambda: fused.run(su, request=json.dumps(request),
                                                                      engine="remote", cache_max_age="0s"))
    remote = su.from_payload(payload)
    assert isinstance(remote, xr.Dataset) and remote.identical(su.get_data(request))
    listing = fused.run(su, engine="remote", cache_max_age="0s")
    assert len(listing) == 76 and "process_request" in set(listing["name"])


@live
def test_live_download_remote_in_order(su):
    months = [("2023-06-01", "2023-06-30"), ("2023-07-01", "2023-07-31"), ("2023-08-01", "2023-08-31")]
    requests = [su.process_request(LA, m, resolution=60) for m in months]
    remote = _timed("download remote, 3 requests", lambda: su.download(requests, engine="remote"))
    assert [ds.attrs["date"] for ds in remote] == [str(list(m)) for m in months]
    local = _timed("download local threads, 3 requests", lambda: su.download(requests, engine="local"))
    assert all(a.identical(b) for a, b in zip(remote, local))


@live
def test_live_iter_download_remote(su):
    months = [("2023-06-01", "2023-06-30"), ("2023-07-01", "2023-07-31"), ("2023-08-01", "2023-08-31")]
    requests = [su.process_request(LA, m, resolution=60) for m in months]
    t0 = time.time()
    got, first = {}, None
    for index, result in su.iter_download(requests, engine="remote"):
        first = first or time.time() - t0
        got[index] = result
    TIMES["iter_download remote, first of 3"], TIMES["iter_download remote, all 3"] = first, time.time() - t0
    assert set(got) == {0, 1, 2} and all(got[i].identical(su.get_data(r)) for i, r in enumerate(requests))
    bad = su.process_request(OCEAN, "2023-06-10", resolution=60)
    with pytest.raises(RuntimeError, match="request 1 of 2 failed: .*NoDataError"):
        list(su.iter_download([requests[0], bad], engine="remote"))


@live
def test_live_statistical_request(su):
    request = su.statistical_request(FIELD, ("2024-06-01", "2024-06-30"), index="NDVI", aggregation_interval="P1M")
    local = _timed("statistical request local", lambda: su.get_data(request))
    assert list(local["output"]) == ["NDVI"] and list(local["interval_from"]) == ["2024-06-01"]
    assert 0.0 < local["mean"].iloc[0] <= 1.0 and local["sample_count"].iloc[0] > 0
    remote = su.from_payload(fused.run(su, request=json.dumps(request), engine="remote", cache_max_age="0s"))
    assert remote["mean"].tolist() == local["mean"].tolist()
