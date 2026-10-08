<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Fused_Logo.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `sentinel-2` `raster` `stac` `spectral-index` `composite` `tiles` `utilities`

<!--fused:readme-->
## Overview

Utilities to search, load, mask, index, composite, analyse, classify, render, tile and export Sentinel-2 L2A from the cloud. Every function is a plain Python function. Load the file once and call the functions, as with `common`.

The UDF of this file is a world map tile layer: the least cloudy Sentinel-2 pixels for any place and date, made on request and not stored.

## Quick start

```python
@fused.udf
def udf(aoi: str = "-73.99,40.76,-73.95,40.80"):
    s2 = fused.load("https://github.com/fusedio/udfs/tree/<commit>/public/sentinel_utils/")
    summer = s2.image(aoi, "2024-06-01", "2024-08-31", s2.needs("NDVI"), res=20)
    ndvi = s2.index(summer, "NDVI")
    return float(ndvi.mean())
```

A tile UDF of your own:

```python
@fused.udf
def udf(bounds: fused.types.Bounds, date: str = "2024-07-01", what: str = "NDVI"):
    s2 = fused.load("https://github.com/fusedio/udfs/tree/<commit>/public/sentinel_utils/")
    z, x, y = s2.bounds_to_tile(bounds)
    return s2.tile(z, x, y, date, what=what)
```

Pin a commit in the URL. Then your UDF does not change when this file changes.

## Functions

| Group | Functions |
|---|---|
| Bands and places | `band_name`, `band_names`, `band_info`, `is_mask`, `scl_classes`, `bbox`, `date_window`, `frequency`, `utm_crs`, `size_m`, `auto_res`, `geobox`, `on_grid`, `keep_crs`, `reflectance`, `nanquantile`, `save_file`, `BestFill`, `NoData` |
| Search | `search(aoi, start, end)`, `rank`, `dates`, `to_json`, `from_json` |
| Load and mask | `load(scenes, bands, aoi, keep=)`, `mask`, `clear_mask`, `aoi_cloud`, `clear_scenes`, `scl_summary`, `pixel_series` |
| Indices | `index(ds, what)`, `indices`, `needs`, `is_index`, `index_catalog`, `index_info`, `list_indices`, `parse_formula` |
| Composites | `composite(ds, method, period)`, `clear_count`, `image(aoi, start, end, bands)`, `cube(aoi, start, end, what)` |
| Statistics | `zonal_stats`, `zone_labels`, `sample`, `change`, `class_area` |
| Classification | `features`, `cluster`, `train`, `predict`, `vote`, `labels_worldcover`, `labels_from_zones`, `vectorize` |
| Time series | `regularize`, `fill_gaps`, `smooth`, `harmonic`, `phenology`, `phenology_map`, `climatology`, `anomaly` |
| Embeddings (AlphaEarth) | `load_embeddings`, `embedding_at`, `embedding_of`, `embedding_similarity`, `embedding_difference`, `embedding_preview` |
| Render | `render(ds, what)`, `render_style`, `stretch`, `colorize`, `colorize_classes`, `css_gradient` |
| Map tiles | `tile(z, x, y, date)`, `tile_scenes`, `mosaic_tile`, `read_tile`, `bounds_to_tile` |
| Export | `export(aoi, date)`, `to_cog`, `to_h3` |
| Large areas | `block_grid`, `run_fanout`, `worker_udf`, `run_blocks`, `save_tiles`, `block_worker`, `tile_worker` |
| Timelapse | `timelapse(aoi, start, end)`, `period_ranges`, `animate`, `animation_html`, `frame_worker` |

## Examples

```python
s2 = fused.load("https://github.com/fusedio/udfs/tree/<commit>/public/sentinel_utils/")
box = [-118.62, 34.02, -118.50, 34.10]

scenes = s2.search(box, "2024-06-01", "2024-08-31")
best = s2.rank(scenes, box, date="2024-07-15", per_mgrs=2)
cloud = s2.aoi_cloud(scenes, box)

ds = s2.load(best, "red,nir", box, res=20, keep="clear")
ndvi = s2.index(ds, "NDVI")
water = s2.index(ds, "(G - S1) / (G + S1) > 0", name="water")

before = s2.image(box, "2024-11-15", "2025-01-06", "nir,swir22", res=20, keep="land")
after = s2.image(box, "2025-01-20", "2025-03-15", "nir,swir22", res=20, keep="land")
burn = s2.change(before, after, "burn")
km2 = s2.class_area(burn["class"])

monthly = s2.cube(box, "2020-01-01", "2024-12-31", "NDVI", period="month", res=60)
z = s2.anomaly(monthly, baseline=[2020, 2021, 2023], target_years=[2024])

info = s2.export(box, "2024-07-01", kind="bands", bands="blue,green,red,nir")
anim, frames = s2.timelapse(box, "2024-10-01", "2025-06-30", period="month")
```

## The standard

Follow these rules when you add a function. Then people can combine any two functions without reading the code.

**Parameters**

| Meaning | Name | Notes |
|---|---|---|
| place | `aoi` | `'w,s,e,n'`, a list, a shapely geometry or a GeoDataFrame. Only tile functions take `bounds` or `z, x, y`. |
| time range | `start`, `end` | `YYYY-MM-DD`, inclusive |
| time window | `date`, `days` | `date` ± `days` |
| grouping | `period` | `month`, `quarter`, `season`, `year`, or a pandas frequency |
| bands | `bands` | a list or a comma string; any spelling (B04, red, R) |
| what to compute or draw | `what` | a preset, a catalog index or a formula |
| reducer | `method` | |
| SCL classes to keep | `keep` | `clear`, `land`, `water`, ... |
| quality | `max_cloud`, `min_clear` | percent |
| grid | `res` (m), `crs`, `shape`, `like` | `like` takes the grid of another result |
| work | `threads` (in process), `max_workers` (`fused.submit`) | |

- The first 3 or 4 parameters are positional. All others are keyword-only.
- Library code uses `None` for "not set". Only UDF parameters use `""`.
- Defaults are module constants: `MAX_CLOUD = 80`, `DAYS = 30`, `MAX_PIXELS`.

**Return types**

| Result | Type |
|---|---|
| scenes | a GeoDataFrame with the columns of `SCENE_COLUMNS` (also when empty) |
| bands | `xarray.Dataset` (time, y, x), reflectance, with the CRS attached |
| one quantity | `xarray.DataArray` |
| tables | a long DataFrame; `time` is datetime64; percents end in `_pct` |
| images | uint8 RGBA (4, H, W) |
| class rasters | carry their class names in `attrs["labels"]` |
| two results | a tuple only as `(result, report)` |

**Errors**
- A bad argument raises `ValueError` that lists the allowed values.
- "No scene" or "no valid pixel" raises `NoData`, which is a `ValueError`.
- Catch only specific exceptions. Library code warns; it never prints.

**Code**
- Imports go inside function bodies, so the file loads fast and anywhere.
- Private helpers start with `_`. Workers for `fused.submit` end in `_worker`, carry `@fused.udf(cache_max_age=...)`, and never take parameters named `x`, `y` or `z` (Fused reads those as a map tile call).
- `run_fanout("block_worker", args)` sends the whole file with each worker, so a worker can call every function here.
- "Tile" means a map tile only. A Sentinel-2 granule is an `mgrs` tile.

## Data rules

1. **Sources.** `search` uses the Element84 STAC API for Collection 1 (about 0.5 s). If that API fails, it reads the source.coop STAC GeoParquet. Collection 1 is almost empty for January to November 2022, so that window comes from the older `sentinel-2-l2a` collection, or from a Fused-hosted index if that API fails. Where two sources have the same acquisition, the better source wins.
2. **Offset.** Each scene row has its own `offset`. Reflectance = DN × 0.0001 + offset. Collection 1 DNs include +1000, so its offset is −0.1. Legacy DNs do not, so their offset is 0, although their STAC metadata gives −0.1. `load` applies the offset per scene before it merges the scenes of one local solar day.
3. **AOI cloud.** A scene's `cloud_cover` is for the whole 110 km tile. `aoi_cloud` and `clear_scenes` measure the clear share inside your AOI from SCL.
4. **Masks.** `keep="clear"` keeps SCL 4, 5 and 6. `dilate` grows cloud (SCL 8, 9, 10) only. `rescue` gives back SCL 1, 2, 3, 7 and 11 to pixels that are never clear (building shadows, bright roofs), so composites have no holes. Use `keep="land"` for burn or vegetation change.
5. **Formulas.** `index` parses formulas with an allow-list and never calls `eval`. The result is NaN wherever an input band is NaN, also for comparisons. The catalog is Awesome Spectral Indices at a pinned commit.
6. **Tiles and exports.** Scenes are ranked once per zoom-8 region by clear area, (100 − nodata %) × (100 − cloud %). Each MGRS tile's best scene comes before any second best, so no seam shows at tile edges. Cloud (SCL 8, 9, 10) is filled from the next scene. Cloud shadow is not masked, because SCL marks dark water as shadow. Reading stops at 99.5 % clear (97 % below zoom 9). Exported band files store DN with scale 0.0001 and offset −0.1, so GIS tools read reflectance.
7. **Size guards.** `load` stops above 200 M values. `export` is limited to 4096 px. Use a coarser `res`, `rank(per_mgrs=)`, `clear_scenes`, or `cube` for long time ranges.

## Speed (remote engine)

| Step | Time |
|---|---|
| scene search, new / cached | 0.4–0.6 s / 0.05 s |
| one map tile, zoom 11–15 | 0.3–1.8 s |
| one map tile, zoom 7 | 5–6.5 s (the tile layer caches results for 7 days) |
| export of a 15 × 13 km box at 10 m | about 6 s |

A point series is limited by bandwidth: a 10 m band is stored in 1024 × 1024 blocks (about 1.8 MB), and one pixel costs one block per band and date. `pixel_series(clear_only=True)` reads the small SCL blocks first and skips cloudy dates.

## Tests

`tests/test_sentinel_utils.py` holds fast tests on synthetic data and live tests on real data:

```bash
pytest tests/test_sentinel_utils.py
SENTINEL_UTILS_LIVE=1 pytest tests/test_sentinel_utils.py
```

## External links

- [Sentinel-2 L2A on AWS (Element84 Earth Search)](https://element84.com/earth-search/)
- [Sentinel-2 STAC GeoParquet on source.coop](https://source.coop/tge-labs/s2-stac-geoparquet)
- [Awesome Spectral Indices](https://github.com/awesome-spectral-indices/awesome-spectral-indices)
- [AlphaEarth Foundations embeddings on source.coop](https://source.coop/tge-labs/aef)
- [ESA WorldCover](https://esa-worldcover.org/)
