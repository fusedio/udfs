<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/Fused_Logo.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `sentinel-2` `raster` `stac` `dem` `embeddings` `spectral-index` `utilities`

<!--fused:readme-->
## Overview

`sentinel_utils` reads Sentinel-2 L2A, the Copernicus DEM (30 m) and AlphaEarth embeddings on Fused. The API follows
[sentinelhub-py](https://github.com/sentinel-hub/sentinelhub-py): process and statistical requests, area splitters
and a download client. The search follows sentinelsat. All names are snake_case.

`sentinel_utils.py` is the main file. The 12 `su_*.py` files are modules. `meta.json` lists them as headers, so
`fused.load` of this folder loads all of them. Each function of the main file calls the function of the same name in
its module. `su.module("su_constants")` gives one module.

## Quick start

Always pin a commit in the URL. Then your code does not change when this folder changes.

```python
import json
import fused

su = fused.load("https://github.com/fusedio/udfs/tree/<commit>/public/sentinel_utils/")
area = [-118.62, 34.02, -118.50, 34.10]

scenes = su.query(area, ("2024-06-01", "2024-08-31"), cloud_cover=20)
req = su.process_request(area, ("2024-06-01", "2024-08-31"), ["B04", "B03", "B02"], resolution=10,
                         mask="clear", reducer="median")
ds = su.get_data(req)
stats = su.get_data(su.statistical_request(area, ("2024-01-01", "2024-12-31"), index="NDVI",
                                           aggregation_interval="P1M"))
ndvi = su.collection().filter_bounds(area).filter_date("2024-06-01", "2024-08-31").median().index("NDVI").get_data()
su.api()
```

`su.api()` gives a table of the 76 public functions with their signatures. `fused.run(su)` gives the same table.

## Modules

| Module | Functions |
|---|---|
| `su_constants` | collections, band names, `MASKS`, `NoDataError`: `band_names`, `band_info`, `collection_info` |
| `su_geometry` | `to_bbox`, `to_geometry`, `get_utm_crs`, `bbox_to_dimensions`, `make_geobox`, `area_km2` |
| `su_time` | `parse_time`, `parse_time_interval`, `filter_times`, `split_time_interval` |
| `su_catalog` | `query`, `count`, `get_product`, `get_timestamps`, `to_geojson`, `rank` |
| `su_reader` | pixel engine: `read_stack`, map `tile`, `export`, `to_cog`, `render` |
| `su_process` | `process_request`, `run_process`: mosaics, composites, indices, DEM, embeddings |
| `su_statistical` | `statistical_request`, `run_statistical`: statistics per interval in a polygon |
| `su_areas` | splitters: `bbox_splitter`, `osm_splitter`, `utm_grid_splitter`, `utm_zone_splitter`, `tile_splitter`, `custom_grid_splitter` |
| `su_indices` | NDVI, NDWI, MNDWI, NDMI, NBR, NDSI, NDRE, EVI, SAVI, BSI; `normalized_difference` |
| `su_embeddings` | AlphaEarth: `load_embeddings`, `embedding_similarity`, `embedding_difference`, `embedding_preview` |
| `su_download` | `get_data`, `download`, `iter_download`, `save_data`, `to_payload`, `from_payload` |
| `su_collection` | `collection()`: a lazy chain in Earth Engine style |

## Parameters

Each name has the same meaning in all functions.

| Name | Values |
|---|---|
| `area` | place in EPSG:4326: `"w,s,e,n"`, `[w, s, e, n]`, shapely geometry, GeoDataFrame or GeoJSON dict. Split an area that crosses 180°. |
| `date` | one day `"2024-07-01"`, or `(start, end)`; also `"20240701"`, `"NOW"`, `"NOW-30DAYS"` |
| `bands` | list or comma string in any spelling: `B04`, `red`, `b4`; also `SCL`, `CLD`, `SNW`, `dataMask` |
| `collection` | `su.SENTINEL2_L2A` (default), `su.DEM`, `su.AEF` (`date` is the year) |
| `resolution` / `size` / `crs` | metres / `(width, height)` px / default is the UTM zone of the area |
| `cloud_cover` | maximum scene cloud in percent, 0 to 100 |
| `mosaic_order` | `least_cloudy` (default), `newest`, `oldest` |
| `mask` | `clear` (SCL 4, 5, 6), `land`, `water`, `snow`, ...; other pixels become NaN |
| `reducer` | `median`, `mean`, `min`, `max`, `count` over all dates |
| `index` | `NDVI`, `NDWI`, `MNDWI`, `NDMI`, `NBR`, `NDSI`, `NDRE`, `EVI`, `SAVI`, `BSI` |
| `output` | `dataset` (default), `array` (H, W, B), `geotiff` (COG bytes), `png` |
| `aggregation_interval` | ISO 8601: `P1D` (each acquisition), `P10D`, `P1M`, `P1Y` |

A bad value raises `ValueError`, and the message lists the allowed values. No scene or no valid pixel raises
`NoDataError` (a `ValueError`). Indices use reflectance clipped at 0.

## Many requests

A request is a plain JSON dict. Make it only with `process_request` or `statistical_request`. Workers refuse other
dicts.

```python
results = su.download([req_1, req_2, req_3])
for i, result in su.iter_download([req_1, req_2, req_3]):
    show(i, result)
```

- `download` runs the requests in parallel on remote workers (`.map`) and gives the results in request order.
- `iter_download` gives `(i, result)` when request `i` is done, so the first result shows before the last is ready.
  Use `break` to stop. The open workers stop too.
- A remote job that fails (for example a timeout) runs one more time. A `NoDataError` or a `ValueError` is final.
  A request that still fails raises `RuntimeError` ("request i of n failed: ..."). With `ignore_errors=True` you get
  `None` for that request and one warning at the end.
- Keep one request under about 60 s. Use a coarser `resolution` or a smaller `size`, or split the area with
  `su.utm_zone_splitter(area, 20_000)`.

## Run one request on Fused

```python
result = su.from_payload(fused.run(su, request=json.dumps(req), engine="remote"))
```

`fused.run` gives a small table. `su.from_payload` makes the same object that `su.get_data(req)` gives. For this
path your app needs only `fused`, pandas, shapely, odc-geo, xarray and nest_asyncio. A UDF that runs on Fused cannot
load a local path: use the GitHub URL.

## Accounts

All work runs on the Fused account of the caller: images, statistics, `download` workers, caches and saved files.
The package needs no UDF, token, secret or file of the author. `fd://` paths (for example `save_data`) are in the
team bucket of the caller. Do not call the share links of a workbench canvas from an app. Those run as the canvas
owner.

## Examples

The workbench canvas [Sentinel2_Toolkit](https://www.fused.io/workbench/aman/Sentinel2_Toolkit) has examples (ports
of the sentinelhub-py examples, time series, timelapse, classification, change detection, embeddings) and a world
viewer.

## Tests

```bash
pytest tests/test_sentinel_utils.py
SENTINEL_UTILS_LIVE=1 pytest tests/test_sentinel_utils.py
```

## Data and credits

- Scene search: [Earth Search by Element84](https://element84.com/earth-search/) first, then the
  [Sentinel-2 STAC GeoParquet on source.coop](https://source.coop/tge-labs/s2-stac-geoparquet). Collection 1 and the
  older collection are merged; Collection 1 wins for the same acquisition.
- Imagery: "Contains modified Copernicus Sentinel data [year]".
- Terrain: Copernicus DEM GLO-30.
- Embeddings: [AlphaEarth Foundations embeddings on source.coop](https://source.coop/tge-labs/aef).
