"""sentinel_utils: Sentinel-2 L2A, Copernicus DEM and AlphaEarth embeddings on Fused, in the style of sentinelhub-py.

Load it one time, then call the functions on it:
    su = fused.load("sentinel_utils")                     in the workbench canvas
    su = fused.load("<GitHub URL or local folder>")       outside the workbench (the package folder with meta.json)
    products = su.query([-118.62, 34.02, -118.50, 34.10], ("2024-06-01", "2024-06-30"), cloud_cover=30)
    ds = su.get_data(su.process_request([-118.62, 34.02, -118.50, 34.10], "2023-06-22", resolution=60))
    tables = su.download([su.statistical_request(field, ("2024-01-01", "2024-12-31"), index="NDVI") for field in fields])
    ndvi = su.collection().filter_bounds(area).filter_date("2024-06-01", "2024-08-31").median().index("NDVI")
One request on a Fused worker: su.from_payload(fused.run(su, request=json.dumps(request), engine="remote")).
su.api() lists every public function. su.module("su_constants") gives one module (MASKS, NoDataError, ...).
Each function here calls the function of the same name in its module. Outside the canvas the modules are headers
(meta.json). In the canvas, module() fetches the code of the 12 nodes one time and imports them as headers too, because
fused.load by name loads every sibling again (su_download alone makes 60 loads, about 13 s).
"""

MODULES = ("su_constants", "su_geometry", "su_time", "su_catalog", "su_reader", "su_process", "su_statistical",
           "su_areas", "su_indices", "su_embeddings", "su_download", "su_collection")
_HELD = {}

try:
    import su_constants
    import su_geometry
    import su_time
    import su_catalog
    import su_reader
    import su_process
    import su_statistical
    import su_areas
    import su_indices
    import su_embeddings
    import su_download
    import su_collection
    _HELD.update({name: globals()[name] for name in MODULES})
except ImportError:
    pass


@fused.udf
def udf(request: str = ""):
    """Returns the table of public functions; with the JSON of a request, su_download.to_payload of its result."""
    import json

    if not request:
        return api()
    download = module("su_download")
    return download.to_payload(download.get_data(json.loads(request)))


def module(name):
    """Returns the module `name` (one of MODULES), for example module("su_constants").MASKS."""
    if name not in MODULES:
        raise ValueError(f"module must be one of {list(MODULES)}, got {name!r}")
    if not _HELD:
        _HELD.update(_canvas_modules())
    return _HELD[name]


def api():
    """Returns a DataFrame of the public functions of every module: module, name, signature, doc."""
    import pandas as pd

    tables = [module(name).api().assign(module=name) for name in MODULES]
    return pd.concat(tables, ignore_index=True)[["module", "name", "signature", "doc"]]


def _canvas_modules():
    from concurrent.futures import ThreadPoolExecutor

    if _canvas_name() is None:
        raise ValueError("the sentinel_utils modules are not loaded and this is not a canvas run: load the package "
                         "folder or GitHub URL with fused.load, not sentinel_utils.py alone")
    with ThreadPoolExecutor(len(MODULES)) as pool:
        codes = list(pool.map(lambda name: fused.load(name, import_globals=False).code, MODULES))
    package = fused.models.Udf.model_validate(
        {"name": "sentinel_utils", "type": "geopandas_v2", "entrypoint": "udf",
         "code": "\n".join(f"import {name}" for name in MODULES),
         "headers": [{"module_name": name, "source_code": code} for name, code in zip(MODULES, codes)]},
        context={"import_globals": True})
    return {name: getattr(package, name) for name in MODULES}


def _canvas_name():
    try:
        name = fused.context.this_udf().collection_name
    except ValueError:
        return None
    return None if name in (None, "default") else name


def get_available_collections(*args, **kwargs):
    return module("su_constants").get_available_collections(*args, **kwargs)


def collection_info(*args, **kwargs):
    return module("su_constants").collection_info(*args, **kwargs)


def band_names(*args, **kwargs):
    return module("su_constants").band_names(*args, **kwargs)


def band_info(*args, **kwargs):
    return module("su_constants").band_info(*args, **kwargs)


def to_bbox(*args, **kwargs):
    return module("su_geometry").to_bbox(*args, **kwargs)


def to_geometry(*args, **kwargs):
    return module("su_geometry").to_geometry(*args, **kwargs)


def read_geojson(*args, **kwargs):
    return module("su_geometry").read_geojson(*args, **kwargs)


def geojson_to_wkt(*args, **kwargs):
    return module("su_geometry").geojson_to_wkt(*args, **kwargs)


def get_utm_crs(*args, **kwargs):
    return module("su_geometry").get_utm_crs(*args, **kwargs)


def transform_bbox(*args, **kwargs):
    return module("su_geometry").transform_bbox(*args, **kwargs)


def to_utm_bbox(*args, **kwargs):
    return module("su_geometry").to_utm_bbox(*args, **kwargs)


def bbox_to_dimensions(*args, **kwargs):
    return module("su_geometry").bbox_to_dimensions(*args, **kwargs)


def bbox_to_resolution(*args, **kwargs):
    return module("su_geometry").bbox_to_resolution(*args, **kwargs)


def area_km2(*args, **kwargs):
    return module("su_geometry").area_km2(*args, **kwargs)


def make_geobox(*args, **kwargs):
    return module("su_geometry").make_geobox(*args, **kwargs)


def parse_time(*args, **kwargs):
    return module("su_time").parse_time(*args, **kwargs)


def parse_time_interval(*args, **kwargs):
    return module("su_time").parse_time_interval(*args, **kwargs)


def serialize_time(*args, **kwargs):
    return module("su_time").serialize_time(*args, **kwargs)


def filter_times(*args, **kwargs):
    return module("su_time").filter_times(*args, **kwargs)


def split_time_interval(*args, **kwargs):
    return module("su_time").split_time_interval(*args, **kwargs)


def query(*args, **kwargs):
    return module("su_catalog").query(*args, **kwargs)


def count(*args, **kwargs):
    return module("su_catalog").count(*args, **kwargs)


def get_product(*args, **kwargs):
    return module("su_catalog").get_product(*args, **kwargs)


def get_timestamps(*args, **kwargs):
    return module("su_catalog").get_timestamps(*args, **kwargs)


def to_geojson(*args, **kwargs):
    return module("su_catalog").to_geojson(*args, **kwargs)


def to_dataframe(*args, **kwargs):
    return module("su_catalog").to_dataframe(*args, **kwargs)


def get_collections(*args, **kwargs):
    return module("su_catalog").get_collections(*args, **kwargs)


def rank(*args, **kwargs):
    return module("su_catalog").rank(*args, **kwargs)


def read_stack(*args, **kwargs):
    return module("su_reader").read_stack(*args, **kwargs)


def tile(*args, **kwargs):
    return module("su_reader").tile(*args, **kwargs)


def export(*args, **kwargs):
    return module("su_reader").export(*args, **kwargs)


def to_cog(*args, **kwargs):
    return module("su_reader").to_cog(*args, **kwargs)


def render(*args, **kwargs):
    return module("su_reader").render(*args, **kwargs)


def colorize(*args, **kwargs):
    return module("su_reader").colorize(*args, **kwargs)


def process_request(*args, **kwargs):
    return module("su_process").process_request(*args, **kwargs)


def run_process(*args, **kwargs):
    return module("su_process").run_process(*args, **kwargs)


def statistical_request(*args, **kwargs):
    return module("su_statistical").statistical_request(*args, **kwargs)


def run_statistical(*args, **kwargs):
    return module("su_statistical").run_statistical(*args, **kwargs)


def bbox_splitter(*args, **kwargs):
    return module("su_areas").bbox_splitter(*args, **kwargs)


def osm_splitter(*args, **kwargs):
    return module("su_areas").osm_splitter(*args, **kwargs)


def utm_grid_splitter(*args, **kwargs):
    return module("su_areas").utm_grid_splitter(*args, **kwargs)


def utm_zone_splitter(*args, **kwargs):
    return module("su_areas").utm_zone_splitter(*args, **kwargs)


def tile_splitter(*args, **kwargs):
    return module("su_areas").tile_splitter(*args, **kwargs)


def custom_grid_splitter(*args, **kwargs):
    return module("su_areas").custom_grid_splitter(*args, **kwargs)


def normalized_difference(*args, **kwargs):
    return module("su_indices").normalized_difference(*args, **kwargs)


def compute_index(*args, **kwargs):
    return module("su_indices").compute_index(*args, **kwargs)


def required_bands(*args, **kwargs):
    return module("su_indices").required_bands(*args, **kwargs)


def embedding_year(*args, **kwargs):
    return module("su_embeddings").embedding_year(*args, **kwargs)


def load_embeddings(*args, **kwargs):
    return module("su_embeddings").load_embeddings(*args, **kwargs)


def embedding_at(*args, **kwargs):
    return module("su_embeddings").embedding_at(*args, **kwargs)


def embedding_of(*args, **kwargs):
    return module("su_embeddings").embedding_of(*args, **kwargs)


def embedding_similarity(*args, **kwargs):
    return module("su_embeddings").embedding_similarity(*args, **kwargs)


def embedding_difference(*args, **kwargs):
    return module("su_embeddings").embedding_difference(*args, **kwargs)


def embedding_preview(*args, **kwargs):
    return module("su_embeddings").embedding_preview(*args, **kwargs)


def get_data(*args, **kwargs):
    return module("su_download").get_data(*args, **kwargs)


def check_request(*args, **kwargs):
    return module("su_download").check_request(*args, **kwargs)


def download(*args, **kwargs):
    return module("su_download").download(*args, **kwargs)


def iter_download(*args, **kwargs):
    return module("su_download").iter_download(*args, **kwargs)


def save_data(*args, **kwargs):
    return module("su_download").save_data(*args, **kwargs)


def to_payload(*args, **kwargs):
    return module("su_download").to_payload(*args, **kwargs)


def from_payload(*args, **kwargs):
    return module("su_download").from_payload(*args, **kwargs)


def collection(*args, **kwargs):
    return module("su_collection").collection(*args, **kwargs)
