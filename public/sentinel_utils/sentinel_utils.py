"""sentinel_utils: Sentinel-2 L2A, Copernicus DEM and AlphaEarth embeddings on Fused, in the style of sentinelhub-py.

Load it one time, then call the functions on it:
    su = fused.load("sentinel_utils")                     in the workbench canvas
    su = fused.load("<GitHub URL or local folder>")       outside the workbench (the package folder with meta.json)
    products = su.query([-118.62, 34.02, -118.50, 34.10], ("2024-06-01", "2024-06-30"), cloud_cover=30)
    ds = su.get_data(su.process_request([-118.62, 34.02, -118.50, 34.10], "2023-06-22", resolution=60))
    tables = su.download([su.statistical_request(field, ("2024-01-01", "2024-12-31"), index="NDVI") for field in fields])
    ndvi = su.collection().filter_bounds(area).filter_date("2024-06-01", "2024-08-31").median().index("NDVI")
One request on a Fused worker: su.from_payload(fused.run(su, request=json.dumps(request), engine="remote")).
su.api() lists every public function. su.module("su_search") gives one module (MASKS, NoDataError, ...).
Each function here calls the function of the same name in its module. Outside the canvas the modules are headers
(meta.json). In the canvas, module() fetches the code of the 3 nodes one time and imports them as headers too, because
fused.load by name loads every sibling again (su_analysis alone makes 60 loads, about 13 s).
"""

MODULES = ("su_search", "su_read", "su_analysis")
_LEGACY = {"su_constants": "su_search", "su_geometry": "su_search", "su_time": "su_search", "su_catalog": "su_search",
           "su_indices": "su_read", "su_reader": "su_read", "su_process": "su_read", "su_embeddings": "su_read",
           "su_statistical": "su_analysis", "su_areas": "su_analysis", "su_collection": "su_analysis",
           "su_download": "su_analysis"}
SENTINEL2_L2A = "sentinel-2-l2a"
DEM = "cop-dem-glo-30"
AEF = "alphaearth-embeddings"
_HELD = {}

try:
    import su_search
    import su_read
    import su_analysis
    _HELD.update({name: globals()[name] for name in MODULES})
except ImportError:
    pass


@fused.udf
def udf(request: str = ""):
    """Returns the table of public functions; with the JSON of a request, su_analysis.to_payload of its result."""
    import json

    if not request:
        return api()
    download = module("su_analysis")
    return download.to_payload(download.get_data(json.loads(request)))


def module(name):
    """Returns the module `name` (one of MODULES), for example module("su_search").MASKS. The names of the old
    twelve modules (su_constants, su_reader ...) return the module that holds them now."""
    name = _LEGACY.get(name, name)
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
    return module("su_search").get_available_collections(*args, **kwargs)


def collection_info(*args, **kwargs):
    return module("su_search").collection_info(*args, **kwargs)


def band_names(*args, **kwargs):
    return module("su_search").band_names(*args, **kwargs)


def band_info(*args, **kwargs):
    return module("su_search").band_info(*args, **kwargs)


def to_bbox(*args, **kwargs):
    return module("su_search").to_bbox(*args, **kwargs)


def to_geometry(*args, **kwargs):
    return module("su_search").to_geometry(*args, **kwargs)


def read_geojson(*args, **kwargs):
    return module("su_search").read_geojson(*args, **kwargs)


def geojson_to_wkt(*args, **kwargs):
    return module("su_search").geojson_to_wkt(*args, **kwargs)


def get_utm_crs(*args, **kwargs):
    return module("su_search").get_utm_crs(*args, **kwargs)


def transform_bbox(*args, **kwargs):
    return module("su_search").transform_bbox(*args, **kwargs)


def to_utm_bbox(*args, **kwargs):
    return module("su_search").to_utm_bbox(*args, **kwargs)


def bbox_to_dimensions(*args, **kwargs):
    return module("su_search").bbox_to_dimensions(*args, **kwargs)


def bbox_to_resolution(*args, **kwargs):
    return module("su_search").bbox_to_resolution(*args, **kwargs)


def area_km2(*args, **kwargs):
    return module("su_search").area_km2(*args, **kwargs)


def make_geobox(*args, **kwargs):
    return module("su_search").make_geobox(*args, **kwargs)


def parse_time(*args, **kwargs):
    return module("su_search").parse_time(*args, **kwargs)


def parse_time_interval(*args, **kwargs):
    return module("su_search").parse_time_interval(*args, **kwargs)


def serialize_time(*args, **kwargs):
    return module("su_search").serialize_time(*args, **kwargs)


def filter_times(*args, **kwargs):
    return module("su_search").filter_times(*args, **kwargs)


def split_time_interval(*args, **kwargs):
    return module("su_search").split_time_interval(*args, **kwargs)


def query(*args, **kwargs):
    return module("su_search").query(*args, **kwargs)


def count(*args, **kwargs):
    return module("su_search").count(*args, **kwargs)


def get_product(*args, **kwargs):
    return module("su_search").get_product(*args, **kwargs)


def get_timestamps(*args, **kwargs):
    return module("su_search").get_timestamps(*args, **kwargs)


def to_geojson(*args, **kwargs):
    return module("su_search").to_geojson(*args, **kwargs)


def to_dataframe(*args, **kwargs):
    return module("su_search").to_dataframe(*args, **kwargs)


def get_collections(*args, **kwargs):
    return module("su_search").get_collections(*args, **kwargs)


def rank(*args, **kwargs):
    return module("su_search").rank(*args, **kwargs)


def clearest_days(*args, **kwargs):
    return module("su_search").clearest_days(*args, **kwargs)


def read_stack(*args, **kwargs):
    return module("su_read").read_stack(*args, **kwargs)


def tile(*args, **kwargs):
    return module("su_read").tile(*args, **kwargs)


def export(*args, **kwargs):
    return module("su_read").export(*args, **kwargs)


def to_cog(*args, **kwargs):
    return module("su_read").to_cog(*args, **kwargs)


def render(*args, **kwargs):
    return module("su_read").render(*args, **kwargs)


def colorize(*args, **kwargs):
    return module("su_read").colorize(*args, **kwargs)


def process_request(*args, **kwargs):
    return module("su_read").process_request(*args, **kwargs)


def run_process(*args, **kwargs):
    return module("su_read").run_process(*args, **kwargs)


def statistical_request(*args, **kwargs):
    return module("su_analysis").statistical_request(*args, **kwargs)


def run_statistical(*args, **kwargs):
    return module("su_analysis").run_statistical(*args, **kwargs)


def bbox_splitter(*args, **kwargs):
    return module("su_analysis").bbox_splitter(*args, **kwargs)


def osm_splitter(*args, **kwargs):
    return module("su_analysis").osm_splitter(*args, **kwargs)


def utm_grid_splitter(*args, **kwargs):
    return module("su_analysis").utm_grid_splitter(*args, **kwargs)


def utm_zone_splitter(*args, **kwargs):
    return module("su_analysis").utm_zone_splitter(*args, **kwargs)


def tile_splitter(*args, **kwargs):
    return module("su_analysis").tile_splitter(*args, **kwargs)


def custom_grid_splitter(*args, **kwargs):
    return module("su_analysis").custom_grid_splitter(*args, **kwargs)


def normalized_difference(*args, **kwargs):
    return module("su_read").normalized_difference(*args, **kwargs)


def compute_index(*args, **kwargs):
    return module("su_read").compute_index(*args, **kwargs)


def required_bands(*args, **kwargs):
    return module("su_read").required_bands(*args, **kwargs)


def embedding_year(*args, **kwargs):
    return module("su_read").embedding_year(*args, **kwargs)


def load_embeddings(*args, **kwargs):
    return module("su_read").load_embeddings(*args, **kwargs)


def embedding_at(*args, **kwargs):
    return module("su_read").embedding_at(*args, **kwargs)


def embedding_of(*args, **kwargs):
    return module("su_read").embedding_of(*args, **kwargs)


def embedding_similarity(*args, **kwargs):
    return module("su_read").embedding_similarity(*args, **kwargs)


def embedding_difference(*args, **kwargs):
    return module("su_read").embedding_difference(*args, **kwargs)


def embedding_preview(*args, **kwargs):
    return module("su_read").embedding_preview(*args, **kwargs)


def get_data(*args, **kwargs):
    return module("su_analysis").get_data(*args, **kwargs)


def check_request(*args, **kwargs):
    return module("su_analysis").check_request(*args, **kwargs)


def download(*args, **kwargs):
    return module("su_analysis").download(*args, **kwargs)


def iter_download(*args, **kwargs):
    return module("su_analysis").iter_download(*args, **kwargs)


def save_data(*args, **kwargs):
    return module("su_analysis").save_data(*args, **kwargs)


def to_payload(*args, **kwargs):
    return module("su_analysis").to_payload(*args, **kwargs)


def from_payload(*args, **kwargs):
    return module("su_analysis").from_payload(*args, **kwargs)


def collection(*args, **kwargs):
    return module("su_analysis").collection(*args, **kwargs)
