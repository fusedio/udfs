SENTINEL2_L2A = "sentinel-2-l2a"
DEM = "cop-dem-glo-30"
AEF = "alphaearth-embeddings"

SCALE = 0.0001
MAX_PIXELS = 200_000_000

MOSAICKING = ("simple", "orbit")
MOSAIC_ORDERS = ("least_cloudy", "clearest_day", "newest", "oldest")
UNITS = ("reflectance", "dn")
RESAMPLING = ("nearest", "bilinear", "cubic")
OUTPUTS = ("dataset", "array", "geotiff", "png")

S2_BANDS = (
    {"name": "B01", "asset": "coastal", "alias": "coastal", "resolution": 60, "wavelength": 443},
    {"name": "B02", "asset": "blue", "alias": "blue", "resolution": 10, "wavelength": 490},
    {"name": "B03", "asset": "green", "alias": "green", "resolution": 10, "wavelength": 560},
    {"name": "B04", "asset": "red", "alias": "red", "resolution": 10, "wavelength": 665},
    {"name": "B05", "asset": "rededge1", "alias": "rededge1", "resolution": 20, "wavelength": 705},
    {"name": "B06", "asset": "rededge2", "alias": "rededge2", "resolution": 20, "wavelength": 740},
    {"name": "B07", "asset": "rededge3", "alias": "rededge3", "resolution": 20, "wavelength": 783},
    {"name": "B08", "asset": "nir", "alias": "nir", "resolution": 10, "wavelength": 842},
    {"name": "B8A", "asset": "nir08", "alias": "nir08", "resolution": 20, "wavelength": 865},
    {"name": "B09", "asset": "nir09", "alias": "nir09", "resolution": 60, "wavelength": 945},
    {"name": "B11", "asset": "swir16", "alias": "swir1", "resolution": 20, "wavelength": 1610},
    {"name": "B12", "asset": "swir22", "alias": "swir2", "resolution": 20, "wavelength": 2190},
    {"name": "SCL", "asset": "scl", "alias": "scl", "resolution": 20, "wavelength": None},
    {"name": "CLD", "asset": "cloud", "alias": "cloud", "resolution": 20, "wavelength": None},
    {"name": "SNW", "asset": "snow", "alias": "snow", "resolution": 20, "wavelength": None},
    {"name": "AOT", "asset": "aot", "alias": "aot", "resolution": 10, "wavelength": None},
    {"name": "WVP", "asset": "wvp", "alias": "wvp", "resolution": 10, "wavelength": None},
    {"name": "dataMask", "asset": None, "alias": "datamask", "resolution": None, "wavelength": None},
)

SCL_CLASSES = {
    0: "no_data", 1: "saturated", 2: "dark_area", 3: "cloud_shadow", 4: "vegetation", 5: "not_vegetated",
    6: "water", 7: "unclassified", 8: "cloud_medium", 9: "cloud_high", 10: "thin_cirrus", 11: "snow",
}

MASKS = {
    "clear": (4, 5, 6),
    "clear_snow": (4, 5, 6, 11),
    "land": (4, 5),
    "water": (6,),
    "snow": (11,),
    "cloud": (8, 9, 10),
    "shadow": (3,),
    "invalid": (0, 1),
    "no_cloud": (2, 3, 4, 5, 6, 7, 11),
}

DATA_COLLECTIONS = {
    SENTINEL2_L2A: {
        "title": "Sentinel-2 L2A surface reflectance",
        "resolution": 10,
        "bands": tuple(b["name"] for b in S2_BANDS),
        "temporal": ("2015-06-27", None),
        "provider": "ESA Copernicus, via source.coop and Element84 Earth Search",
        "url": "https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a",
    },
    DEM: {
        "title": "Copernicus DEM GLO-30",
        "resolution": 30,
        "bands": ("DEM",),
        "temporal": None,
        "provider": "ESA Copernicus, via Element84 Earth Search",
        "url": "https://earth-search.aws.element84.com/v1/collections/cop-dem-glo-30",
    },
    AEF: {
        "title": "AlphaEarth Foundations annual embeddings",
        "resolution": 10,
        "bands": tuple(f"A{i:02d}" for i in range(64)),
        "temporal": ("2017-01-01", "2025-12-31"),
        "provider": "Google DeepMind, via source.coop",
        "url": "https://data.source.coop/tge-labs/aef",
    },
}

_BAND_KEYS = {
    "datamask": "dataMask",
    **{key: b["name"] for b in S2_BANDS if b["asset"]
       for key in (b["name"].lower(), b["asset"], b["alias"], "b" + b["name"][1:].lstrip("0").lower())},
}


class NoDataError(ValueError):
    """No scene, or no valid pixel, for the request."""


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return api_table(globals())


def api_table(namespace):
    """Returns a DataFrame (name, signature, doc) of the public functions in a module namespace, without udf and api."""
    import inspect
    import pandas as pd

    rows = [{"name": n, "signature": f"{n}{inspect.signature(f)}", "doc": (inspect.getdoc(f) or "").split("\n")[0]}
            for n, f in namespace.items() if inspect.isfunction(f) and not n.startswith("_") and n not in ("udf", "api")]
    return pd.DataFrame(rows, columns=["name", "signature", "doc"])


def check_choice(name, value, allowed):
    """Returns `value` when it is in `allowed`, else raises ValueError that lists the allowed values."""
    if value not in allowed:
        raise ValueError(f"{name} must be one of {list(allowed)}, got {value!r}")
    return value


def get_available_collections():
    """Returns the names of the data collections."""
    return list(DATA_COLLECTIONS)


def collection_info(collection):
    """Returns the description dict of a data collection."""
    return {"name": collection, **DATA_COLLECTIONS[check_choice("collection", collection, DATA_COLLECTIONS)]}


def band_names(bands, collection=SENTINEL2_L2A):
    """Returns canonical band names (B04, B8A, SCL ...) for a list or comma string of band spellings."""
    check_choice("collection", collection, DATA_COLLECTIONS)
    if isinstance(bands, str):
        bands = [b for b in bands.split(",") if b.strip()]
    allowed = DATA_COLLECTIONS[collection]["bands"]
    out = []
    for band in bands:
        key = str(band).strip()
        if collection == SENTINEL2_L2A:
            key = _BAND_KEYS.get(key.lower(), key)
        else:
            key = next((b for b in allowed if b.lower() == key.lower()), key)
        check_choice("band", key, allowed)
        if key not in out:
            out.append(key)
    return out


def band_info(band):
    """Returns the dict of one Sentinel-2 band: name, asset, alias, resolution (m), wavelength (nm)."""
    name = band_names([band])[0]
    return dict(next(b for b in S2_BANDS if b["name"] == name))
