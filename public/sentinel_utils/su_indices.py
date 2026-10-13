try:
    import su_constants as constants
except ImportError:
    constants = fused.load("su_constants")

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


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


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
