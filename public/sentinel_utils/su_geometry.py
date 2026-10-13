try:
    import su_constants as constants
except ImportError:
    constants = fused.load("su_constants")

_AREA_FORMS = "'w,s,e,n', (w, s, e, n), WKT, a shapely geometry, a GeoDataFrame/GeoSeries or a GeoJSON dict"


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def to_bbox(area):
    """Returns (w, s, e, n) in EPSG:4326 degrees."""
    numbers = _numbers(area)
    if numbers is None:
        numbers = to_geometry(area).bounds
    return _check_bbox(numbers)


def to_geometry(area):
    """Returns a shapely geometry in EPSG:4326."""
    import shapely
    import shapely.geometry

    numbers = _numbers(area)
    if numbers is not None:
        return shapely.box(*_check_bbox(numbers))
    if hasattr(area, "to_crs"):
        geom = (area.to_crs(4326) if area.crs else area).union_all()
    elif hasattr(area, "geom_type"):
        geom = area
    elif isinstance(area, dict):
        geom = shapely.union_all([shapely.geometry.shape(g) for g in _geojson_geometries(area)])
    elif isinstance(area, str):
        try:
            geom = shapely.from_wkt(area)
        except shapely.errors.GEOSException:
            raise ValueError(f"area must be {_AREA_FORMS}, got {area!r}") from None
    else:
        raise ValueError(f"area must be {_AREA_FORMS}, got {type(area).__name__}")
    if geom.is_empty:
        raise ValueError("area is an empty geometry")
    _check_bbox(geom.bounds)
    return geom


def read_geojson(path_or_url):
    """Returns the GeoJSON dict read from a local file or an http(s) URL."""
    import json

    if str(path_or_url).startswith(("http://", "https://")):
        import requests

        response = requests.get(path_or_url, timeout=60)
        response.raise_for_status()
        return response.json()
    with open(path_or_url) as f:
        return json.load(f)


def geojson_to_wkt(geojson):
    """Returns the WKT string of the union of the GeoJSON geometries."""
    import shapely

    return shapely.to_wkt(to_geometry(geojson), rounding_precision=7)


def get_utm_crs(lon, lat):
    """Returns the UTM CRS ("EPSG:326zz" north, "EPSG:327zz" south) of a point."""
    zone = int((float(lon) + 180) // 6) % 60 + 1
    return f"EPSG:{(32600 if lat >= 0 else 32700) + zone}"


def transform_bbox(bbox, src_crs, dst_crs):
    """Returns the (xmin, ymin, xmax, ymax) envelope of `bbox` reprojected from src_crs to dst_crs."""
    from pyproj import Transformer

    if str(src_crs) == str(dst_crs):
        return tuple(float(v) for v in bbox)
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    return tuple(float(v) for v in transformer.transform_bounds(*bbox, densify_pts=21))


def to_utm_bbox(area):
    """Returns ((xmin, ymin, xmax, ymax) in metres, crs) in the UTM zone of the area centre."""
    w, s, e, n = to_bbox(area)
    crs = get_utm_crs((w + e) / 2, (s + n) / 2)
    return transform_bbox((w, s, e, n), "EPSG:4326", crs), crs


def bbox_to_dimensions(area, resolution):
    """Returns (width, height) in pixels of the area at `resolution` metres, in the UTM zone of its centre."""
    (x0, y0, x1, y1), _ = to_utm_bbox(area)
    rx, ry = _pair(resolution, "resolution")
    return round((x1 - x0) / rx), round((y1 - y0) / ry)


def bbox_to_resolution(area, width, height):
    """Returns (res_x, res_y) in metres of the area drawn at width x height pixels."""
    (x0, y0, x1, y1), _ = to_utm_bbox(area)
    return (x1 - x0) / int(width), (y1 - y0) / int(height)


def area_km2(area):
    """Returns the geodesic area in square kilometres."""
    from pyproj import Geod

    return abs(Geod(ellps="WGS84").geometry_area_perimeter(to_geometry(area))[0]) / 1e6


def make_geobox(area, *, resolution=None, size=None, crs=None):
    """Returns a north-up odc.geo GeoBox over the area (size keeps the exact bounds); crs defaults to UTM of the centre."""
    import math
    from odc.geo import CRS
    from odc.geo.geobox import GeoBox

    if resolution is not None and size is not None:
        raise ValueError("give resolution or size, not both")
    w, s, e, n = to_bbox(area)
    crs = CRS(get_utm_crs((w + e) / 2, (s + n) / 2) if crs in (None, "utm") else crs)
    bounds = transform_bbox((w, s, e, n), "EPSG:4326", str(crs))
    if size is not None:
        width, height = (int(v) for v in _pair(size, "size"))
        return GeoBox.from_bbox(bounds, crs=crs, shape=(height, width), tight=True)
    if crs.geographic or crs.units[0] != "metre":
        raise ValueError(f"resolution is in metres but {crs} uses {crs.units[0]}; give size or a metric crs")
    step = float(resolution or 10)
    if crs.epsg == 3857:
        step /= math.cos(math.radians((s + n) / 2))
    return GeoBox.from_bbox(bounds, crs=crs, resolution=step)


def _numbers(area):
    if isinstance(area, str):
        if "(" in area:
            return None
        area = area.replace("[", "").replace("]", "").split(",")
    elif not isinstance(area, (list, tuple)) and type(area).__name__ != "ndarray":
        return None
    try:
        values = [float(v) for v in area]
    except (TypeError, ValueError):
        values = []
    if len(values) != 4:
        raise ValueError(f"area must be {_AREA_FORMS}, got {area!r}")
    return values


def _check_bbox(values):
    w, s, e, n = (float(v) for v in values)
    if not (-180 <= w <= e <= 180 and -90 <= s <= n <= 90):
        raise ValueError(f"area must be in EPSG:4326 degrees with west <= east and south <= north "
                         f"(split an area that crosses 180), got {[w, s, e, n]}")
    return w, s, e, n


def _geojson_geometries(obj):
    kind = obj.get("type")
    if kind == "FeatureCollection":
        return [g for f in obj["features"] for g in _geojson_geometries(f)]
    if kind == "Feature":
        return [obj["geometry"]] if obj.get("geometry") else []
    if kind in ("Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection"):
        return [obj]
    raise ValueError(f"GeoJSON type must be FeatureCollection, Feature or a geometry type, got {kind!r}")


def _pair(value, name):
    import numbers

    if isinstance(value, numbers.Real) and value > 0:
        return float(value), float(value)
    if isinstance(value, (list, tuple)) and len(value) == 2 and all(v > 0 for v in value):
        return float(value[0]), float(value[1])
    raise ValueError(f"{name} must be a positive number or a (x, y) pair of positive numbers, got {value!r}")
