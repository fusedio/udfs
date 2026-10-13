try:
    import su_constants as constants
    import su_geometry as geometry
    import su_catalog as catalog
except ImportError:
    constants = fused.load("su_constants")
    geometry = fused.load("su_geometry")
    catalog = fused.load("su_catalog")

_MGRS_ROWS = "CDEFGHJKLMNPQRSTUVWX"
_MAX_CELLS = 1_000_000
_MAX_LAT_3857 = 85.0511


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def bbox_splitter(area, split_shape=(5, 4), *, reduce_bbox_sizes=False):
    """Returns the area bbox cut into split_shape (columns, rows) equal EPSG:4326 cells that meet the area."""
    import shapely

    geom = geometry.to_geometry(area)
    cols, rows = _count_pair(split_shape, "split_shape")
    w, s, e, n = geom.bounds
    out = _grid((w, s), ((e - w) / cols, (n - s) / rows), (cols, rows), "EPSG:4326", geom)
    if reduce_bbox_sizes:
        bounds = out.geometry.intersection(geom).bounds.to_numpy()
        out["bbox"] = [tuple(float(v) for v in b) for b in bounds]
        out = out.set_geometry(shapely.box(*bounds.T), crs="EPSG:4326")
    return out


def osm_splitter(area, zoom_level):
    """Returns the XYZ web map tiles at zoom_level that meet the area; bbox in EPSG:3857."""
    import geopandas as gpd
    import mercantile
    import shapely

    geom = geometry.to_geometry(area)
    w, s, e, n = geom.bounds
    if max(abs(s), abs(n)) > _MAX_LAT_3857:
        raise ValueError(f"osm_splitter needs latitudes between -{_MAX_LAT_3857} and {_MAX_LAT_3857}, got {s}, {n}")
    if not 0 <= int(zoom_level) <= 24:
        raise ValueError(f"zoom_level must be an integer from 0 to 24, got {zoom_level!r}")
    tiles = list(mercantile.tiles(w, s, e, n, zooms=int(zoom_level)))
    out = gpd.GeoDataFrame(
        {"index_x": [t.x for t in tiles], "index_y": [t.y for t in tiles], "crs": "EPSG:3857",
         "bbox": [tuple(mercantile.xy_bounds(t)) for t in tiles], "zoom": [t.z for t in tiles],
         "x": [t.x for t in tiles], "y": [t.y for t in tiles]},
        geometry=[shapely.box(*mercantile.bounds(t)) for t in tiles], crs="EPSG:4326")
    return out[_meets(out.geometry, geom)].reset_index(drop=True)


def utm_grid_splitter(area, bbox_size):
    """Returns cells of bbox_size metres aligned to the UTM grid, made per MGRS grid zone (6 x 8 degrees)."""
    w, s, e, n = geometry.to_bbox(area)
    zones = []
    for zone in _zones(w, e):
        for k, row in enumerate(_MGRS_ROWS):
            lat0, lat1 = -80 + 8 * k, (84 if row == "X" else -72 + 8 * k)
            if lat1 > s and lat0 < n:
                zones.append((zone, row, row >= "N", (-186 + 6 * zone, lat0, -180 + 6 * zone, lat1)))
    return _utm_split(area, zones, bbox_size, None)


def utm_zone_splitter(area, bbox_size, *, offset=None):
    """Returns cells of bbox_size metres aligned to multiples of bbox_size (+ offset) in each UTM zone."""
    w, s, e, n = geometry.to_bbox(area)
    zones = [(zone, "", north, (-186 + 6 * zone, 0 if north else -80, -180 + 6 * zone, 84 if north else 0))
             for zone in _zones(w, e) for north in (True, False)]
    return _utm_split(area, zones, bbox_size, offset)


def tile_splitter(area, date, *, tile_split_shape=1):
    """Returns the Sentinel-2 MGRS tiles over the area found by su_catalog.query, each cut into tile_split_shape."""
    import shapely

    geom = geometry.to_geometry(area)
    products = catalog.query(area, date)
    if not len(products):
        raise constants.NoDataError(f"no Sentinel-2 scene over the area for date {date!r}")
    cols, rows = _count_pair(tile_split_shape, "tile_split_shape")
    parts = []
    for tile_id, group in products.groupby("tile_id", sort=True):
        crs = str(group["crs"].iloc[0])
        x0, y0, x1, y1 = _project(shapely.union_all(group.geometry.values), crs).bounds
        cells = _grid((x0, y0), ((x1 - x0) / cols, (y1 - y0) / rows), (cols, rows), crs, _project(geom, crs))
        parts.append(cells.assign(tile_id=tile_id))
    return _concat(parts, ["tile_id"])


def custom_grid_splitter(area, grid):
    """Returns the cells of grid (GeoDataFrame, or list of EPSG:4326 bboxes) that meet the area; index_x = grid position."""
    import geopandas as gpd
    import shapely

    geom = geometry.to_geometry(area)
    if hasattr(grid, "to_crs"):
        cells = gpd.GeoSeries(grid.geometry.values, crs=grid.crs or "EPSG:4326")
    else:
        cells = gpd.GeoSeries([shapely.box(*geometry.to_bbox(b)) for b in grid], crs="EPSG:4326")
    crs = cells.crs.to_string()
    out = gpd.GeoDataFrame(
        {"index_x": range(len(cells)), "index_y": 0, "crs": crs,
         "bbox": [tuple(float(v) for v in b) for b in cells.bounds.to_numpy()]},
        geometry=_to_wgs84(cells).values, crs="EPSG:4326")
    return out[_meets(out.geometry, geom)].reset_index(drop=True)


def _utm_split(area, zones, bbox_size, offset):
    import shapely

    geom = geometry.to_geometry(area)
    sx, sy = _size_pair(bbox_size, "bbox_size")
    ox, oy = (0.0, 0.0) if offset is None else _size_pair(offset, "offset", allow_zero=True)
    parts = []
    for zone, row, north, box in zones:
        part = shapely.box(*box).intersection(geom)
        if part.is_empty or (geom.area > 0 and part.area == 0):
            continue
        crs = f"EPSG:{(32600 if north else 32700) + zone}"
        clip = _project(part, crs)
        x0, y0, x1, y1 = clip.bounds
        ax, ay = (x0 - ox) // sx * sx + ox, (y0 - oy) // sy * sy + oy
        counts = (max(1, int(-((ax - x1) // sx))), max(1, int(-((ay - y1) // sy))))
        if counts[0] * counts[1] > _MAX_CELLS:
            raise ValueError(f"bbox_size {bbox_size!r} gives over {_MAX_CELLS} cells; use a larger bbox_size")
        cells = _grid((ax, ay), (sx, sy), counts, crs, clip).assign(utm_zone=f"{zone:02d}")
        parts.append(cells.assign(utm_row=row) if row else cells)
    return _concat(parts, ["utm_zone", "utm_row"] if any(z[1] for z in zones) else ["utm_zone"])


def _grid(origin, size, counts, crs, clip):
    import geopandas as gpd
    import numpy as np
    import shapely

    (x0, y0), (sx, sy), (nx, ny) = origin, size, counts
    i, j = (a.ravel() for a in np.meshgrid(np.arange(nx), np.arange(ny), indexing="ij"))
    xmin, ymin = x0 + i * sx, y0 + j * sy
    cells = gpd.GeoSeries(shapely.box(xmin, ymin, xmin + sx, ymin + sy), crs=crs)
    keep = _meets(cells, clip)
    cells = cells[keep]
    return gpd.GeoDataFrame(
        {"index_x": i[keep], "index_y": j[keep], "crs": crs,
         "bbox": [tuple(float(v) for v in b) for b in cells.bounds.to_numpy()]},
        geometry=_to_wgs84(cells).values, crs="EPSG:4326")


def _meets(cells, geom):
    import shapely

    boxes = cells.to_numpy()
    hits = shapely.intersects(boxes, geom)
    if geom.area > 0 and hits.any():
        hits[hits] = shapely.area(shapely.intersection(boxes[hits], geom)) > 1e-9 * shapely.area(boxes[hits])
    return hits


def _to_wgs84(cells):
    if cells.crs.to_epsg() == 4326:
        return cells
    width = (cells.bounds["maxx"] - cells.bounds["minx"]).min() if len(cells) else 1.0
    return cells.segmentize(max(width / 8, 1.0)).to_crs("EPSG:4326")


def _project(geom, crs):
    import numpy as np
    import shapely
    from pyproj import Transformer

    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    return shapely.transform(shapely.segmentize(geom, 0.01),
                             lambda xy: np.column_stack(transformer.transform(xy[:, 0], xy[:, 1])))


def _zones(w, e):
    return range(min(int((w + 180) // 6) + 1, 60), min(int((e + 180) // 6) + 1, 60) + 1)


def _concat(parts, extra):
    import geopandas as gpd
    import pandas as pd

    columns = ["index_x", "index_y", "crs", "bbox", *extra, "geometry"]
    if not parts:
        return gpd.GeoDataFrame({c: [] for c in columns}, geometry="geometry", crs="EPSG:4326")
    return gpd.GeoDataFrame(pd.concat(parts, ignore_index=True)[columns], geometry="geometry", crs="EPSG:4326")


def _count_pair(value, name):
    pair = (value, value) if isinstance(value, int) else tuple(value) if isinstance(value, (list, tuple)) else ()
    if len(pair) != 2 or not all(isinstance(v, int) and v >= 1 for v in pair):
        raise ValueError(f"{name} must be a positive integer or a (columns, rows) pair of positive integers, got {value!r}")
    return pair


def _size_pair(value, name, allow_zero=False):
    import numbers

    pair = (value, value) if isinstance(value, numbers.Real) else tuple(value) if isinstance(value, (list, tuple)) else ()
    low = 0 if allow_zero else 1e-9
    if len(pair) != 2 or not all(isinstance(v, numbers.Real) and v >= low for v in pair):
        raise ValueError(f"{name} must be a positive number of metres or a (x, y) pair of them, got {value!r}")
    return float(pair[0]), float(pair[1])
