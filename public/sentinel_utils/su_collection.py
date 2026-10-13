"""Earth Engine style collections (earthengine-api ImageCollection, in snake_case): lazy and chainable.

    s2 = collection().filter_bounds(area).filter_date("2024-06-01", "2024-08-31")
    ndvi = s2.filter_metadata("cloud_cover", "less_than", 20).median().normalized_difference(["B08", "B04"])
    ds = ndvi.get_data()
The last line gives the same Dataset as get_data(process_request(area, ("2024-06-01", "2024-08-31"), cloud_cover=20,
reducer="median", index="NDVI")). Each method returns a new object and reads no data. A chain compiles to the JSON
request of su_process (`to_request()`). Only get_info, size, get_data, save_data, reduce_region and reduce_regions read
data. Lists of images and of regions run through su_download.download.
filter_date includes the end day (as su_time), not the exclusive end of Earth Engine. cloud_cover limits include the
limit value, so "less_than" and "not_greater_than" give the same limit.
Rule: reduce_region / reduce_regions give the statistics of the composite image itself (its pixels inside the region),
as Earth Engine does; the clearest-day series of the Statistical API is su_statistical.statistical_request.
"""
import dataclasses

try:
    import su_constants as constants
    import su_geometry as geometry
    import su_time as time
    import su_catalog as catalog
    import su_indices as indices
    import su_process as process
    import su_statistical as statistical
    import su_download as download
except ImportError:
    constants = fused.load("su_constants")
    geometry = fused.load("su_geometry")
    time = fused.load("su_time")
    catalog = fused.load("su_catalog")
    indices = fused.load("su_indices")
    process = fused.load("su_process")
    statistical = fused.load("su_statistical")
    download = fused.load("su_download")

METADATA = ("cloud_cover",)
OPERATORS = {"equals": ("min", "max"), "less_than": ("max",), "not_greater_than": ("max",),
             "greater_than": ("min",), "not_less_than": ("min",)}
REGION_REDUCERS = ("mean", "median", "min", "max", "std", "count")


@fused.udf
def udf():
    return api()


def api():
    """Returns a DataFrame of the public functions: name, signature, doc."""
    return constants.api_table(globals())


def collection(name=constants.SENTINEL2_L2A):
    """Returns a lazy image collection of `name` (one of DATA_COLLECTIONS); chain filters, then a composite."""
    return _Collection(constants.check_choice("collection", name, constants.DATA_COLLECTIONS))


@dataclasses.dataclass(frozen=True, eq=False)
class _Collection:
    name: str
    area: object = None
    date: object = None
    cloud: tuple = (0.0, 100.0)
    bands: tuple = None

    def filter_bounds(self, area):
        return dataclasses.replace(self, area=area)

    def filter_date(self, start, end=None):
        return dataclasses.replace(self, date=start if end is None else (start, end))

    def filter_metadata(self, name, operator, value):
        constants.check_choice("name", name, METADATA)
        limits = OPERATORS[constants.check_choice("operator", operator, OPERATORS)]
        low, high = self.cloud
        low = max(low, float(value)) if "min" in limits else low
        high = min(high, float(value)) if "max" in limits else high
        return dataclasses.replace(self, cloud=(low, high))

    def select(self, bands):
        return dataclasses.replace(self, bands=tuple(constants.band_names(bands, self.name)))

    def get_info(self):
        return catalog.query(self._area(), self.date, collection=self.name, cloud_cover=self._cloud())

    def size(self):
        return len(self.get_info())

    def first(self):
        return self.mosaic("oldest")

    def mosaic(self, order="least_cloudy"):
        return _Image(self, mosaic_order=constants.check_choice("order", order, constants.MOSAIC_ORDERS))

    def median(self):
        return _Image(self, reducer="median")

    def mean(self):
        return _Image(self, reducer="mean")

    def min(self):
        return _Image(self, reducer="min")

    def max(self):
        return _Image(self, reducer="max")

    def count(self):
        return _Image(self, reducer="count")

    def aggregate(self, interval="P1M", reducer="median"):
        constants.check_choice("reducer", reducer, process.REDUCERS)
        start, end = time.parse_time_interval(self.date)
        return _ImageList(_Image(self.filter_date(f"{a:%Y-%m-%d}", f"{b:%Y-%m-%d}"), reducer=reducer)
                          for a, b in statistical._intervals(start, end, interval))

    def _area(self):
        if self.area is None:
            raise ValueError("the request needs an area: call filter_bounds(area) or clip(area) first")
        return self.area

    def _cloud(self):
        low, high = self.cloud
        return high if low == 0 else (low, high)


@dataclasses.dataclass(frozen=True, eq=False)
class _Image:
    source: _Collection
    mosaic_order: str = "least_cloudy"
    reducer: str = None
    index_name: str = None
    mask: str = None
    resolution: float = None

    def select(self, bands):
        return dataclasses.replace(self, source=self.source.select(bands))

    def normalized_difference(self, bands):
        pair = tuple(constants.band_names(bands, self.source.name))
        names = [n for n, b in indices.INDICES.items() if tuple(b) == pair]
        if not names:
            pairs = {n: b for n, b in indices.INDICES.items() if len(b) == 2 and all(isinstance(x, str) for x in b)}
            raise ValueError(f"bands must be the pair of one index in {pairs}, got {list(pair)}")
        return self.index(names[0])

    def index(self, name):
        name = constants.check_choice("index", str(name).upper(), indices.INDICES)
        return dataclasses.replace(self, index_name=name)

    def update_mask(self, mask_name):
        return dataclasses.replace(self, mask=constants.check_choice("mask", mask_name, constants.MASKS))

    def clip(self, area):
        return dataclasses.replace(self, source=self.source.filter_bounds(area))

    def resample(self, resolution):
        return dataclasses.replace(self, resolution=resolution)

    def to_request(self, output="dataset"):
        src = self.source
        return process.process_request(src._area(), src.date, src.bands, collection=src.name,
                                       resolution=self.resolution, mosaic_order=self.mosaic_order,
                                       cloud_cover=src._cloud(), mask=self.mask, index=self.index_name,
                                       reducer=self.reducer, output=output)

    def get_data(self, output="dataset"):
        return download.get_data(self.to_request(output))

    def save_data(self, path=None):
        return download.save_data(self.to_request(), path)

    def reduce_region(self, reducer="mean", area=None, resolution=None):
        request = self._region_request(reducer, area, resolution)
        return _region_table(download.get_data(request), request, self._outputs(request), reducer)

    def reduce_regions(self, features, reducer="mean", resolution=None, max_workers=16, engine="remote"):
        import pandas as pd

        shapes, ids = ((list(features.geometry), list(features.index)) if hasattr(features, "geometry")
                       else (list(features), list(range(len(features)))))
        requests = [self._region_request(reducer, shape, resolution) for shape in shapes]
        images = download.download(requests, max_workers=max_workers, engine=engine)
        tables = [_region_table(ds, r, self._outputs(r), reducer).assign(feature=i)
                  for i, ds, r in zip(ids, images, requests)]
        return pd.concat(tables, ignore_index=True)[["feature", "interval_from", "interval_to", "output", reducer]]

    def _region_request(self, reducer, area, resolution):
        constants.check_choice("reducer", reducer, REGION_REDUCERS)
        image = self if area is None else self.clip(area)
        return image.resample(resolution or self.resolution).to_request()

    def _outputs(self, request):
        bands = [b for b in request["bands"] if b != "dataMask"]
        if not self.index_name:
            return bands
        return (bands if self.source.bands else []) + [self.index_name]


class _ImageList(tuple):
    def map(self, function):
        return _ImageList(function(image) for image in self)

    def to_request(self, output="dataset"):
        return [image.to_request(output) for image in self]

    def get_data(self, output="dataset", max_workers=16, engine="remote", ignore_errors=False):
        return download.download(self.to_request(output), max_workers=max_workers, engine=engine,
                                 ignore_errors=ignore_errors)

    def reduce_region(self, reducer="mean", area=None, resolution=None, max_workers=16, engine="remote"):
        import pandas as pd

        requests = [image._region_request(reducer, area, resolution) for image in self]
        images = download.download(requests, max_workers=max_workers, engine=engine)
        tables = [_region_table(ds, r, image._outputs(r), reducer) for ds, r, image in zip(images, requests, self)]
        return pd.concat(tables, ignore_index=True)


def _region_table(ds, request, names, reducer):
    import numpy as np

    inside = statistical._inside(geometry.to_geometry(request["area"]), process._geobox(request))
    valid = ds["dataMask"].values > 0
    values = np.stack([np.where(valid, ds[n].values, np.nan)[inside] for n in names]).astype("float64")[:, None]
    date = tuple(request["date"]) if isinstance(request["date"], list) else request["date"]
    first, last = (t.tz_localize(None).normalize() for t in time.parse_time_interval(date))
    table = statistical._stats_table(values, [first], [(first, last)], names, [50] if reducer == "median" else None)
    column = (table["sample_count"] - table["no_data_count"] if reducer == "count"
              else table["p50" if reducer == "median" else reducer])
    return table[["interval_from", "interval_to", "output"]].assign(**{reducer: column})
