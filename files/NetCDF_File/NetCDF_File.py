@fused.udf
def udf(path: str, *, chip_len: int = 1024):
    import rioxarray
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils

    min_max = None
    # min_max = (0,255)

    try:
        # Try georeferenced
        xds = rioxarray.open_rasterio(path)
        var = list(xds.data_vars)[0]
        coarsen_factor = (xds[var].shape[-2] // chip_len) + 1
        da = (
            xds.coarsen(x=coarsen_factor, y=coarsen_factor, boundary="trim")
            .max()
            .rio.reproject(4326)[var]
        )
        arr = utils.arr_to_plasma(da.values.squeeze(), min_max=min_max)
        return arr, da.rio.bounds()
    except:
        # Try without georeferencing
        xds = rioxarray.open_rasterio(path)
        var = list(xds.data_vars)[0]
        coarsen_factor = (xds[var].shape[-2] // chip_len) + 1
        arr = (
            xds.coarsen(x=coarsen_factor, y=coarsen_factor, boundary="trim")
            .max()[var]
            .values
        )
        arr = utils.arr_to_plasma(arr.squeeze(), min_max=min_max)
        s = arr.shape[-1] / arr.shape[-2]
        return arr, [-s / 2, -1, s / 2, 1]
