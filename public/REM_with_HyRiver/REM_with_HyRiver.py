@fused.udf
def udf(bounds=None):
    import nest_asyncio
    import numpy as np
    import py3dep
    import pynhd
    import shapely.ops as ops
    import xarray as xr
    import xrspatial as xs
    from datashader import transfer_functions as tf
    from datashader.colors import Greys9, inferno
    from utils import idw

    nest_asyncio.apply()

    @fused.cache
    def fn(bounds, res=10):
        # Get the DEM and the river network
        dem = py3dep.get_dem(bounds, res)
        wd = pynhd.WaterData("nhdflowline_network")
        flw = wd.bybox(bounds)

        # Prepare the river network by removing isolated nodes and selecting the main stem
        flw = pynhd.prepare_nhdplus(flw, 0, 0, 0, remove_isolated=True)
        flw = flw[flw.levelpathi == flw.levelpathi.min()].copy()
        lines = ops.linemerge(flw.geometry.tolist())
        riv_dem = py3dep.elevation_profile(lines, 10, crs=flw.crs)
        elevation = idw(riv_dem, dem, 20)
        rem = dem - elevation
        return rem, dem

    bounds = (-119.59, 39.24, -119.47, 39.30) if bounds is None else bounds
    rem, dem = fn(bounds)
    illuminated = xs.hillshade(dem, angle_altitude=10, azimuth=90)
    tf.Image.border = 0
    img = tf.stack(
        tf.shade(dem, cmap=Greys9, how="linear"),
        tf.shade(illuminated, cmap=["black", "white"], how="linear", alpha=180),
        tf.shade(rem, cmap=inferno[::-1], span=[0, 7], how="log", alpha=200),
    )
    return img[::-1]
