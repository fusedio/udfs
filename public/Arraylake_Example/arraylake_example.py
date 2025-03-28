"""
Example UDF for working with a large Zarr-based RGB datacube stored in Arraylake.
This datacube is a monthly composite at 30m resolution generated from Sentinel-2 data over South America.
(See https://earthmover.io/blog/serverless-datacube-pipeline for more details.)

This UDF opens a dataset from Arraylake, subsets it to a bounding box, and coarsens it to an appropriate resolution.

NOTE: using Arraylake requires an Earthmover account. This UDF will only work for Earthmover customers.
"""

from datetime import datetime

import xarray as xr
import fused

import arraylake

# remove this after Arraylake v0.13 is released an it becomes default
arraylake.config.set({"chunkstore.use_delegated_credentials": True})


@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    repo_name="earthmover-demos/sentinel-datacube-South-America-3",
    varname="rgb_median",
    time: datetime = datetime(2020, 10, 1),
    min_data_value=1000,
    scale_factor: int = 3_000,
) -> xr.DataArray:

    client = arraylake.Client(token=fused.secrets["ARRAYLAKE_TOKEN"])
    repo = client.get_repo(repo_name, read_only=True)

    ds = repo.to_xarray(mask_and_scale=False, chunks={})

    # try to infer the resolution of the dataset
    pixel_res = 110e3 * abs(ds.latitude.values[1] - ds.latitude.values[0])
    resolution = int(5 * 2 ** (15 - zoom))
    coarsen_factor = max(int(resolution // pixel_res), 1)
    print(f"Coarsening by {coarsen_factor}")

    min_lon, min_lat, max_lon, max_lat = bounds
    ds = ds.sel(time=time, method="nearest")
    ds = ds.sel(
        longitude=slice(min_lon, max_lon),
        latitude=slice(
            max_lat,
            min_lat,
        ),
    )
    data = ds[varname]
    data.load()
    data = data.where(data > 0)
    if coarsen_factor > 1:
        data = data.coarsen(
            longitude=coarsen_factor, latitude=coarsen_factor, boundary="pad"
        ).mean()
    # fused needs dimensions in this order
    data = data.transpose("band", "latitude", "longitude")
    data = data - min_data_value
    data = (256 / scale_factor) * data
    # avoid oversaturating pixels
    data = data.where(data.max("band") <= 256, 256)
    return data
