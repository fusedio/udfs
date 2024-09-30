import os
from dotenv import load_dotenv

import xarray as xr
import fused

import arraylake

from datetime import datetime


# Example UDF for working with a large Zarr-based RGB datacube stored in Arraylake
# This UDF opens a dataset from Arraylake, subsets it to a bounding box, and coarsens it to an appropriate resolution.

# NOTE: using Arraylake requires an Earthmover account. This UDF will only work for Earthmover customers.

@fused.udf
def udf(
    bbox: fused.types.Bbox = None,
    repo_name="earthmover-demos/sentinel-datacube-South-America-3",
    varname="rgb_median",
    time: datetime = datetime(2020, 10, 1),
    scale_factor: int = 256 / 4000,
) -> xr.DataArray:

    # Load secrets from environment variables following Fused recommended approach
    env_file_path = "/mnt/cache/.env"
    load_dotenv(env_file_path, override=True)

    arraylake.config.set(
        {
            "s3.aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        }
    )

    client = arraylake.Client(token=os.environ["ARRAYLAKE_TOKEN"])
    repo = client.get_repo(repo_name)

    # open full dataset with Xarray lazily
    ds = repo.to_xarray(mask_and_scale=False, chunks={})

    # infer the resolution of the dataset
    # NOTE: may have to change lat / lon variable names depending on the dataset
    pixel_res = 110e3 * abs(ds.latitude.values[1] - ds.latitude.values[0])
    resolution = int(5 * 2 ** (15 - bbox.z[0]))
    coarsen_factor = max(int(resolution // pixel_res), 1)
    print(f"Coarsening by {coarsen_factor}")

    # subset to bounding box
    min_lon, min_lat, max_lon, max_lat = bbox.bounds.values[0]
    ds = ds.sel(time=time, method="nearest")
    ds = ds.sel(
        longitude=slice(min_lon, max_lon),
        latitude=slice(
            max_lat,
            min_lat,
        ),
    )
    data = ds[varname]
    
    # this actually loads the data
    data.load()

    # apply a mask
    data = data.where(data > 0)
    if coarsen_factor > 1:
        data = data.coarsen(
            longitude=coarsen_factor, latitude=coarsen_factor, boundary="pad"
        ).mean()

    # format output as expected by Fused
    data = data.transpose("band", ...)
    # rescale scale data for visualization
    data = data * scale_factor
    return data
