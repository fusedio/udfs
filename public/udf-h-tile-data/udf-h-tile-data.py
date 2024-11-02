import os
from dotenv import load_dotenv

import xarray as xr
import fused

from arraylake import Client, config

from datetime import datetime

def fetch_arraylake(
    bbox,
    repo_name,
    dataset,
    time,
    variable,
):
    import os
    from arraylake import Client, config
    from dotenv import load_dotenv
    
    env_file_path = "/mnt/cache/.env"
    load_dotenv(env_file_path, override=True)

    config.set({"chunkstore.use_delegated_credentials": True})
    al_token = os.environ['ARRAYLAKE_T']
    
    client = Client(token=al_token)

    # @fused.cache(reset=True)
    def fetch_repo(repo_name):
        return client.get_repo(repo_name)

    repo = fetch_repo(repo_name)

    # @fused.cache(reset=True)
    def fetch_dataset(dataset):
        return repo.to_xarray(dataset, decode_coords='all')

    ds = fetch_dataset(dataset)


    ds = ds.sel(time=time)

    # print(bbox.total_bounds)
    clipped = ds.rio.clip_box(*bbox.total_bounds)[variable].compute()
    # print(clipped)
    return clipped


@fused.udf
def udf(
    bbox: fused.types.TileGDF=None,
    repo_name="sylvera/test-fused",
    dataset="era5_land_monthly_averaged",
    variable="stl2",

    year: int = 2020,
) -> xr.DataArray:
    import utils
    time = f"{year}-01-01"

    
    # fetch_arraylake = utils.fetch_arraylake
    y_decreasing = utils.y_decreasing
    
    arr = fetch_arraylake(bbox, repo_name, dataset, time, variable)

    arr = y_decreasing(arr)
    # print(arr)
    return arr


























    