import rioxarray

temperature_palette = [
    "#313695",  # Very Cold
    "#4575b4",  # Cold
    "#74add1",  # Cool
    "#abd9e9",  # Mild
    "#fee090",  # Warm
    "#fdae61",  # Hot
    "#f46d43",  # Very Hot
    "#d73027"   # Extreme Heat
]

visualize_fused = fused.load(
    "https://github.com/fusedio/udfs/tree/2b25cb3/public/common/"
).utils.visualize


def y_decreasing(da, dim='y'):
    if da[dim][0] < da[dim][-1]:
        return da.reindex({dim: da[dim][::-1]})

    return da

def visualise(data, min, max, colormap, alphamask):
    import utils
    data = y_decreasing(data)
    
    rgb_image = visualize_fused(
        data=data,
        min=min,
        max=max,
        colormap=colormap,
    )
    ALPHA_CHANNEL = 3
    rgb_image[ALPHA_CHANNEL, :, :] = alphamask
    return rgb_image

visualize_fused = fused.load(
    "https://github.com/fusedio/udfs/tree/2b25cb3/public/common/"
).utils.visualize

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

    @fused.cache
    def fetch_repo(repo_name):
        return client.get_repo(repo_name)

    repo = fetch_repo(repo_name)

    @fused.cache()
    def fetch_dataset(dataset):
        return repo.to_xarray(dataset, decode_coords='all')

    ds = fetch_dataset(dataset)

    if time:
        ds = ds.sel(time=time)
        # ds = ds.chunk({'x': 1000, 'y': 1000})

    # print(bbox.total_bounds)
    clipped = ds.rio.clip_box(*bbox.total_bounds)[variable].compute()
    # print(clipped)
    return clipped


def visualise(data, min, max, colormap, alphamask):
    import utils
    data = y_decreasing(data)
    
    rgb_image = visualize_fused(
        data=data,
        min=min,
        max=max,
        colormap=colormap,
    )
    ALPHA_CHANNEL = 3
    rgb_image[ALPHA_CHANNEL, :, :] = alphamask
    return rgb_image