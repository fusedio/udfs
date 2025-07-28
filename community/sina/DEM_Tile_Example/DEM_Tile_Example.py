def udf(
    bounds: fused.types.Bounds = [-77.083,38.804,-76.969,38.983],
    provider: str = "AWS"
):
    import odc.stac
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    import utils

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = common.get_tiles(bounds, clip=True)
    zoom = tile.iloc[0].z

    print(f'{type(tile) = }')
    
    collection = "cop-dem-glo-30"

    if provider == "AWS":
        odc.stac.configure_s3_access(aws_unsigned=True)
        catalog = pystac_client.Client.open(
            "https://earth-search.aws.element84.com/v1"
        )
    elif provider == "MSPC":
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. provider options are "AWS" and "MSPC"'
        )
    
    items = catalog.search(
        collections=[collection],
        bbox=bounds,
    ).item_collection()
    print(f"Returned {len(items)} Items")

    # Calculate the resolution based on zoom level.
    resolution = int(20 * 2 ** (13 - min(zoom, 13)))
    print(f"{resolution=}")

    # Load the data into an XArray dataset
    xarray_dataset = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=["data"],
        resolution=resolution,
        bbox=bounds,
    ).astype(float)
    
    # Use data from the most recent time.
    arr = xarray_dataset["data"].max(dim="time")

    # Visualize that data as an RGB image.
    rgb_image = common.visualize(
        data=arr,
        min=0,
        max=3000,
        colormap=palettable.matplotlib.Viridis_20,
    )
    return rgb_image
