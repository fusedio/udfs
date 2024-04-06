utils = fused.load(
    "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
).utils
arr_to_plasma = utils.arr_to_plasma

@fused.cache
def get_dem(bbox, overview):
    provider = "AWS"
    # collection = 'cop-dem-glo-90'
    collection = "cop-dem-glo-30"
    import odc.stac
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    if provider == "AWS":
        odc.stac.configure_s3_access(aws_unsigned=True)
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        import planetary_computer

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
        bbox=bbox.total_bounds,
    ).item_collection()
    if len(items) == 0:
        return None
    resolution = int(20 * 2 ** (13 - overview))
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=["data"],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)
    arr = ds["data"].max(dim="time")
    return arr