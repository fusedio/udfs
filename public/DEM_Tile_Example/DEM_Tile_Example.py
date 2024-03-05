def udf(bbox, provider="AWS"):
    utils = fused.load("https://github.com/fusedio/udfs/tree/f928ee1/public/common/").utils
    # collection = 'cop-dem-glo-90'
    collection = "cop-dem-glo-30"
    from pystac.extensions.eo import EOExtension as eo
    import pystac_client
    import odc.stac

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
    print(items[0].assets.keys())
    print(f"Returned {len(items)} Items")
    resolution = int(20 * 2 ** (13 - bbox.z[0]))
    print(f"{resolution=}")
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=["data"],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)
    arr = ds["data"].max(dim="time")
    return utils.arr_to_plasma(arr.values, min_max=(0, 500), reverse=False)
