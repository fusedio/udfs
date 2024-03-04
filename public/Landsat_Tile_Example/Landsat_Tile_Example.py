# todo: investigate why sometime configure_s3_access get cached
def udf(
    bbox,
    time_of_interest="2023-09-01/2023-10-30",
    red_band="red",
    nir_band="nir08",
    collection="landsat-c2-l2",
):
    arr_to_plasma = fused.core.load_udf_from_github(
        "https://github.com/fusedio/udfs/tree/ccbab4334b0cfa989c0af7d2393fb3d607a04eef/public/common/"
    ).utils.arr_to_plasma
    from pystac.extensions.eo import EOExtension as eo
    import pystac_client
    import odc.stac

    odc.stac.configure_s3_access(requester_pays=True)
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    items = catalog.search(
        collections=[collection],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    # least_cloudy_item = min(items, key=lambda item: eo.ext(item).cloud_cover)
    # print(least_cloudy_item.assets.keys())
    print(f"Returned {len(items)} Items")
    resolution = int(5 * 2 ** (15 - bbox.z[0]))
    print(f"{resolution=}")
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[nir_band, red_band],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)
    ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])
    print(ndvi.shape)
    arr = ndvi.max(dim="time")
    # arr = ndvi.groupby("time.month").median()[0]
    return arr_to_plasma(arr.values, min_max=(0, 0.5))
