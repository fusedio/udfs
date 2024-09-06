@fused.udf
def udf(bbox, collection="3dep-seamless", band="data", res_factor:int=1):
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    import odc.stac
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    items = catalog.search( 
        collections=[collection],
        bbox=bbox.total_bounds,
    ).item_collection()
    print(items[0].assets.keys()) 
    print(f"Returned {len(items)} Items")
    resolution = int(20/res_factor * 2 ** (max(0, 13 - bbox.z[0])))
    print(f"{resolution=}")
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[band],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)
    arr = ds[band].max(dim="time")
    return utils.arr_to_plasma(arr.values, min_max=(0, 100), reverse=False)
