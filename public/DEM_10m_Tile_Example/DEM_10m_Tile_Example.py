@fused.udf
def udf(
    bounds: fused.types.TileGDF,
    collection="3dep-seamless",
    band="data",
    res_factor:int=1
):

    import odc.stac
    import palettable
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/5cfb808/public/common/"
    ).utils
    
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    items = catalog.search( 
        collections=[collection],
        bbox=bounds.total_bounds,
    ).item_collection()
    print(items[0].assets.keys()) 
    print(f"Returned {len(items)} Items")
    resolution = int(20/res_factor * 2 ** (max(0, 13 - bounds.z[0])))
    print(f"{resolution=}")
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[band],
        resolution=resolution,
        bbox=bounds.total_bounds,
    ).astype(float)
    
    # Use data from the most recent time.
    arr = ds[band].max(dim="time")
    
    # Visualize that data as an RGB image.
    rgb_image = utils.visualize(
        data=arr,
        min=0,
        max=100,
        colormap=palettable.matplotlib.Viridis_20,
    )
    return rgb_image