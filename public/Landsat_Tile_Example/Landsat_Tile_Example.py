# todo: investigate why sometime configure_s3_access get cached
@fused.udf
def udf(
    bounds: fused.types.Tile,
    time_of_interest="2022-09-01/2023-10-30",
    red_band="red",
    nir_band="nir08",
    collection="landsat-c2-l2",
    cloud_threshold=10,
):
    """Display NDVI based on Landsat & STAC"""
    import odc.stac
    import palettable
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    # Load utility functions.
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/5cfb808/public/common/"
    ).utils

    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    
    # Search for imagery within a specified bounding box and time period
    items = catalog.search(
        collections=[collection],
        bbox=bounds.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": cloud_threshold}},
    ).item_collection()
    print(f"Returned {len(items)} Items")

    # Determine the pixel spacing for the zoom level
    pixel_spacing = int(5 * 2 ** (15 - bounds.z[0]))
    print(f"{pixel_spacing = }")

    # Load imagery into an XArray dataset
    odc.stac.configure_s3_access(requester_pays=True)
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[nir_band, red_band],
        resolution=pixel_spacing,
        bbox=bounds.total_bounds,
    ).astype(float)

    # Calculate the Normalized Difference Vegetation Index
    ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])

    # Select the maximum value across all times
    arr = ndvi.max(dim="time")
  
    return utils.visualize(
        arr.values,
        min=0,
        max=0.5,
        colormap=palettable.scientific.sequential.Bamako_20_r,
    )
