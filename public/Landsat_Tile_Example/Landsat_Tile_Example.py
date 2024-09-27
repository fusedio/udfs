# todo: investigate why sometime configure_s3_access get cached
def udf(
    bbox,
    time_of_interest="2023-09-01/2023-10-30",
    red_band="red",
    nir_band="nir08",
    collection="landsat-c2-l2",
):

    import odc.stac
    import palettable
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    # Load utility functions.
    common_vis = fused.load(
        "https://github.com/fusedio/udfs/tree/aa3695a/community/tylere/common_vis"
    ).utils
    
    odc.stac.configure_s3_access(requester_pays=True)
    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    items = catalog.search(
        collections=[collection],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    print(f"Returned {len(items)} Items")

    resolution = int(5 * 2 ** (15 - bbox.z[0]))
    print(f"{resolution = }")
    
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[nir_band, red_band],
        resolution=resolution,
        bbox=bbox.total_bounds,
    ).astype(float)

    # Calculate the Normalized Difference Vegetation Index
    ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])

    # Select the maximum value across all times
    arr = ndvi.max(dim="time")
  
    return common_vis.visualize(
        arr.values,
        min=0,
        max=0.5,
        colormap=palettable.scientific.sequential.Bamako_20_r,
    )
