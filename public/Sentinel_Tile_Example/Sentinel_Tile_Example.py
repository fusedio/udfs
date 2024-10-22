def udf(
    bbox,
    provider="AWS",
    time_of_interest="2023-11-01/2023-12-30"
):  
    """
    This UDF returns Sentinel 2 NDVI of the passed bounding box (viewport if in Workbench, or {x}/{y}/{z} in HTTP endpoint)
    Data fetched from either AWS S3 or Microsoft Planterary Computer
    """
    import odc.stac
    import planetary_computer
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo
    import utils
    
    if provider == "AWS":
        red_band = "red"
        nir_band = "nir"
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider == "MSPC":
        red_band = "B04"
        nir_band = "B08"
        catalog = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1",
            modifier=planetary_computer.sign_inplace,
        )
    else:
        raise ValueError(
            f'{provider=} does not exist. provider options are "AWS" and "MSPC"'
        )
    
    items = catalog.search(
        collections=["sentinel-2-l2a"],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    
    # Capping resolution to min 10m, the native Sentinel 2 pixel size
    resolution = int(10 * 2 ** max(0, (15 - bbox.z[0])))
    print(f"{resolution=}")

    if len(items) < 1:
        print(f'No items found. Please either zoom out or move to a different area')
    else:
        print(f"Returned {len(items)} Items")
    
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
        
        rgb_image = utils.visualize(
            data=arr,
            min=0,
            max=1,
            colormap=['black', 'green']
        )
        return rgb_image
