def udf(bbox, provider='AWS', time_of_interest = "2023-11-01/2023-12-30"):
    from utils import arr_to_plasma 
    from pystac.extensions.eo import EOExtension as eo
    import pystac_client
    import odc.stac  
    if provider=='AWS':
        # odc.stac.configure_s3_access(requester_pays=True)
        red_band='red'
        nir_band='nir'
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    elif provider=='MSPC':
        import planetary_computer
        red_band='B04'
        nir_band='B08'
        catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1", modifier=planetary_computer.sign_inplace) 
    else:
        raise ValueError(f'{provider=} does not exist. provider options are "AWS" and "MSPC"')
    items = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=bbox.total_bounds,
    datetime=time_of_interest,
    query={"eo:cloud_cover": {"lt": 10}},
    ).item_collection()
    # least_cloudy_item = min(items, key=lambda item: eo.ext(item).cloud_cover)
    # print(least_cloudy_item.assets.keys())
    print(f"Returned {len(items)} Items")
    resolution = int(5*2**(15-bbox.z[0]))
    print(f'{resolution=}')
    ds = odc.stac.load(
        items, 
        crs="EPSG:3857",
        bands=[nir_band,red_band], 
        resolution=resolution,
        bbox=bbox.total_bounds,   
    ).astype(float)
    ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])
    print(ndvi.shape)
    arr = ndvi.max(dim='time')
    # arr = ndvi.groupby("time.month").median()[0]
    return arr_to_plasma(arr.values, min_max=(0,.8))
