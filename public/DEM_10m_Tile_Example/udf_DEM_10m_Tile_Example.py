def udf(bbox, collection='3dep-seamless', band='data'):
    from utils import arr_to_plasma
    from pystac.extensions.eo import EOExtension as eo
    import pystac_client
    import odc.stac   
    import planetary_computer
    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1", 
                                            modifier=planetary_computer.sign_inplace) 
    items = catalog.search(
    collections=[collection], 
    bbox=bbox.total_bounds,
    ).item_collection()
    print(items[0].assets.keys())
    print(f"Returned {len(items)} Items") 
    resolution = int(20*2**(13-bbox.z[0]))
    print(f'{resolution=}')
    ds = odc.stac.load(   
        items, 
        crs="EPSG:3857",
        bands=[band], 
        resolution=resolution,
        bbox=bbox.total_bounds,   
    ).astype(float) 
    arr = ds[band].max(dim='time')
    return arr_to_plasma(arr.values, min_max=(0,100))