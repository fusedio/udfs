def udf(
    bbox: fused.types.TileGDF = None,
    example_type: str = "named_colormap",
):
    """Demonstrate the use of visualize()"""
    import numpy as np
    import odc.stac   
    import palettable
    import pystac_client
    
    from utils import visualize
    
    def get_sample_image():
        band = 'data'
        collection = 'cop-dem-glo-30'
        odc.stac.configure_s3_access(aws_unsigned=True)
        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
        items = catalog.search(
            collections=[collection], 
            bbox=bbox.total_bounds,
        ).item_collection()
        try:
            resolution = int(20*2**(13-bbox.z[0]))
        except ValueError:
            resolution = 10
        try:
            ds = odc.stac.load(   
                items, 
                crs="EPSG:3857",
                bands=[band], 
                resolution=resolution,
                bbox=bbox.total_bounds,   
            ).astype(float)
            arr = ds[band].max(dim='time').values
            return arr
        except ValueError:
            # Return an empty array for tiles with no data.
            return np.full((256, 256), np.nan)


    print(f'Example type: {example_type}')
    match example_type:
        case "named_colormap":
            colormap=palettable.matplotlib.Plasma_15
        case 'manual_colormap':
            colormap=((1, 0, 0),   
                       'yellow',
                       'cyan',
                       (0, 1, 0),
                       (0, 0, 1))

    arr = get_sample_image()
    mask = arr > 0
    return visualize(arr, 
                     min=0,
                     max=1500,
                     mask=mask, 
                     opacity=0.5,
                     colormap=colormap)
