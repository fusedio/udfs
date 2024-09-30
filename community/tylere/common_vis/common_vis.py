def udf(
    bbox   
):

    from pystac.extensions.eo import EOExtension as eo
    import pystac_client
    import odc.stac   
    import numpy
    import palettable

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
        ds = odc.stac.load(   
            items, 
            crs="EPSG:3857",
            bands=[band], 
            resolution=resolution,
            bbox=bbox.total_bounds,   
        ).astype(float) 
        arr = ds[band].max(dim='time').values
        return arr

    arr = get_sample_image()
    return visualize(arr, 
                     min=0,
                     max=1500,
                     mask=arr>0, 
                     opacity=0.5,
                     # Named colormap example
                     colormap=palettable.matplotlib.Plasma_15,
                     # Manual colormap example
                     # colormap=[(1, 0, 0),   
                     #           'yellow',
                     #           'cyan',
                     #           (0, 1, 0),
                     #           (0, 0, 1)],
                    )
