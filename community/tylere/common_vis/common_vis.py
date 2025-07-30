def udf(
    bbox   
):

    from pystac.extensions.eo import EOExtension as eo
    import pystac_client
    import odc.stac   
    import numpy
    import palettable


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

def visualize(
    data,
    mask = None,
    min = 0,
    max = 1,
    opacity = 1,
    colormap = None,
):
    """Convert objects into visualization tiles."""

    import numpy as np
    import palettable
    from matplotlib.colors import LinearSegmentedColormap
    from matplotlib.colors import Normalize   

    
    if colormap is None:
        # Set a default colormap
        colormap = palettable.colorbrewer.sequential.Greys_9
        cm = colormap.mpl_colormap
    elif isinstance(colormap, palettable.palette.Palette):
        cm = colormap.mpl_colormap
    elif isinstance(colormap, list):
        cm = LinearSegmentedColormap.from_list('custom', colormap)
    else:
        print('visualize: no type match for colormap')
    
    if isinstance(data, np.ndarray):
        norm_data = Normalize(vmin=min, vmax=max, clip=False)(data)
        mapped_colors = cm(norm_data)
        if isinstance(mask, (float, np.ndarray)):
            mapped_colors[:,:,3] = mask * opacity
        else:
            mapped_colors[:,:,3] = opacity
        
        # Convert to unsigned 8-bit ints for visualization.
        vis_dtype = np.uint8
        max_color_value = np.iinfo(vis_dtype).max
        norm_data255 = (mapped_colors * max_color_value).astype(vis_dtype)
        
        # Reshape array to 4 x nRow x nCol.
        shaped = norm_data255.transpose(2,0,1)
    
        return shaped
    else:
        print('visualize: data instance type not recognized')
