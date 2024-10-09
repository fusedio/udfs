import numpy as np
import xarray as xr

def visualize(
    data: np.ndarray = None,
    mask: float | np.ndarray = None,
    min: float = 0,
    max: float = 1,
    opacity: float = 1,
    colormap = None,
):
    """Convert objects into visualization tiles."""
    
    import palettable
    from matplotlib.colors import LinearSegmentedColormap
    from matplotlib.colors import Normalize   

    if data is None:
        return
    
    if colormap is None:
        # Set a default colormap
        colormap = palettable.colorbrewer.sequential.Greys_9
        cm = colormap.mpl_colormap
    elif isinstance(colormap, palettable.palette.Palette):
        cm = colormap.mpl_colormap
    elif isinstance(colormap, (list, tuple)):
        cm = LinearSegmentedColormap.from_list('custom', colormap)
    else:
        print('visualize: no type match for colormap')

    if isinstance(data, xr.DataArray):
        # Convert from an Xarray DataArray to a Numpy ND Array
        data = data.values
    
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
        