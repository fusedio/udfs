import xarray as xr
import numpy as np
from scipy.spatial import KDTree

def idw(riv_dem: xr.DataArray, dem: xr.DataArray, n_nb: int) -> xr.DataArray:
    """Interpolate grid DEM from river DEM using Inverse Distance Weighting."""
    riv_coords = np.column_stack((riv_dem.x, riv_dem.y))
    kdt = KDTree(riv_coords)
    dem_grid = np.dstack(np.meshgrid(dem.x, dem.y)).reshape(-1, 2)
    distances, idx = kdt.query(dem_grid, k=n_nb)
    weights = np.reciprocal(distances)
    weights = weights / weights.sum(axis=1, keepdims=True)
    interp = weights * riv_dem.to_numpy()[idx]
    interp = interp.sum(axis=1).reshape((dem.sizes["y"], dem.sizes["x"]))
    return xr.DataArray(interp, dims=("y", "x"), coords={"x": dem.x, "y": dem.y})
