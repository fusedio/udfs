import fused
import numpy as np
import geopandas as gpd
import shapely
from matplotlib import colors
import matplotlib.pyplot as plt

def bounds_to_gdf(bounds_list, crs = 4326):
    box = shapely.box(*bounds_list)
    return gpd.GeoDataFrame(geometry=[box], crs = crs)

read_tiff = fused.load(
    "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
).utils.read_tiff
