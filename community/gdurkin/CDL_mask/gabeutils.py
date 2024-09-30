import fused
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import shapely
from matplotlib import colors


def bounds_to_gdf(bounds_list, crs=4326):
    box = shapely.box(*bounds_list)
    return gpd.GeoDataFrame(geometry=[box], crs=crs)


read_tiff = fused.load(
    "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
).utils.read_tiff
