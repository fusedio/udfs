import shapely
import geopandas as gpd
def bounds_to_gdf(bounds_list, crs = 4326):
    box = shapely.box(*bounds_list)
    return gpd.GeoDataFrame(geometry=[box], crs = crs)