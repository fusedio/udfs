@fused.udf
def udf(bounds: fused.types.Bounds):
    # adding custom path for spherely (not yet installed by default)
    import sys;
    sys.path.append(f"/mnt/cache/envs/geopython/lib/python3.11/site-packages")

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    # loading the Overture building polygons for the current bounds
    from utils import get_overture
    gdf = get_overture(tile, theme="buildings", use_columns=["geometry"])
    gdf = gdf[["geometry"]]

    # constructing spherely Geography objects through WKB
    import spherely
    geogs = spherely.from_wkb(gdf.geometry.to_wkb())

    # Calculating the area using shapely by reprojecting to UTM first
    geoms_utm = gdf.geometry.to_crs(gdf.estimate_utm_crs())
    gdf["area_utm"] = geoms_utm.area

    # Calculating the area using spherely
    gdf["area_spherical"] = spherely.area(geogs)

    # Calculating the geodesic area using pyproj (ellipsoid model)
    from pyproj import Geod
    geod = Geod(ellps="WGS84")
    gdf["area_geod"] = [geod.geometry_area_perimeter(pol)[0] for pol in gdf.geometry]

    print("Average relative difference compared to geodesic area:")
    print(f'Spherical: {((gdf["area_spherical"] - gdf["area_geod"]) / gdf["area_geod"] * 100).mean():.2f}%')
    print(f'UTM: {((gdf["area_utm"] - gdf["area_geod"]) / gdf["area_geod"] * 100).mean():.2f}%')

    return gdf