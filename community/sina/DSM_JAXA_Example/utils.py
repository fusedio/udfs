import fused
import geopandas as gpd

# Load utility functions.
common = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils

def dsm_to_tile(
    bounds: fused.types.Bounds,
    z_levels=[4, 6, 9, 11],
    verbose=True
):

    tile = common.get_tiles(bounds)
    zoom = tile.iloc[0].z

    if zoom >= z_levels[2]:
        tiff_list = []
        if zoom >= z_levels[3]:
            overview_level = 0
        else:
            overview_level = 1
        if verbose:
            print(f"{overview_level=}")
        bounds_list = bounds_to_navigation(bounds, interval=1)

        for b in bounds_list:
            zoom_level = 3
            tiff_list.append(
                f"https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/2021-02/{zoom_level}/{b[0]}-{b[1]}/{b[0]}-{b[2]}-{b[1]}-{b[3]}-DSM.tiff"
            )
    elif zoom >= z_levels[0]:
        tiff_list = []
        if zoom >= z_levels[1]:
            overview_level = 0
        else:
            overview_level = 1
        if verbose:
            print(f"{overview_level=}")
        bounds_list = bounds_to_navigation(bounds, interval=10)
        for b in bounds_list:
            zoom_level = 2
            tiff_list.append(
                f"https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/2021-02/{zoom_level}/{b[0]}-{b[1]}/{b[0]}-{b[2]}-{b[1]}-{b[3]}-DSM.tiff"
            )
            # url='https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/2021-02/1/E000.00-E090.00/E000.00-N00.00-E090.00-N90.00-DSM.tiff'
    else:
        return tile
    if verbose:
        print(tiff_list)
    arr = common.mosaic_tiff(tile, tiff_list, overview_level=overview_level)
    return arr


def convert_lat(lat):
    if lat >= 0:
        return f"N{str(1000+lat)[-2:]}.00"
    else:
        return f"S{str(1000+abs(lat))[-2:]}.00"


def convert_lng(lng):
    if lng >= 0:
        return f"E{str(1000+lng)[-3:]}.00"
    else:
        return f"W{str(1000+abs(lng))[-3:]}.00"


def bounds_to_navigation(
    bounds: fused.types.Bounds,
    interval=1
):
    import numpy as np

    a = []
    if interval == 1:
        for lat in range(int(np.floor(bounds[1])), int(np.ceil(bounds[3]))):
            for lng in range(int(np.floor(bounds[0])), int(np.ceil(bounds[2]))):
                a.append(
                    [
                        convert_lng(lng),
                        convert_lng(lng + 1),
                        convert_lat(lat),
                        convert_lat(lat + 1),
                    ]
                )
    elif interval == 10:
        for lat in range(
            int(np.floor(bounds[1] / 10) * 10), int(np.ceil(bounds[3] / 10) * 10), 10
        ):
            for lng in range(
                int(np.floor(bounds[0] / 10) * 10),
                int(np.ceil(bounds[2] / 10) * 10),
                10,
            ):
                a.append(
                    [
                        convert_lng(lng),
                        convert_lng(lng + 10),
                        convert_lat(lat),
                        convert_lat(lat + 10),
                    ]
                )
    return a
