# ref: https://data.earth.jaxa.jp/app/viewer/v1/?collection=https://s3.ap-northeast-1.wasabisys.com/je-pds/cog/v1/JAXA.EORC_ALOS.PRISM_AW3D30.v3.2_global/collection.json&band=DSM&date=2021-02-01T00:00:00.000Z
@fused.udf
def udf(bbox, min_max=(0, 255), z_levels=[4, 6, 9, 11], verbose=False):
    arr_to_plasma = fused.core.load_udf_from_github(
        "https://github.com/fusedio/udfs/tree/ccbab4334b0cfa989c0af7d2393fb3d607a04eef/public/common/"
    ).utils.arr_to_plasma
    from utils import dsm_to_tile

    arr = dsm_to_tile(bbox, z_levels=z_levels, verbose=verbose)
    print("done")
    return arr_to_plasma(arr, min_max=min_max)
