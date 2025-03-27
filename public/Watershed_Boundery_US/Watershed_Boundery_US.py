@fused.udf
def udf(bounds: fused.types.Bounds = None, path:str='s3://fused-asset/infra/hydro_wbdhu12_us'):
    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    table_to_tile=common_utils.table_to_tile
    df = table_to_tile(tile, path, min_zoom=9, use_columns=['huc12', 'name', 'hutype'])
    return df 