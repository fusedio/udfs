@fused.udf
def udf(bounds: fused.types.TileGDF=None, path:str='s3://fused-asset/infra/hydro_wbdhu12_us'):
    table_to_tile=fused.load('https://github.com/fusedio/udfs/tree/eb03067/public/common/').utils.table_to_tile
    df = table_to_tile(bounds, path, min_zoom=9, use_columns=['huc12', 'name', 'hutype']) 
    return df 