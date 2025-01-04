@fused.udf
def udf(bbox: fused.types.TileGDF):    
    path='s3://fused-asset/infra/building_oak/'
    utils = fused.load('https://github.com/fusedio/udfs/tree/19b5240/public/common/').utils
    df = utils.table_to_tile(bbox, table=path, use_columns=None, min_zoom=13)
    return df