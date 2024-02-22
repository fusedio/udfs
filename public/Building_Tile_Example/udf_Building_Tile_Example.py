@fused.udf
def udf(bbox, table_path="s3://fused-asset/infra/building_msft_us"): 
    from utils import table_to_tile
    df=table_to_tile(bbox, table=table_path) 
    # df.geometry = df.geometry.buffer(0.0001) 
    df['name']=''
    return df