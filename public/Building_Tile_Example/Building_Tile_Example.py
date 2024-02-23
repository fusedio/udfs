@fused.udf
def udf(bbox, table_path="s3://fused-asset/infra/building_msft_us"): 
    utils = fused.utils.import_from_github('https://github.com/fusedio/udfs/tree/79d0e6a45c000cb1d99f1a7772c8babc0113afcd/public/common/').utils
    df=utils.table_to_tile(bbox, table=table_path) 
    # df.geometry = df.geometry.buffer(0.0001) 
    df['name']=''
    return df
