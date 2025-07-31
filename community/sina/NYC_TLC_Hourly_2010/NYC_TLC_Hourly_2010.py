@fused.udf
def udf(hex_res:int=10, path: str='s3://fused-asset/misc/nyc/TLC_2010_count_hourly_hex12.parquet', hourly=True):
    import h3
    import pandas as pd
    hex_res=min(hex_res,12)
    @fused.cache
    def get_data(path, hex_res, hourly=False):
        version='1.0'
        df = pd.read_parquet(path)   
        df['hex'] = df['hex'].map(lambda x:h3.cell_to_parent(x, hex_res))
        if hourly:
            df = df.groupby(['hex','hour'])['cnt'].sum().reset_index()
        else:
            df = df.groupby('hex')['cnt'].sum().reset_index()
        return df
    df=get_data(path, hex_res, hourly=hourly)
    # df=df[df.cnt>=100]
    df['metric']=df.cnt**0.5*10
    print(df)    
    return df