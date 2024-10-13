@fused.udf
def udf(path: str='s3://fused-asset/misc/nyc/TLC_2010_count_hourly_hex12.parquet'):
    hex_res=11
    import h3
    import pandas as pd
    def get_data(path, hex_res, hourly=False):
        df = pd.read_parquet(path)   
        df['hex'] = df['hex'].map(lambda x:h3.cell_to_parent(x, hex_res))
        if hourly:
            df = df.groupby(['hex','hour'])['cnt'].sum().reset_index()
        else:
            df = df.groupby('hex')['cnt'].sum().reset_index()
        return df
    df=get_data(path, hex_res, hourly=False)
    print(df)
    df.cnt=df.cnt/df.cnt.max()*3000    
    return df