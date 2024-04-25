@fused.udf
def udf(year: str = '2010', month: str = '01', n_rows: int = 1000, weight_by: str | None = None):
    import pandas as pd

    @fused.cache
    def get_data(year, month):
        return pd.read_parquet(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet')

    @fused.cache
    def limit_data(year, month, n_rows):
        return get_data(year=year, month=month).head(n_rows)
    
    df = limit_data(year=year, month=month, n_rows=n_rows)
    print(df.T)
    df['geometry'] = None
    if weight_by:
        df['weight'] = df[weight_by] / df[weight_by].max()
    else:
        df['weight'] = 1
    return df
    