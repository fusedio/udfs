@fused.udf
def udf(path: str):
    import pandas as pd

    df = pd.read_csv(path)
    print(df)
    return df
