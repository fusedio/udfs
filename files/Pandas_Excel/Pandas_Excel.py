@fused.udf
def udf(path: str):
    import pandas as pd

    df = pd.read_excel(path)
    print(df)
    return df
