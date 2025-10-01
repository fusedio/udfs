@fused.udf
def udf(path: str):
    import pandas as pd

    df = pd.read_csv(path)
    print(df.T) # transpose the dataframe to make data schema more visible to AI 
    return df
