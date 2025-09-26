@fused.udf
def udf(name: str = "community"):
    import pandas as pd

    return pd.DataFrame({"hello": [name]})
