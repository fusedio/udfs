@fused.udf
def udf(name: str = "Julien"):
    import pandas as pd

    return pd.DataFrame({"hello": [name]})
