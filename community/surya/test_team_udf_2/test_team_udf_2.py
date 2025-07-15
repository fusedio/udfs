@fused.udf(cache_max_age="0s")
def udf():
    """
    This UDF will be used for integration tests
    Do not delete it
    """
    import pandas as pd
    return pd.DataFrame({"data": [0]})