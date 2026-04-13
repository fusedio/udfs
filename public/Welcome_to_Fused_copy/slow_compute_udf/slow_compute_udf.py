@fused.udf
def udf(val: int = 5):
    import time
    import pandas as pd

    # Simulate heavy compute
    time.sleep(5)

    return pd.DataFrame({"val": [val], "result": [val * 10]})
