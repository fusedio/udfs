@fused.udf
def udf(n: int = 30):
    import pandas as pd

    # Load the slow compute UDF
    slow_udf = fused.load("slow_compute_udf")

    # Run it n times in parallel with udf.map()
    results = slow_udf.map(range(n))

    return results.df().reset_index(drop=True)
