@fused.udf
def udf(path: str = "s3://fused-sample/demo_data/"):
    import pandas as pd

    files  = fused.api.list(path)
    print(files)
    return files
