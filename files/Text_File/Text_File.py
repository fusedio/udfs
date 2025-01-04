@fused.udf
def udf(path: str):
    import fsspec

    with fsspec.open(path, "r") as f:
        print(f.read())
