@fused.udf
def udf(path: str):
    import s3fs

    with s3fs.S3FileSystem().open(path, "r") as f:
        print(f.read())
