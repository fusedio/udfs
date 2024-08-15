@fused.udf
def udf(path: str, preview: bool):
    import imageio.v3 as iio
    import s3fs

    with s3fs.S3FileSystem().open(path) as f:
        im = iio.imread(f)
        transposed_image = im.transpose(2, 0, 1)
        print(transposed_image)

        if preview:
            w, h = im.shape[1], im.shape[0]
            if w > h:
                return transposed_image, (0, 0, 1, 1 / (w / h))
            else:
                return transposed_image, (0, 0, 1 / (h / w), 1)

        return transposed_image
