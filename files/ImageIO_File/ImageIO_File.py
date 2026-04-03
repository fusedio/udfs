@fused.udf
def udf(path: str, preview: bool):
    import imageio.v3 as iio
    import fsspec

    with fsspec.open(path, "rb") as f:
        im = iio.imread(f)
    transposed_image = im.transpose(2, 0, 1)

    if preview:
        w, h = im.shape[1], im.shape[0]
        if w > h:
            return transposed_image, (0, 0, 1, 1 / (w / h))
        else:
            return transposed_image, (0, 0, 1 / (h / w), 1)

    return transposed_image
