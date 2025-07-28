@fused.udf
def udf(bbox: fused.types.Tile, preview: bool = False):
    import imageio.v3 as iio
    import s3fs

    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    path = f"s3://elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
    print(path)

    # load dem
    @fused.cache
    def load(bbox):
        with s3fs.S3FileSystem().open(path) as f:
            im = iio.imread(f)
            transposed_image = im.transpose(2, 0, 1)
            print(transposed_image)
            if preview:
                w, h = (im.shape[1], im.shape[0])
                if w > h:
                    return (transposed_image, (0, 0, 1, 1 / (w / h)))
                else:
                    return (transposed_image, (0, 0, 1 / (h / w), 1))
            return transposed_image

    arr = load(bbox)

    if bbox.iloc[0].z < 10:
        return None

    # Load pinned versions of utility functions.
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/ee9bec5/public/common/"
    ).utils
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils
    # Load Overture Buildings
    gdf_overture = overture_utils.get_overture(bbox=bbox)

    gdf_zonal = utils.geom_stats(gdf_overture, arr[0, :, :])
    print(gdf_zonal.T)
    return gdf_zonal
    return gdf_overture

    return arr
