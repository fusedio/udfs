@fused.udf
def udf(bounds: fused.types.Bounds, preview: bool = False):
    import imageio.v3 as iio
    import s3fs
    print(bounds)
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tiles = common.get_tiles(bounds)
    print(tiles)
    x, y, z = tiles.iloc[0][["x", "y", "z"]]
    path = f"s3://elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
    print(path)

    # load dem
    @fused.cache
    def load(bounds):
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

    arr = load(bounds)

    if tiles.iloc[0].z < 10:
        return None

    # Load pinned versions of utility functions.

    overture_maps = fused.load("https://github.com/fusedio/udfs/tree/38ff24d/public/Overture_Maps_Example")
    # Load Overture Buildings
    gdf_overture = overture_maps.get_overture(bounds=bounds)

    gdf_zonal = common.geom_stats(gdf_overture, arr[0, :, :])
    print(gdf_zonal.T)
    return gdf_zonal