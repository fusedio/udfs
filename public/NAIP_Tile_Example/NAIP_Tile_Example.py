@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.408,37.781,-122.391,37.796],
    var="NDVI",
    chip_len: int=256,
    buffer_degree=0.000,
):
    # convert bounds to tile
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = utils.get_tiles(bounds, clip=True)
    zoom = tile.iloc[0].z

    min_zoom = 15
    if zoom >= min_zoom:
        import numpy as np

        output_shape = (chip_len, chip_len)
        matching_items = utils.bounds_stac_items(
            tile.geometry[0], table="s3://fused-asset/imagery/naip/"
        )
        max_matching_items = 10
        print(f"{len(matching_items)=}")
        if len(matching_items) < max_matching_items:
            input_tiff_path = matching_items.iloc[0].assets["naip-analytic"]["href"]
            crs = matching_items.iloc[0]["proj:epsg"]
            arr = utils.read_tiff_naip(
                tile, input_tiff_path, crs, buffer_degree, output_shape
            )
            if var == "RGB":
                arr = arr[:3]
            elif var == "NDVI":
                ir = arr[3].astype(float)
                red = arr[0].astype(float)
                tresh = 0.1
                mask = ((ir - red) / (ir + red)) < tresh
                arr[3][mask] = 0
                arr = np.array([arr[3] * 1, arr[3] * 0, arr[3] * 0])
            else:
                raise ValueError(
                    f'{var=} does not exist. var options are "RGB" and "NDVI"'
                )
            return arr
        else:
            print(
                f"{matching_items} images exceeded max of {max_matching_items}. "
                "Please zoom in more."
            )
            return None
    else:
        print(f"minimum_zoom is {min_zoom}. Please zoom in more.")

