@fused.udf
def udf(
    bbox,
    var="NDVI",
    chip_len="256",
    buffer_degree=0.000,
):
    utils = fused.core.load_udf_from_github(
        "https://github.com/fusedio/udfs/tree/f928ee1bd5cbf72573b587c63a7cbfa4a24b8dfe/public/common/"
    ).utils
    min_zoom = 15
    if bbox.z[0] >= min_zoom:
        import numpy as np

        output_shape = (int(chip_len), int(chip_len))
        matching_items = utils.bbox_stac_items(
            bbox.geometry[0], table="s3://fused-asset/imagery/naip/"
        )
        max_matching_items = 10
        print(f"{len(matching_items)=}")
        if len(matching_items) < max_matching_items:
            input_tiff_path = matching_items.iloc[0].assets["naip-analytic"]["href"]
            crs = matching_items.iloc[0]["proj:epsg"]
            arr = utils.read_tiff_naip(
                bbox, input_tiff_path, crs, buffer_degree, output_shape
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
