@fused.udf
def udf(bbox, context, year="2022"):
    if bbox.z[0] >= 5:
        from utils import (
            LULC_IO_COLORS,
            arr_to_color,
            bbox_stac_items,
            mosaic_tiff,
            s3_to_https,
        )

        matching_items = bbox_stac_items(
            bbox.geometry[0], table="s3://fused-asset/lulc/io10m/"
        )
        mask = matching_items["datetime"].map(lambda x: str(x)[:4] == year)
        tiff_list = (
            matching_items[mask]
            .assets.map(lambda x: s3_to_https(x["supercell"]["href"][:-17] + ".tif"))
            .values
        )
        data = mosaic_tiff(
            bbox,
            tiff_list,
            output_shape=(256, 256),
            overview_level=min(max(12 - bbox.z[0], 0), 4),
        )
        data = arr_to_color(data, color_map=LULC_IO_COLORS)
        return data
    else:
        print("Please zoom in more.")
