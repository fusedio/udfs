import fused

@fused.udf
def udf(bounds: fused.types.TileGDF, context, year="2022", chip_len:int=256):
    if bounds.z[0] >= 5:
        from utils import (
            LULC_IO_COLORS,
            arr_to_color,
            bbox_stac_items,
            mosaic_tiff,
            s3_to_https,
        )

        matching_items = bbox_stac_items(
            bounds.geometry[0], table="s3://fused-asset/lulc/io10m/"
        )
        mask = matching_items["datetime"].map(lambda x: str(x)[:4] == year)
        tiff_list = (
            matching_items[mask]
            .assets.map(lambda x: s3_to_https(x["supercell"]["href"][:-17] + ".tif"))
            .values
        )
        data = mosaic_tiff(
            bounds,
            tiff_list,
            output_shape=(chip_len, chip_len),
            overview_level=min(max(12 - bounds.z[0], 0), 4),
        )
        data = arr_to_color(data, color_map=LULC_IO_COLORS)
        print(data.shape)
        return data
    else:
        print("Please zoom in more.")
