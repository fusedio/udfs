import fused

@fused.udf
def udf(bounds: fused.types.Bounds = [-122.499,37.707,-122.381,37.808], year="2022", chip_len:int=256):

    # Load pinned versions of utility functions.
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)
    zoom = tile.iloc[0].z

    if zoom >= 5:
        from utils import (
            LULC_IO_COLORS,
            arr_to_color,
            bounds_stac_items,
            mosaic_tiff,
            s3_to_https,
        )

        matching_items = bounds_stac_items(
            tile.geometry[0], table="s3://fused-asset/lulc/io10m/"
        )
        mask = matching_items["datetime"].map(lambda x: str(x)[:4] == year)
        tiff_list = (
            matching_items[mask]
            .assets.map(lambda x: s3_to_https(x["supercell"]["href"][:-17] + ".tif"))
            .values
        )
        data = mosaic_tiff(
            tile,
            tiff_list,
            output_shape=(chip_len, chip_len),
            overview_level=min(max(12 - zoom, 0), 4),
        )
        data = arr_to_color(data, color_map=LULC_IO_COLORS)
        print(data.shape)
        return data
    else:
        print("Please zoom in more.")
