@fused.udf
def udf(bounds: fused.types.Bounds = [-122.499,37.707,-122.381,37.808], year="2022", chip_len:int=256):
    LULC_IO_COLORS = {
        1: (65, 155, 223),  # Water
        2: (57, 125, 73),  # Trees
        4: (57, 125, 73),  # Flooded vegetation
        5: (228, 150, 53),  # Crops
        7: (196, 40, 27),  # Built area
        8: (165, 155, 143),  # Bare ground
        9: (168, 235, 255),  # Snow
        10: (97, 97, 97),  # Clouds
        11: (227, 226, 195),  # Rangeland
    }

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)
    zoom = tile.iloc[0].z

    if zoom >= 5:
        matching_items = bounds_stac_items(
            tile.geometry[0], table="s3://fused-asset/lulc/io10m/"
        )
        mask = matching_items["datetime"].map(lambda x: str(x)[:4] == year)
        tiff_list = (
            matching_items[mask]
            .assets.map(lambda x: s3_to_https(x["supercell"]["href"][:-17] + ".tif"))
            .values
        )
        data = common.mosaic_tiff(
            tile,
            tiff_list,
            output_shape=(chip_len, chip_len),
            overview_level=min(max(12 - zoom, 0), 4),
        )
        data = arr_to_color(data, color_map=LULC_IO_COLORS)
        print(data.shape)
        return data, bounds
    else:
        print("Please zoom in more.")


def s3_to_https(path):
    arr = path[5:].split("/")
    out = "https://" + arr[0] + ".s3.amazonaws.com/" + "/".join(arr[1:])
    return out


def arr_to_color(arr, color_map={1: (65, 155, 223), 2: (57, 125, 73)}):
    import numpy as np

    def map_values(value, band=0):
        if value in color_map:
            return color_map[value][band]
        else:
            return 0

    mapped_arr = [np.vectorize(lambda x: map_values(x, i))(arr) for i in [0, 1, 2]]
    return np.asarray(mapped_arr).astype("uint8")


def bounds_stac_items(bounds, table):
    import fused
    import pandas as pd

    df = fused.get_chunks_metadata(table)
    df = df[df.intersects(bounds)]
    List = df[["file_id", "chunk_id"]].values
    rows_df = pd.concat(
        [fused.get_chunk_from_table(table, fc[0], fc[1]) for fc in List]
    )
    return rows_df[rows_df.intersects(bounds)]
