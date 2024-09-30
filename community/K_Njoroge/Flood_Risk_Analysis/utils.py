import fused
import geopandas as gpd
import numpy as np

read_tiff = fused.load(
    "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
).utils.read_tiff
mosaic_tiff = fused.load(
    "https://github.com/fusedio/udfs/tree/3c4bc47/public/common/"
).utils.mosaic_tiff
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


def bbox_stac_items(bbox, table):
    import fused
    import pandas as pd

    df = fused.get_chunks_metadata(table)
    df = df[df.intersects(bbox)]
    List = df[["file_id", "chunk_id"]].values
    rows_df = pd.concat(
        [fused.get_chunk_from_table(table, fc[0], fc[1]) for fc in List]
    )
    return rows_df[rows_df.intersects(bbox)]


def get_lulc(bbox, year):
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
    return data


def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-03-12-alpha-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: gpd.GeoDataFrame = None,
    point_convert: str = None,
):
    import concurrent.futures
    import logging

    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box, shape

    utils = fused.load("https://github.com/fusedio/udfs/tree/main/public/common/").utils

    if release == "2024-02-15-alpha-0":
        if overture_type == "administrative_boundary":
            overture_type = "administrativeBoundary"
        elif overture_type == "land_use":
            overture_type = "landUse"
        theme_per_type = {
            "building": "buildings",
            "administrativeBoundary": "admins",
            "place": "places",
            "landUse": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    else:
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }

    if theme is None:
        theme = theme_per_type.get(overture_type, "buildings")

    if overture_type is None:
        type_per_theme = {v: k for k, v in theme_per_type.items()}
        overture_type = type_per_theme[theme]

    if num_parts is None:
        num_parts = 1 if overture_type != "building" else 5

    if min_zoom is None:
        if theme == "admins":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        bounds = polygon.geometry.bounds
        bbox = gpd.GeoDataFrame(
            {
                "geometry": [
                    box(
                        bounds.minx.loc[0],
                        bounds.miny.loc[0],
                        bounds.maxx.loc[0],
                        bounds.maxy.loc[0],
                    )
                ]
            }
        )

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return utils.table_to_tile(
                bbox, table=part_path, use_columns=use_columns, min_zoom=min_zoom
            )
        except ValueError:
            return None

    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        # Don't bother creating a thread pool to do one thing
        dfs = [get_part(0)]

    dfs = [df for df in dfs if df is not None]

    if len(dfs):
        gdf = pd.concat(dfs)

    else:
        logging.warn("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    return gdf
