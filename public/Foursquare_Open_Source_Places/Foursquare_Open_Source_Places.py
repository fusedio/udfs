@fused.udf
def udf(
    bounds: fused.types.Bounds,
    release: str = "2025-01-10",
    min_zoom: int = 10,
    use_columns: list = ["geometry", "name", "fsq_category_ids"],
):
    from utils import join_fsq_categories

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)


    path = f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/places/"
    df = utils.table_to_tile(
        tile, table=path, min_zoom=min_zoom, use_columns=use_columns
    )

    df = join_fsq_categories(df, release=release)

    return df
