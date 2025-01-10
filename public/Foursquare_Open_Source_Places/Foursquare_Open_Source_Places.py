@fused.udf
def udf(
    bbox: fused.types.TileGDF,
    release: str = "2025-01-10",
    min_zoom: int = 10,
    use_columns: list = ["geometry", "name", "fsq_category_ids"],
):
    from utils import join_fsq_categories

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/19b5240/public/common/"
    ).utils

    path = f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/{release}/places/"
    df = utils.table_to_tile(
        bbox, table=path, min_zoom=min_zoom, use_columns=use_columns
    )

    df = join_fsq_categories(df, release=release)

    return df
