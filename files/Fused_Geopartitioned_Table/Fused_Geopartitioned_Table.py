@fused.udf
def udf(bounds: fused.types.Tile, path: str, preview: bool):
    use_columns = ["geometry"] if preview else None
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/2ea46f3/public/common/"
    ).utils
    base_path = (
        path.rsplit("/", maxsplit=1)[0]
        if path.endswith("/_sample") or path.endswith("/_metadata")
        else path
    )
    df = utils.table_to_tile(bounds, table=base_path, use_columns=use_columns)
    return df
