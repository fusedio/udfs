@fused.udf
def udf(bbox: fused.types.TileGDF, path: str, preview: bool):
    use_columns = ["geometry"] if preview else None
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/19b5240/public/common/"
    ).utils
    base_path = path.rsplit("/", maxsplit=1)[0] if path.endswith("/_sample") else path
    df = utils.table_to_tile(bbox, table=base_path, use_columns=use_columns)
    return df
