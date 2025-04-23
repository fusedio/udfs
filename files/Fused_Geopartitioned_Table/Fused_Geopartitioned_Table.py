@fused.udf
def udf(bounds: fused.types.Bounds, path: str, preview: bool):
    use_columns = ["geometry"] if preview else None
    common = fused.load(
        "https://github.com/fusedio/udfs/tree/b41216d/public/common/"
    ).utils
    base_path = (
        path.rsplit("/", maxsplit=1)[0]
        if path.endswith("/_sample") or path.endswith("/_metadata")
        else path
    )
    df = common.table_to_tile(bounds, table=base_path, use_columns=use_columns, min_zoom = 7)
    return df
