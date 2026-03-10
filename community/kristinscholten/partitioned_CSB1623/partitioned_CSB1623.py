@fused.udf
def udf(bounds: fused.types.Bounds = None, path: str='s3://fused-asset/misc/sina/partitioned_CSB1623/_sample', preview: bool=False):
    use_columns = ['geometry'] if preview else None
    common = fused.load("https://github.com/fusedio/udfs/tree/507b2a3/public/common/")
    base_path = path.rsplit('/', maxsplit=1)[0] if path.endswith('/_sample') or path.endswith('/_metadata') else path
    df = common.table_to_tile(bounds, table=base_path, use_columns=use_columns, min_zoom=7)
    return df