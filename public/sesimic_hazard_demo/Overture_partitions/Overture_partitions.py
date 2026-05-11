@fused.udf
def udf():
    import geopandas as gpd
    bounds=[6.6272, 36.6197, 18.4802, 47.0921] #[6.6, 36.6, 18.5, 47.1]
    path: str = 's3://fused-asset/overture/2026-03-18.0/theme=buildings/type=building/part=2/_sample'
    preview: bool = False
    use_columns = ['geometry'] if preview else None
    common = fused.load('https://github.com/fusedio/udfs/tree/d4f567c/public/common/')
    base_path = path.rsplit('/', maxsplit=1)[0] if path.endswith('/_sample') or path.endswith('/_metadata') else path
    df = common.table_to_tile(bounds, table=base_path, use_columns=use_columns, min_zoom=7)
    print(df.T)
    gdf = gpd.GeoDataFrame(df)
    return gdf