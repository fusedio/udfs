@fused.udf
def udf(
    # Florence: Part 1, file 36
    # path: str = "s3://fused-asset/overture/2026-03-18.0/theme=buildings/type=building/part=0/52.parquet", 
    path: str = "s3://fused-asset/overture/2026-03-18.0/theme=buildings/type=building/part=2/36.parquet", 
    res: int = 6
):
    import geopandas as gpd

    common = fused.load("https://github.com/fusedio/udfs/tree/4dde28e/public/common/")
    con = common.duckdb_connect()
    gdf = con.sql(f"""
        SELECT 
            h3_latlng_to_cell(ST_Y(ST_Centroid(geometry::GEOMETRY)), ST_X(ST_Centroid(geometry::GEOMETRY)), {res}) AS hex,
            sum(ST_Area_Spheroid(geometry::GEOMETRY)) as total_area_sqm,
            count(1) as building_count
        FROM read_parquet('{path}') 
        group by 1
        order by 1
    """).df()
    return gdf.reset_index(drop=True)