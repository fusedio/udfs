@fused.udf
def udf(month='2024-05',
    bounds:fused.types.Bounds=[-130, 25, -60, 50],
    hex_res: int = 4
):
    # month lands in the object path inside read_parquet(); keep it to YYYY-MM.
    import re as _re
    if not _re.fullmatch(r"\d{4}-\d{2}", str(month)):
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    # bounds only guarantees four elements, not four numbers: coerce before
    # they reach the query.
    bounds = [float(v) for v in bounds]
    path = f's3://fused-asset/data/era5/t2m_daily_mean_v4_1000/month={month}/0.parquet'

    # This loads DuckDB with the spatial & H3 extensions directly
    common = fused.load("https://github.com/fusedio/udfs/tree/56ec615/public/common/")
    con = common.duckdb_connect() 

    query = f"""
        SELECT  
            h3_cell_to_parent(hex, {hex_res}) as hex,
            round(avg(daily_mean) - 273.15, 2) as monthly_mean_temp
        FROM read_parquet('{path}')
        WHERE 1=1
        AND h3_cell_to_lng(hex) between {bounds[0]} and {bounds[2]}
        AND h3_cell_to_lat(hex) between {bounds[1]} and {bounds[3]} 
        GROUP BY 1
    """ 

    data = con.execute(query).df()

    print(f"{data.T=}")

    return data























    