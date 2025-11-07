@fused.udf
def udf(
    bounds: fused.types.Bounds=[-130, 25, -60, 50]
):
    # Loading the common Fused hex library to access pre-installed H3 library in DuckDB
    common = fused.load("https://github.com/fusedio/udfs/tree/56ec615/public/common/")
    con = common.duckdb_connect() 

    query = """
        SELECT  
            h3_cell_to_parent(hex, 5) as hex, 
            avg(mean_value) as mean_value 
        FROM read_parquet('s3://fused-asset/data/dem/r6_z4_100k.parquet')
        where 1=1
        and h3_cell_to_lng(hex) between ? and ? 
        and h3_cell_to_lat(hex) between ? and ? 
        group by 1
    """
    data = con.execute(query, [bounds[0], bounds[2], bounds[1], bounds[3]]).df()

    print(data.T)

    return data