@fused.udf
def udf(count: int = 0):
    import duckdb

    h3_utils = fused.load(
        "https://github.com/fusedio/udfs/tree/870e162/public/DuckDB_H3_Example/"
    ).utils

    # Create DuckDB connection
    con = duckdb.connect()
    con.sql(f"""INSTALL httpfs; LOAD httpfs;""")
    h3_utils.load_h3_duckdb(con)

    # Load and return data
    path = "https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/sf.h3cells.json"
    df = con.sql(f"FROM '{path}' WHERE count > {count}").df()

    return df
