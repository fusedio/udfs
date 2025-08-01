@fused.udf
def udf(count: int = 0):
    # Create duckdb connection
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    con = common.duckdb_connect()

    # Load and return 
    path = "https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/sf.h3cells.json"
    df = con.sql(f"FROM '{path}' WHERE count > {count}").df()

    return df
