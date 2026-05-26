@fused.udf
def udf(
    res: int = 8,
    bounds: fused.types.Bounds = [-74.556, 40.400, -73.374, 41.029]
):
    common = fused.load("https://github.com/fusedio/udfs/tree/9a3aae2/public/common/")

    # Bounds to hex (to keep only hex within the current viewport)
    hex_gdf = common.bounds_to_hex(
        bounds,
        res=res,
        hex_col="hex",
    )
    hex_list = hex_gdf["hex"].tolist()

    con = common.duckdb_connect()

    qr = """
        SELECT
            state,
            county,
            POP20,
            HOUSING20,
            hex
        FROM
            's3://us-west-2.opendata.source.coop/fused/hex/release_2025_04_beta/census/2020_partitioned_h8.parquet'
        WHERE
            hex = ANY(?)
    """

    # Execute the parameterized query with hex_list directly
    df = con.execute(qr, [hex_list]).df()

    # Debug: show the resulting schema
    print(df.T)

    return df