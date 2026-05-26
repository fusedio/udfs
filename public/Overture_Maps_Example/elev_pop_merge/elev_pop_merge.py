@fused.udf
def udf(bounds: fused.types.Bounds=[-74.556, 40.4, -73.374, 41.029], res: int=8):
    
    elevation = fused.run('copdem_elevation_hex8', bounds=bounds, res=res)
    print(f"{elevation.T=}")

    population = fused.run('census_h8_within_bounds', bounds=bounds, res=res)
    print(f'{population.T=}')

    common = fused.load("https://github.com/fusedio/udfs/tree/9a3aae2/public/common/")
    con = common.duckdb_connect()

    # Joining both datasets
    qr = f"""
    SELECT 
        e.hex,
        e.data_avg as avg_elevation,
        p.POP20 as POP20,
        p.HOUSING20 as HOUSING20,
        p.county as county
    FROM elevation as e
    LEFT JOIN population as p
    ON e.hex = p.hex
    """
    join_df = con.execute(qr).df()
    
    return join_df