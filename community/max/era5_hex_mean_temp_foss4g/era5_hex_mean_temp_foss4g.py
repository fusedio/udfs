@fused.udf
def udf(month='2024-05',
    bounds:list=[-130, 25, -60, 50]
):
    path = f's3://fused-asset/data/era5/t2m_daily_mean_v2_1000/month={month}/0.parquet'
    
    common = fused.load("https://github.com/fusedio/udfs/tree/56ec615/public/common/")
    con = common.duckdb_connect() 

    query = f"""
        WITH base_data AS (
            SELECT  
                h3_cell_to_parent(hex, 5) as hex, 
                avg(daily_mean) - 273.15 as daily_mean 
            FROM read_parquet('{path}')
            WHERE 1=1
            AND h3_cell_to_lng(hex) between {bounds[0]} and {bounds[2]} 
            AND h3_cell_to_lat(hex) between {bounds[1]} and {bounds[3]} 
            GROUP BY 1
        ),
        all_hexes AS (
            -- Get all hexes including neighbors by expanding k-ring
            SELECT DISTINCT unnest(h3_grid_disk(hex, 1)) as hex
            FROM base_data
        ),
        smoothed AS (
            -- For each hex (original + neighbors), get its k-ring neighbors
            SELECT 
                ah.hex as hex,
                unnest(h3_grid_disk(ah.hex, 1)) as neighbor_hex
            FROM all_hexes ah
        )
        SELECT 
            s.hex,
            round(avg(b.daily_mean), 2) as daily_mean
        FROM smoothed s
        JOIN base_data b ON b.hex = s.neighbor_hex
        GROUP BY s.hex
    """ 

    data = con.execute(query).df()

    print(f"{data.T=}")

    return data