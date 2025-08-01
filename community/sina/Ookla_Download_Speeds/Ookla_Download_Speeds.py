@fused.udf
def udf(bounds: fused.types.Bounds = [-92.317,-64.329,116.124,79.475]):

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    zoom = common.estimate_zoom(bounds)

    file_path='s3://ookla-open-data/parquet/performance/type=mobile/year=2024/quarter=3/2024-07-01_performance_mobile_tiles.parquet'

    @fused.cache
    def get_data(bounds, file_path, h3_size):
        con = common.duckdb_connect()

        # DuckDB query to:
        # 1. Convert lat/long to H3 cells
        # 2. Calculate average download speed per cell
        # 3. Filter by geographic bounds
        qr=f'''select  h3_latlng_to_cell(tile_y, tile_x, {h3_size}) as hex, 
                    avg(avg_d_kbps) as metric
        from read_parquet("{file_path}") 
        where 1=1
        and tile_x between {bounds[0]} and {bounds[2]}
        and tile_y between {bounds[1]} and {bounds[3]}
        group by 1
        ''' 
        df = con.sql(qr).df()
        return df
        
    # Calculate H3 resolution based on zoom level:    
    res_offset=0
    res = max(min(int(2+zoom/1.5),8)-res_offset,2)
    df = get_data(bounds, file_path, h3_size=res)
    print(df)
    return df