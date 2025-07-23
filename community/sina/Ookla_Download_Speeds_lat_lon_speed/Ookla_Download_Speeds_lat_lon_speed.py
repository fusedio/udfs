@fused.udf
def udf(bounds: fused.types.Bounds=[-92.317,-64.329,116.124,79.475], lat: float=37.7749, lon: float=-122.4194):
    
    file_path='s3://ookla-open-data/parquet/performance/type=mobile/year=2024/quarter=3/2024-07-01_performance_mobile_tiles.parquet'
    
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    
    # Sample usage: Set default lat/lon for San Francisco if none provided
    if lat is None and lon is None and bounds is None:
        print("Using sample coordinates for San Francisco")
        lat = 37.7749
        lon = -122.4194
    
    # Check if we're using point query or bounds
    if lat is not None and lon is not None:
        # Create a small bounding box around the input lat/lon
        buffer = 0.01  # ~1km at equator
        total_bounds = [lon - buffer, lat - buffer, lon + buffer, lat + buffer]
        using_point_query = True
    else:
        # Use the provided bounds
        total_bounds = bounds.total_bounds
        using_point_query = False
    
    @fused.cache
    def get_data(total_bounds, file_path, h3_size):
        con = utils.duckdb_connect()
        # DuckDB query to:
        # 1. Convert lat/long to H3 cells
        # 2. Calculate average download speed per cell
        # 3. Filter by geographic bounds
        qr=f'''select  h3_latlng_to_cell(tile_y, tile_x, {h3_size}) as hex, 
                    avg(avg_d_kbps) as metric
        from read_parquet("{file_path}") 
        where 1=1
        and tile_x between {total_bounds[0]} and {total_bounds[2]}
        and tile_y between {total_bounds[1]} and {total_bounds[3]}
        group by 1
        ''' 
        df = con.sql(qr).df()
        return df
    
    # Calculate H3 resolution based on zoom level or use a fixed high resolution for point queries    
    if using_point_query:
        res = 8  # Use high resolution for point queries
    else:
        res_offset = 0
        res = max(min(int(2+bounds.z[0]/1.5),8)-res_offset,2)
    
    df = get_data(total_bounds, file_path, h3_size=res)
    
    # For point queries, find the closest H3 cell and return its speed
    if using_point_query:
        con = utils.duckdb_connect()
        
        # Convert the input lat/lon to an H3 cell
        point_cell_query = f'''
        SELECT h3_latlng_to_cell({lat}, {lon}, {res}) as point_hex
        '''
        point_cell_df = con.sql(point_cell_query).df()
        
        if not point_cell_df.empty:
            point_cell = point_cell_df['point_hex'].iloc[0]
            
            # Find the cell in our results that matches the point's cell
            if 'hex' in df.columns and not df.empty:
                point_speed = df[df['hex'] == point_cell]
                
                if not point_speed.empty:
                    print(f"Speed at location ({lat}, {lon}): {point_speed['metric'].iloc[0]} kbps")
                    point_speed.rename(columns={'metric': 'internet_speed_kbs'}, inplace=True)
                    print(f"{point_speed=}")
                    return point_speed
                else:
                    print(f"No exact match found for location ({lat}, {lon}). Returning all cells in area.")
            else:
                print(f"No data found for location ({lat}, {lon})")
    
    print(df) 
    return df
