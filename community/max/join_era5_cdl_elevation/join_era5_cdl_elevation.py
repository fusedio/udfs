@fused.udf
def udf(
    bounds: fused.types.Bounds = [-125, 24, -66, 49]  # Continental US bounds (west, south, east, north),
    min_elevation: float = None,
    max_elevation: float = None,
    k_ring_size: int = 1  # K-ring distance (1 or 2)
):
    import pandas as pd
    import duckdb

    # Get elevation data with bounds
    elevation_df = fused.run("elevation_hex_mean_height", bounds=bounds)
    print(f"{elevation_df.T=}")
    
    # Get ERA5 temperature data
    era5_temp = fused.run("era5_hex_mean_temp_foss4g", bounds=bounds)  # era5_hex_mean_temp_foss4g
    print(f"ERA5 temp shape: {era5_temp.shape}")
    print(f"{era5_temp.T=}")
    
    # Get all CDL crop data
    cdl_raw = fused.run("cdl_data_all_crops_staging")
    print(f'{cdl_raw.T=}')
    crops_to_remove = [
        64,  # shrubland
        152, # shrubland
        176, # pasture
        63,  # forest
        142, # forest
        141, # other forest
        190, # woody wetland
        111, # more wetland
        195, # open water
        131, # Barren
        121, # developped / open space 
        143, # mixed forest 
    ]

    # Filter out any hex that have any crop in crops_to_remove
    cdl_raw = cdl_raw[~cdl_raw['crop_type'].isin(crops_to_remove)]
    
    print(f"ERA5 shape: {elevation_df.shape}, hex range: {elevation_df['hex'].min():.2e} to {elevation_df['hex'].max():.2e}")
    print(f"CDL raw shape: {cdl_raw.shape}, hex range: {cdl_raw['hex'].min():.2e} to {cdl_raw['hex'].max():.2e}")
    
    # Filter CDL to only hexes that overlap with ERA5 bounds
    elevation_hex = set(elevation_df['hex'].astype(str))
    cdl_filtered = cdl_raw[cdl_raw['hex'].astype(str).isin(elevation_hex)]
    
    print(f"CDL filtered to overlapping region: {cdl_filtered.shape}")
    
    if len(cdl_filtered) == 0:
        print("⚠️  No CDL data in this region.")
        return pd.DataFrame()
    
    # Initialize DuckDB with H3 extension
    con = duckdb.connect()
    con.execute("INSTALL h3 FROM community;")
    con.execute("LOAD h3;")
    
    # Register the DataFrame so DuckDB can query it
    con.register('cdl_filtered', cdl_filtered)
    
    # Aggregate CDL using k-rings to get top crop per hex based on spatial context
    cdl_aggregated = con.execute(f"""
        WITH k_ring_expanded AS (
            SELECT 
                hex as center_hex,
                unnest(h3_grid_disk(hex, {k_ring_size})) as ring_hex
            FROM cdl_filtered
        ),
        k_ring_data AS (
            SELECT 
                k.center_hex,
                c.crop_type as crop_type,
                c.area as crop_area
            FROM k_ring_expanded k
            JOIN cdl_filtered c ON CAST(c.hex AS BIGINT) = CAST(k.ring_hex AS BIGINT)
        ),
        aggregated_crops AS (
            SELECT 
                center_hex,
                crop_type,
                SUM(crop_area) as total_area
            FROM k_ring_data
            GROUP BY center_hex, crop_type
        ),
        ranked_crops AS (
            SELECT 
                center_hex,
                crop_type,
                total_area,
                ROW_NUMBER() OVER (PARTITION BY center_hex ORDER BY total_area DESC) as rank
            FROM aggregated_crops
        )
        SELECT 
            center_hex as hex,
            crop_type as crop_rank_1,
            total_area as area_rank_1
        FROM ranked_crops
        WHERE rank = 1
    """).df()
    
    print(f"CDL aggregated with k-ring={k_ring_size} to top crop per hex: {cdl_aggregated.shape}")
    print(f"Sample:\n{cdl_aggregated.head(5)}")
    
    # Join elevation with aggregated CDL crop data
    merged = elevation_df.merge(
        cdl_aggregated,
        on='hex',
        how='left'
    ).rename(columns={'mean_value': 'elevation'})
    
    # Round elevation to 1 decimal place
    merged['elevation'] = merged['elevation'].round(1)
    
    # Join with ERA5 temperature data
    merged = merged.merge(
        era5_temp,
        on='hex',
        how='left'
    )
    
    print(f"After joining ERA5 temp: {merged.shape}")
    
    # Apply elevation filters if specified
    if min_elevation is not None:
        merged = merged[merged['elevation'] >= min_elevation]
    if max_elevation is not None:
        merged = merged[merged['elevation'] <= max_elevation]
    
    # Add lat/lng columns for hex centers
    con.register('merged', merged)
    merged = con.execute("""
        SELECT 
            *,
            h3_cell_to_lat(hex) as lat,
            h3_cell_to_lng(hex) as lng
        FROM merged
    """).df()
    
    print(f"Final merged data: {merged.shape} rows")
    print(f"Sample:\n{merged.head(10)}")
    
    # Show crop distribution by elevation if data exists
    if len(merged[merged['crop_rank_1'].notna()]) > 0:
        crop_summary = con.execute("""
            SELECT 
                crop_rank_1 as dominant_crop,
                COUNT(*) as num_hexes,
                AVG(elevation) as avg_elevation,
                MIN(elevation) as min_elevation,
                MAX(elevation) as max_elevation,
                AVG(daily_mean) as avg_temp,
                AVG(area_rank_1) as avg_area
            FROM merged
            WHERE crop_rank_1 IS NOT NULL
            GROUP BY crop_rank_1
            ORDER BY num_hexes DESC
            LIMIT 10
        """).df()
        print(f"\nTop crops (rank 1) by hex count:\n{crop_summary}")
    
    con.close()
    return merged