@fused.udf
def udf(ip: str = "115.117.126.142"):
    import duckdb, pandas as pd
    
    # Load your common helper
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    con = common.duckdb_connect(verbose=True)
    from shapely.geometry import Point
    import geopandas as gpd
    # Install/load extensions
    con.sql("INSTALL httpfs FROM core; LOAD httpfs;")
    
    print("DuckDB version:", duckdb.__version__)
    
    # Paths for your MaxMind Parquets
    blocks_v4 = "s3://fused-asset/ipinfo_maxmind/GeoLite2-City-Blocks-IPv4.parquet"
    blocks_v6 = "s3://fused-asset/ipinfo_maxmind/GeoLite2-City-Blocks-IPv6.parquet"
    locs      = "s3://fused-asset/ipinfo_maxmind/GeoLite2-City-Locations-en.parquet"
    
    # Create views
    con.sql(f"""
        CREATE OR REPLACE VIEW blocks4 AS
        SELECT network, geoname_id, latitude, longitude, accuracy_radius
        FROM read_parquet('{blocks_v4}');
    """)
    con.sql(f"""
        CREATE OR REPLACE VIEW blocks6 AS
        SELECT network, geoname_id, latitude, longitude, accuracy_radius
        FROM read_parquet('{blocks_v6}');
    """)
    con.sql(f"""
        CREATE OR REPLACE VIEW loc AS
        SELECT *
        FROM read_parquet('{locs}');
    """)
    
    # Pick family automatically
    is_ipv6 = ":" in ip
    
    if is_ipv6:
        # For IPv6, use Python's ipaddress module
        import ipaddress
        ip_obj = ipaddress.ip_address(ip)
        
        # Get all IPv6 blocks and filter in Python
        blocks_df = con.execute(f"""
            SELECT 
                b.network,
                b.geoname_id,
                b.latitude,
                b.longitude,
                b.accuracy_radius,
                l.country_iso_code,
                l.country_name,
                l.subdivision_1_name,
                l.city_name
            FROM blocks6 b
            JOIN loc l USING(geoname_id)
        """).df()
        
        # Find matching network with longest prefix
        best_match = None
        max_prefix = -1
        
        for idx, row in blocks_df.iterrows():
            try:
                network = ipaddress.ip_network(row['network'])
                if ip_obj in network and network.prefixlen > max_prefix:
                    best_match = row
                    max_prefix = network.prefixlen
            except:
                continue
        
        if best_match is not None:
            df = pd.DataFrame([{
                'country_iso_code': best_match['country_iso_code'],
                'country_name': best_match['country_name'],
                'subdivision_1_name': best_match['subdivision_1_name'],
                'city_name': best_match['city_name'],
                'latitude': best_match['latitude'],
                'longitude': best_match['longitude'],
                'accuracy_radius': best_match['accuracy_radius'],
                'matched_cidr': best_match['network'],
                'prefix_len': max_prefix
            }])
        else:
            df = pd.DataFrame()
    else:
        # IPv4 logic (existing code)
        query = f"""
            WITH ip_parsed AS (
                SELECT 
                    CAST(split_part(?, '.', 1) AS BIGINT) * 16777216 +
                    CAST(split_part(?, '.', 2) AS BIGINT) * 65536 +
                    CAST(split_part(?, '.', 3) AS BIGINT) * 256 +
                    CAST(split_part(?, '.', 4) AS BIGINT) AS ip_int
            ),
            blocks_parsed AS (
                SELECT 
                    b.*,
                    l.country_iso_code, l.country_name,
                    l.subdivision_1_name, l.city_name,
                    CAST(split_part(split_part(b.network, '/', 1), '.', 1) AS BIGINT) * 16777216 +
                    CAST(split_part(split_part(b.network, '/', 1), '.', 2) AS BIGINT) * 65536 +
                    CAST(split_part(split_part(b.network, '/', 1), '.', 3) AS BIGINT) * 256 +
                    CAST(split_part(split_part(b.network, '/', 1), '.', 4) AS BIGINT) AS net_start,
                    CAST(split_part(b.network, '/', 2) AS INTEGER) AS prefix_len
                FROM blocks4 b
                JOIN loc l USING(geoname_id)
            )
            SELECT
                bp.country_iso_code, bp.country_name,
                bp.subdivision_1_name, bp.city_name,
                bp.latitude, bp.longitude, bp.accuracy_radius,
                bp.network AS matched_cidr,
                bp.prefix_len
            FROM blocks_parsed bp, ip_parsed ip
            WHERE (ip.ip_int >> (32 - bp.prefix_len)) = (bp.net_start >> (32 - bp.prefix_len))
            ORDER BY bp.prefix_len DESC
            LIMIT 1;
        """
        
        df = con.execute(query, [ip, ip, ip, ip]).df()
    
    if not df.empty and 'latitude' in df.columns and 'longitude' in df.columns:
        geometry = [Point(lon, lat) for lon, lat in zip(df['longitude'], df['latitude'])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        print(gdf.T)
        return gdf
    else:
        # Return empty GeoDataFrame if no match found
        gdf = gpd.GeoDataFrame(columns=['ip', 'country_iso_code', 'country_name', 
                                         'subdivision_1_name', 'city_name', 
                                         'latitude', 'longitude', 'accuracy_radius',
                                         'matched_cidr', 'prefix_len', 'geometry'],
                               crs="EPSG:4326")
        print("No match found for IP:", ip)
        return gdf