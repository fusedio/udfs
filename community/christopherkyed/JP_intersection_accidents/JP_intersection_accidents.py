@fused.udf
def udf(
    bounds: fused.types.Bounds = [139.745, 35.670, 139.775, 35.695],
    min_zoom: int = 12,
    acc_year=2021
):
    import geopandas as gpd
    import duckdb
    import pandas as pd

    common = fused.load("https://github.com/fusedio/udfs/tree/3991434/public/common/")
    overture_udf = fused.load("https://github.com/fusedio/udfs/tree/a9d31ec/public/Overture_Maps_Example/")
    zoom = common.bounds_to_zoom(bounds)
    print(f"Current zoom: {zoom}, min_zoom: {min_zoom}")

    if zoom >= min_zoom:
        # --------------------------------------------------------------------
        # Load Overture data (roads/connector) – keep in EPSG:4326 (no reproj)
        # --------------------------------------------------------------------
        gdf = overture_udf(
            bounds=bounds,
            release="2025-05-21-0",
            overture_type='connector',
            use_columns=['id'],
            min_zoom=min_zoom,
        )

        if gdf is None or len(gdf) == 0:
            print("No Overture connector data found in this area")
            return pd.DataFrame()

        # Extract lat/lon directly from point geometry (already in 4326)
        gdf["lat"] = gdf.geometry.y
        gdf["lon"] = gdf.geometry.x
        
        # Keep only necessary columns for DuckDB
        int_df = gdf[["id", "lat", "lon"]].copy()

        # --------------------------------------------------------------------
        # Load accidents (HTML UDF) – assumed to contain latitude/longitude cols
        # --------------------------------------------------------------------
        acc_udf = fused.load('https://github.com/fusedio/udfs/tree/1762605/community/christopherkyed/html_accident_h3_tile/')
        df_acc = acc_udf(bounds=bounds, year=acc_year)
        
        # --------------------------------------------------------------------
        # DuckDB setup
        # --------------------------------------------------------------------
        con = duckdb.connect()
        con.sql("INSTALL spatial; LOAD spatial;")
        con.sql("INSTALL h3 FROM community; LOAD h3;")

        # Register tables
        con.register("intersections", int_df)    # columns: id, lat, lon
        con.register("acc", df_acc)        # expect columns: latitude, longitude (or lat, lon)

        # --------------------------------------------------------------------
        # Build points with H3 hex and its 2-ring neighbours, then join with accidents
        # --------------------------------------------------------------------
        sql = """
            WITH points AS (
                SELECT
                    id,
                    h3_latlng_to_cell(lat, lon, 12) AS hex,
                    h3_grid_disk(h3_latlng_to_cell(lat, lon, 12), 2) AS hex_neighbour
                FROM intersections
            ),
            accidents AS (
                SELECT
                prefecture_name,
                year,
                month,
                day,
                hour,
                age_A,
                age_B,
                injuries,
                hex
            FROM acc
            )
            SELECT
                p.id,
                p.hex,
                MODE(prefecture_name) as prefecture_name,
                COUNT(a.*) as acc_cnt,
                (AVG(a.age_A) + AVG(a.age_B))/2 AS avg_age,
                SUM(a.injuries) as sum_injuries
            FROM points AS p
            INNER JOIN accidents AS a
            ON a.hex = ANY(p.hex_neighbour)
            GROUP BY p.id, p.hex
            ORDER BY p.id;
        """
        df_joined = con.sql(sql).df()
        print(df_joined)

        df_joined = df_joined[df_joined['acc_cnt'] > 0]

        return df_joined
