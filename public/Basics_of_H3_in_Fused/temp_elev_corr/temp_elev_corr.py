@fused.udf
def udf(
    # whole US
    bounds: fused.types.Bounds = [-124.784, 24.396, -66.885, 49.384],
    res: int = 4,
    month: str = '2024-05',
):
    import pandas as pd

    common = fused.load("https://github.com/fusedio/udfs/tree/f661b6d/public/common/")

    # Load both UDFs
    elev_udf = fused.load("copdem_elevation")
    temp_udf = fused.load("era5_monthly_mean")

    # Call both with the same bounds
    df_elev = elev_udf(bounds=bounds, res=res)
    df_temp = temp_udf(month=month, bounds=bounds)

    print("Elev columns:", df_elev.columns.tolist())
    print("Temp columns:", df_temp.columns.tolist())
    print(f"Elev rows: {len(df_elev)}, Temp rows: {len(df_temp)}")

    # Aggregate temp to res 4 then join on hex
    con = common.duckdb_connect()
    qr = f"""
        WITH temp_agg AS (
            SELECT
                h3_cell_to_parent(hex, {res}) AS hex,
                avg(monthly_mean_temp) AS monthly_mean_temp
            FROM df_temp
            GROUP BY 1
        )
        SELECT
            e.hex,
            e.data_avg AS elevation_avg,
            t.monthly_mean_temp
        FROM df_elev e
        JOIN temp_agg t ON e.hex = t.hex
    """
    df_joined = con.query(qr).df()

    print(f"Joined rows: {len(df_joined)}")
    print(df_joined.T)

    return df_joined
