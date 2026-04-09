@fused.udf
def udf(limit: int = 100):
    """
    Returns all suppliers from Snowflake's TPCH_SF1 sample dataset (~10,000 rows).
    """
    import snowflake.connector

    # This requires your own DEMO APP in Snowflake + your own credentials
    conn = snowflake.connector.connect(
        user="DEMO_APP_USER",
        password=fused.secrets["snowflake_demo_access_token"],
        account="DINFVZH-WOB67667",
        warehouse="COMPUTE_WH",
        database="SNOWFLAKE_SAMPLE_DATA",
        schema="TPCH_SF1",
    )

    cur = conn.cursor()
    cur.execute("SELECT * FROM SUPPLIER LIMIT %s;", (limit,))
    results = cur.fetch_pandas_all()
    print(f"Returned {len(results)} rows")
    cur.close()
    conn.close()
    return results
