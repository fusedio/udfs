@fused.udf
def udf(
    num_reviews_min: float = 0,
    num_reviews_max: float = 9999,
    avg_rating_min: float = 0.0,
    avg_rating_max: float = 5.0,
    sentiment: str = "All",           # "All", "Positive", "Negative"
    nps_min: float = -100.0,
    nps_max: float = 100.0,
    store_type: str = "All",          # "All", "Distribution Hub", "Logistics Depot", "Processing Center", "Regional Office", "Retail Outlet", "Warehouse"
    staff_headcount_min: float = 0,
    staff_headcount_max: float = 9999,
):
    """
    Joins three supplier data sources into a single GeoDataFrame and applies
    optional filters to return a subset of store records.

    Sources joined
    --------------
    - all_suppliers         : Core supplier records from Snowflake TPCH_SF1
                              (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,
                               S_PHONE, S_ACCTBAL, S_COMMENT)
    - suppliers_feedback    : Customer feedback aggregated per supplier
                              (avg_rating, num_reviews, sentiment,
                               primary_complaint, net_promoter_score,
                               last_audit_date, audit_result,
                               mystery_shopper_pct, on_time_delivery_pct,
                               defect_rate_ppm)
    - supplier_locations    : Physical store/facility metadata with geometry
                              (store_type, latitude, longitude, country_code,
                               sq_meters, staff_headcount, certification,
                               date_established, facility_manager,
                               annual_capacity_units)

    Supplier IDs are normalised to a plain integer before joining, so
    prefixed formats like "S00000001", "SUPP-3", "SUP0000001" all resolve
    correctly. The final result is a GeoDataFrame in EPSG:4326.

    Parameters
    ----------
    num_reviews_min : float, default 0
        Minimum number of customer reviews a store must have.
    num_reviews_max : float, default 9999
        Maximum number of customer reviews a store may have.
    avg_rating_min : float, default 0.0
        Minimum average customer rating (0–5 scale).
    avg_rating_max : float, default 5.0
        Maximum average customer rating (0–5 scale).
    sentiment : str, default "All"
        Filter by overall review sentiment. Options: "All", "Positive",
        "Negative". Case-insensitive match.
    nps_min : float, default -100.0
        Minimum Net Promoter Score (-100 to 100).
    nps_max : float, default 100.0
        Maximum Net Promoter Score (-100 to 100).
    store_type : str, default "All"
        Filter by facility type. Options: "All", "Distribution Hub",
        "Logistics Depot", "Processing Center", "Regional Office",
        "Retail Outlet", "Warehouse".
    staff_headcount_min : float, default 0
        Minimum number of staff employed at the store.
    staff_headcount_max : float, default 9999
        Maximum number of staff employed at the store.

    Returns
    -------
    geopandas.GeoDataFrame
        Filtered store records with all 32 columns from the three sources
        plus a Point geometry column (EPSG:4326).
    """
    import pandas as pd
    import re

    # ── Load the three source UDFs ──────────────────────────────────────────
    suppliers_udf   = fused.load("all_suppliers")
    feedback_udf    = fused.load("suppliers_feedback_gdrive")
    locations_udf   = fused.load("supplier_locations_csv")

    df_sup  = suppliers_udf()
    df_fb   = feedback_udf()
    df_loc  = locations_udf()

    print("all_suppliers shape:", df_sup.shape)
    print("suppliers_feedback shape:", df_fb.shape)
    print("supplier_locations shape:", df_loc.shape)

    # ── Normalize supplier IDs to a plain integer join key ──────────────────
    # all_suppliers     → S_SUPPKEY is already an integer
    # feedback          → supplier_id: "S00000001", "SUPP-3", etc.
    # locations         → supplier_ref: "SUP0000001", etc.

    def extract_int(s):
        """Strip any non-digit prefix/suffix and return int (NaN if none found)."""
        m = re.search(r"\d+", str(s))
        return int(m.group()) if m else None

    df_sup["_join_key"] = df_sup["S_SUPPKEY"].astype(int)
    df_fb["_join_key"]  = df_fb["supplier_id"].apply(extract_int)
    df_loc["_join_key"] = df_loc["supplier_ref"].apply(extract_int)

    print("Sample join keys — suppliers:", df_sup["_join_key"].head(3).tolist())
    print("Sample join keys — feedback: ", df_fb["_join_key"].head(3).tolist())
    print("Sample join keys — locations:", df_loc["_join_key"].head(3).tolist())

    # ── Join: suppliers ← feedback ← locations ─────────────────────────────
    # Suffix feedback columns to avoid clashes
    df_merged = (
        df_sup
        .merge(df_fb,  on="_join_key", how="left", suffixes=("", "_fb"))
        .merge(df_loc, on="_join_key", how="left", suffixes=("", "_loc"))
    )

    df_merged = df_merged.drop(columns=["_join_key", "S_ADDRESS"])

    # ── Re-wrap as GeoDataFrame so geometry serializes correctly ────────────
    import geopandas as gpd
    gdf = gpd.GeoDataFrame(df_merged, geometry="geometry", crs="EPSG:4326")

    print("Merged shape:", gdf.shape)
    print(gdf.dtypes)
    print(gdf.head(3))

    # ── Apply filters ───────────────────────────────────────────────────────
    mask = (
        (gdf["num_reviews"].fillna(0) >= num_reviews_min) &
        (gdf["num_reviews"].fillna(0) <= num_reviews_max) &
        (gdf["avg_rating"].fillna(0) >= avg_rating_min) &
        (gdf["avg_rating"].fillna(0) <= avg_rating_max) &
        (gdf["net_promoter_score"].fillna(0) >= nps_min) &
        (gdf["net_promoter_score"].fillna(0) <= nps_max) &
        (gdf["staff_headcount"].fillna(0) >= staff_headcount_min) &
        (gdf["staff_headcount"].fillna(0) <= staff_headcount_max)
    )
    if sentiment != "All":
        mask &= gdf["sentiment"].str.lower() == sentiment.lower()
    if store_type != "All":
        mask &= gdf["store_type"] == store_type

    gdf = gdf[mask]
    print(f"After filters: {len(gdf)} rows")

    return gdf
