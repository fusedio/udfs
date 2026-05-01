@fused.udf
def udf(
    state_fips: str = "53",   # San Juan County, WA
    county_fips: str = "055",
    year: int = 2024,
    month: int = 12,
    max_cloud_cover: int = 60,
    resolution: int = 500,    # metres
):
    """
    Input:  US county (state_fips + county_fips) + month/year
    Output: single-row DataFrame with mean NDVI clipped to the exact county geometry

    Steps:
      1. Load county geometry from us_counties_from_api UDF
      2. Fetch Sentinel-2 NDVI composite for the county bbox
      3. Clip raster to exact county polygon mask
      4. Return mean NDVI across valid pixels inside the county
    """
    import numpy as np
    import geopandas as gpd
    import pandas as pd
    from rasterio.transform import from_bounds
    from rasterio.features import geometry_mask

    # ── 1. Load county geometry from us_counties_from_api ─────────────
    counties_udf = fused.load("us_counties_from_api")
    county = counties_udf(state_fips=state_fips, county_fips=county_fips)

    geom = county.geometry.iloc[0]
    bounds = county.total_bounds  # [minx, miny, maxx, maxy]
    print(f"County bounds: {bounds}")
    print(f"County geometry type: {geom.geom_type}")

    # ── 2. Fetch NDVI composite via fetch_ndvi_monthly UDF ──────────────
    fetch_ndvi = fused.load("fetch_ndvi_monthly")
    ndvi_composite = fetch_ndvi(
        bounds=list(bounds),
        year=year,
        month=month,
        max_cloud_cover=max_cloud_cover,
        resolution=resolution,
    )

    if ndvi_composite is None:
        print("No scenes found — try raising max_cloud_cover or changing month/year.")
        return pd.DataFrame({"state_fips": [state_fips], "county_fips": [county_fips],
                             "year": [year], "month": [month],
                             "mean_ndvi": [None], "min_ndvi": [None], "max_ndvi": [None]})

    print(f"NDVI composite shape: {ndvi_composite.shape}")

    # ── 3. Clip to exact county polygon ───────────────────────────────
    h, w = ndvi_composite.shape
    transform = from_bounds(*bounds, width=w, height=h)

    # geometry_mask returns True OUTSIDE the geometry — invert it
    poly_mask = geometry_mask(
        [geom.__geo_interface__],
        transform=transform,
        invert=True,          # True = inside county
        out_shape=(h, w),
    )

    clipped = ndvi_composite.copy()
    clipped[~poly_mask] = np.nan  # mask out pixels outside county

    # ── 4. Return mean NDVI inside county ─────────────────────────────
    valid = clipped[~np.isnan(clipped)]
    mean_ndvi = float(np.mean(valid)) if len(valid) > 0 else None
    min_ndvi = float(np.min(valid)) if len(valid) > 0 else None
    max_ndvi = float(np.max(valid)) if len(valid) > 0 else None
    pct_valid = len(valid) / clipped.size * 100

    print(f"Pixels inside county: {len(valid)} / {clipped.size} ({pct_valid:.1f}%)")
    
    print(f"Mean NDVI: {mean_ndvi:.4f}, Min: {min_ndvi:.4f}, Max: {max_ndvi:.4f}")

    return pd.DataFrame({
        "state_fips":  [state_fips],
        "county_fips": [county_fips],
        "year":        [year],
        "month":       [month],
        "mean_ndvi":   [round(mean_ndvi, 4)],
        "min_ndvi":    [round(min_ndvi, 4)],
        "max_ndvi":    [round(max_ndvi, 4)],
        "valid_pixels":[len(valid)],
        "total_pixels":[clipped.size],
    })
