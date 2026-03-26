@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.5, 47.3, -121.7, 47.8],  # King County, WA
    year: int = 2024,
    month: int = 6,
    max_cloud_cover: int = 20,
    resolution: int = 500,
):
    import numpy as np
    map_utils = fused.load("https://github.com/fusedio/udfs/tree/6800334/community/milind/map_utils/")

    # ── Fetch NDVI array from the compute UDF ──────────────────────────
    ndvi_udf = fused.load("fetch_ndvi_monthly")
    ndvi_arr = ndvi_udf(
        bounds=bounds,
        year=year,
        month=month,
        max_cloud_cover=max_cloud_cover,
        resolution=resolution,
    )

    if ndvi_arr is None:
        return "<p style='color:red'>No scenes found — try raising max_cloud_cover or changing month/year.</p>"

    print(f"NDVI array shape: {ndvi_arr.shape}, min={np.nanmin(ndvi_arr):.3f}, max={np.nanmax(ndvi_arr):.3f}")

    # ── Colour map: NDVI -0.2 → 1.0 mapped to RGBA ────────────────────
    # Clamp to [-0.2, 1.0] then normalise to [0, 1]
    lo, hi = -0.2, 1.0
    norm = np.clip((ndvi_arr - lo) / (hi - lo), 0, 1)  # (H, W)

    # Green colour ramp: brown (low) → yellow → bright green (high)
    r = np.interp(norm, [0, 0.3, 0.6, 1.0], [139, 210, 120,  34]).astype(np.uint8)
    g = np.interp(norm, [0, 0.3, 0.6, 1.0], [ 90, 180, 200, 139]).astype(np.uint8)
    b = np.interp(norm, [0, 0.3, 0.6, 1.0], [ 43,  60,  60,  34]).astype(np.uint8)
    a = np.where(np.isnan(ndvi_arr), 0, 220).astype(np.uint8)

    rgba = np.stack([r, g, b, a], axis=-1)  # (H, W, 4)
    print(f"RGBA image shape: {rgba.shape}")

    return map_utils.deckgl_raster(
        image_data=rgba,
        bounds=list(bounds),
        basemap="light",
        config={
            "rasterLayer": {"opacity": 0.85, "pickable": True},
        },
    )