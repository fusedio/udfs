@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.5, 47.3, -121.7, 47.8],  # King County, WA
    year: int = 2024,
    month: int = 6,
    max_cloud_cover: int = 20,
    resolution: int = 500,  # metres
):
    """
    Monthly cloud-free NDVI composite for a given bounds using
    Sentinel-2 L2A from the Microsoft Planetary Computer.

    Returns a numpy array of mean NDVI (float32, values -1 to 1).
    """
    import numpy as np
    import odc.stac
    import planetary_computer
    import pystac_client

    # ── STAC search ────────────────────────────────────────────────────
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    date_range = f"{year}-{month:02d}-01/{year}-{month:02d}-28"

    @fused.cache
    def search_items(bounds, date_range, max_cloud_cover):
        
        items = catalog.search(
            collections=["sentinel-2-l2a"],
            bbox=bounds,
            datetime=date_range,
            query={"eo:cloud_cover": {"lt": max_cloud_cover}},
        ).item_collection()
        print(f"Found {len(items)} scenes for {date_range} (cloud < {max_cloud_cover}%)")
        return items

    items = search_items(tuple(bounds), date_range, max_cloud_cover)

    if len(items) == 0:
        print("No scenes found — try raising max_cloud_cover or changing month/year.")
        return None

    # ── Load & compute NDVI (cached to avoid repeated Planetary Computer requests)
    @fused.cache
    def load_ndvi_composite(bounds, date_range, max_cloud_cover, resolution):
        ds = odc.stac.load(
            items,
            bands=["B08", "B04"],
            crs="EPSG:4326",
            resolution=resolution / 111_320,  # degrees per pixel (approx)
            bbox=list(bounds),
            groupby="solar_day",
        )

        nir_raw = ds["B08"].astype("float32")
        red_raw = ds["B04"].astype("float32")

        nir_raw = nir_raw.where(nir_raw > 0)
        red_raw = red_raw.where(red_raw > 0)

        nir = nir_raw / 10_000.0
        red = red_raw / 10_000.0

        ndvi = (nir - red) / (nir + red + 1e-6)
        ndvi_composite = ndvi.median(dim="time")
        arr = ndvi_composite.values.astype("float32")
        return np.clip(arr, 0, 1)

    arr = load_ndvi_composite(tuple(bounds), date_range, max_cloud_cover, resolution)
    print(f"NDVI array shape: {arr.shape}")
    valid = arr[~np.isnan(arr)]
    print(f"Valid pixels: {len(valid)} / {arr.size}  min={valid.min():.3f}  max={valid.max():.3f}  mean={valid.mean():.3f}")

    return arr
