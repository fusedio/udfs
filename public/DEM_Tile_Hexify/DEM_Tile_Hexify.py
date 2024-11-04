@fused.udf
def udf(bbox: fused.types.TileGDF, stats_type="mean"):
    import pandas as pd
    import rioxarray
    from utils import aggregate_df_hex

    # 1. Initial parameters
    x, y, z = bbox.iloc[0][["x", "y", "z"]]
    url = f"https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{z}/{x}/{y}.tif"
    res_offset = 0  # lower makes the hex finer
    h3_size = max(min(int(3 + bbox.z[0] / 1.5), 12) - res_offset, 2)
    print(h3_size)

    # 2. Read tiff
    da_tiff = rioxarray.open_rasterio(url).squeeze(drop=True).rio.reproject("EPSG:4326")
    df_tiff = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]

    # 3. Hexagonify & aggregate
    df = aggregate_df_hex(
        df_tiff, h3_size, latlng_cols=["y", "x"], stats_type=stats_type
    )
    df["elev_scale"] = int((15 - z) * 1)
    df = df[df["metric"] > 0]
    return df
