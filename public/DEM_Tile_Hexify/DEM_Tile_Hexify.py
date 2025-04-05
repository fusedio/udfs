@fused.udf
def udf(bounds: fused.types.Bounds, stats_type="mean", type='hex', color_scale:float=1):
    import pandas as pd
    import rioxarray
    from utils import aggregate_df_hex, url_to_plasma

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    tile = common_utils.get_tiles(bounds)

    # 1. Initial parameters
    x, y, z = tile.iloc[0][["x", "y", "z"]]
    url = f"https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{z}/{x}/{y}.tif"
    if type=='png':
        return url_to_plasma(url, min_max=(-1000,2000/color_scale**0.5), colormap='plasma')
    else:
        
        res_offset = 0  # lower makes the hex finer
        h3_size = max(min(int(3 + zoom / 1.5), 12) - res_offset, 2)
        print(h3_size)
    
        # 2. Read tiff
        da_tiff = rioxarray.open_rasterio(url).squeeze(drop=True).rio.reproject("EPSG:4326")
        df_tiff = da_tiff.to_dataframe("data").reset_index()[["y", "x", "data"]]
    
        # 3. Hexagonify & aggregate
        df = aggregate_df_hex(
            df_tiff, h3_size, latlng_cols=["y", "x"], stats_type=stats_type
        )
        df["elev_scale"] = int((15 - z) * 1)
        df["metric"]=df["metric"]*color_scale
        df = df[df["metric"] > 0]
        return df
