@fused.udf
def udf(bounds: fused.types.Bounds=[6.6272, 36.6197, 18.4802, 47.0921], chip_len: int=512, h3_res: int=6):
    common = fused.load('https://github.com/fusedio/udfs/tree/5610abb/public/common/')
    parent_udf = fused.load('loading_seimic_data')
    arr = parent_udf(bounds=bounds, chip_len=chip_len)
    print("arr shape:", arr.shape, "dtype:", arr.dtype)

    # Squeeze to 2D if arr has a leading band dimension (e.g. (1, H, W) -> (H, W))
    import numpy as np
    if arr.ndim == 3:
        arr = arr[0]

    # Convert raster array to H3 hexagons
    df = common.arr_to_h3(arr, bounds, res=h3_res)

    # Flatten agg_data lists to mean scalar value
    import numpy as np
    df["sesimic_hazard"] = df["agg_data"].apply(lambda x: float(np.mean(x)))
    df = df[["hex", "sesimic_hazard"]]
    print(df.head())
    print(df.dtypes, df.shape)
    return df