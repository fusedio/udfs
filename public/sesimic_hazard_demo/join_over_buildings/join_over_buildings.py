@fused.udf
def udf(
    path: str = "s3://fused-asset/overture/2026-03-18.0/theme=buildings/type=building/part=2/36.parquet",
    res: int = 6,
    bounds: fused.types.Bounds = [6.6272, 36.6197, 18.4802, 47.0921],
    chip_len: int = 512,
    h3_res: int = 6,
):
    import pandas as pd

    # Load buildings hexagonified at res=6
    overture_udf = fused.load("Overture_to_hex_single_file")
    df_buildings = overture_udf(path=path, res=res)
    print("buildings shape:", df_buildings.shape)
    print(df_buildings.head(3))

    # Load seismic hazard hexagonified at h3_res=6
    seismic_udf = fused.load("hexagonified_seismic_hazard")
    df_seismic = seismic_udf(bounds=bounds, chip_len=chip_len, h3_res=h3_res)
    print("seismic shape:", df_seismic.shape)
    print(df_seismic.head(3))

    # Normalize hex column types — both to lowercase hex string (H3 canonical format)
    # Overture returns decimal integer string -> convert to hex string
    df_buildings["hex"] = df_buildings["hex"].apply(lambda x: format(int(x), 'x'))
    # Seismic returns uint64 -> convert to hex string
    df_seismic["hex"] = df_seismic["hex"].apply(lambda x: format(int(x), 'x'))

    # Inner join on hex
    merged = pd.merge(df_buildings, df_seismic, on="hex", how="inner")
    print("merged shape:", merged.shape)
    print(merged.dtypes)
    print(merged.head())
    return merged
