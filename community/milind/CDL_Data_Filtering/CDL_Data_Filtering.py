@fused.udf
def udf(
    h3_res: int = 5,            # desired output H3 resolution
    crop_value_list: list = [], # optional CDL values to keep; [] = keep all
    min_ratio: float = 0.0,     # drop rows with pct <= 100*min_ratio
    year: int = 2024,
):
    """
    Minimal CDL loader (Source Coop, S3 path).
    Returns: hex (H3 int), data (CDL value), area (m²), pct (0–100 int).
    """
    # DuckDB helper
    common = fused.load("https://github.com/fusedio/udfs/tree/6b98ee5/public/common/")
    con = common.duckdb_connect()

    # Choose backing parquet (only hex7 or hex8 are published)
    s3_resolution = 7 if h3_res <= 7 else 8
    if h3_res > 8:
        print("h3_res > 8 not supported; using hex8 dataset and no further parenting.")
        s3_resolution = 8
    path = f's3://us-west-2.opendata.source.coop/fused/hex/release_2025_04_beta/cdl/hex{s3_resolution}_{year}.parquet'

    # Parent to requested resolution if needed
    qr_hex = "hex" if s3_resolution == h3_res else f"h3_cell_to_parent(hex, {h3_res})"

    @fused.cache
    def get_hex_data(qr_hex: str, path: str):
        return con.sql(f'''
            SELECT
              {qr_hex} AS hex,
              value     AS data,
              SUM(area)::DOUBLE AS area,
              (100 * SUM(area) / SUM(SUM(area)) OVER (PARTITION BY {qr_hex}))::DOUBLE AS pct
            FROM read_parquet("{path}")
            WHERE value != 0
            GROUP BY 1, 2
        ''').df()

    df = get_hex_data(qr_hex, path)

    # Optional crop filter
    if crop_value_list:
        crop_values = [int(x) for x in crop_value_list]
        df = df[df["data"].isin(crop_values)]

    # Min ratio filter
    if min_ratio > 0:
        df = df[df["pct"] > 100 * min_ratio]

    # Final types & cols
    df["hex"] = df["hex"].astype("int64")
    df["pct"] = df["pct"].round().clip(0, 100).astype("int64")
    return df[["hex", "data", "area", "pct"]].reset_index(drop=True)
