@fused.udf
def udf(
    bounds: fused.types.Bounds = [-127.54220803198237,10.667151173068717,-66.93703570835524,55.22298640160706],
    res: int = 5, # if left to None, hex_reader will determine resolution itself
    data_value: int = 1, # Corn
    year: int = 2024,
):
    import h3.api.basic_int as h3
    path = "s3://fused-asset/hex/cdls_v8/year=2024/"

    common = fused.load("https://github.com/fusedio/udfs/tree/6dd2c4e/public/common/")
    hex_reader = fused.load("https://github.com/fusedio/udfs/tree/f2b3909/community/joris/Read_H3_dataset/")
    df = hex_reader.read_h3_dataset(path, bounds, res=res, value = data_value)
    print(df.T)
    
    if 'pct' not in df.columns and df.shape[0] > 0:
        # Dynamically calculating hex pct if not present in data
        data_res = h3.get_resolution(df["hex"].iloc[0])
        print(f"{data_res=}")

        con = common.duckdb_connect()
        df = con.query(f"""
            SELECT 
                hex,
                SUM(area) as total_area,
                ANY_VALUE(data) as data,
                h3_get_hexagon_area_avg({data_res}, 'm^2') as hex_area,
                (SUM(area) / h3_get_hexagon_area_avg({data_res}, 'm^2')) * 100 as pct
            FROM df 
            WHERE data == {data_value}
            GROUP BY hex
        """).to_df()

    print(df.shape)
    if df.shape[0] > 0:
        return df
    else:
        return None