@fused.udf
def udf(
    bounds: fused.types.Bounds = [
        -125.82165127797666,21.313670812049978,-65.62955940309448,52.58604956417555
    ], # Default to global contentinal US (without Alaska)
    vals: list = [111], # 111 - Water bodies
):
    # Only contains 2024 for now
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    path = "s3://fused-asset/data/cdls/public_demo_2024/"

    hex_res = 5 # overwriting for visual
    df = read_hex_table(hex_res, bounds, path)
    df = df[df["data"].isin(vals)]

    return df

def bounds_to_res(bounds, res_offset=0, max_res=11, min_res=3):
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    z = common.estimate_zoom(bounds)
    return max(min(int(3 + z / 1.5 - res_offset), max_res), min_res)


def read_overview(hex_res, bounds, path):
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")
    con = common.duckdb_connect()
    df = con.sql(f'select * from read_parquet("{path}_hex{hex_res}")').df()
    df["pct"] = 255 * df["area"] / df["area"].max()
    df = common.filter_hex_bounds(df, bounds, col_hex="hex")
    return df


def read_hex_table(hex_res, bounds, path, base_res=7):
    import pandas as pd
    common = fused.load("https://github.com/fusedio/udfs/tree/b672adc/public/common/")

    if hex_res <= base_res:
        return read_overview(hex_res, bounds, path)

    con = common.duckdb_connect()
    bbox = bbox = common.to_gdf(bounds)
    df_meta = fused.get_chunks_metadata(path)
    df_meta = df_meta.sjoin(bbox)
    L = df_meta[["file_id", "chunk_id"]].values
    hex_bounds = common.bounds_to_hex(bounds, 7)

    def fn(x):
        df = fused.get_chunk_from_table(
            path, x[0], x[1], columns=["cnt", "cnt_total", "data", "pos7", "pos8", "pos9", "pos10", "pos11"]
        )
        df = df[df.pos7.isin(hex_bounds.hex)]
        return df

    df = common.run_pool(fn, L)
    df = pd.concat(df)
    H3_From_To_Pos = fused.load("https://github.com/fusedlabs/fusedudfs/tree/5738389/H3_From_To_Pos/")
    qr = H3_From_To_Pos.h3_from_pos_query("(select * from df)", columns="*", hex_res=hex_res, base_res=7)
    qr = f"""SELECT hex, data, (100*sum(cnt/cnt_total)/7^{11-hex_res})::FLOAT pct, (h3_cell_area(hex,'m^2')*pct/100) as area, (sum(cnt/cnt_total)*h3_cell_area(h3_cell_to_center_child(any_value(hex),11),'m^2'))::DOUBLE area2 FROM ({qr})  GROUP BY 1,2   """
    df = con.sql(qr).df()
    return df