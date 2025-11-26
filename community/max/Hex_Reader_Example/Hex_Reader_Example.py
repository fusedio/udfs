common = fused.load("https://github.com/fusedio/udfs/tree/b41216d/public/common/").utils
path = "s3://fused-users/fused/asset/CDL_h12k1p1/year=2024/"


@fused.udf
def udf(
    bounds: fused.types.Bounds = [-121.525, 37.70, -120.96, 38.06],
    crop_type: str = "grape",
    res_offset: int = 0,
    hex_res:int=None,
    path: str = path,
):
    CDL = fused.load("UDF_CDLs_Tile_Example")
    try:
        vals = [int(crop_type)]
    except:
        vals = CDL.crop_to_int(crop_type, verbose=False)
    # print(f'udf://Hex_Reader_Example?', 'reset') # reset does not work
    v = vals[0]
    for v in vals:
        print(f"udf://Hex_Reader_Example?crop_type={v}", CDL.int_to_crop(v))
    # vals=[1]
    if not hex_res:
        hex_res = bounds_to_res(bounds, res_offset=res_offset)
    df = read_hex_table(hex_res, bounds, path, res_offset=res_offset)
    df["pct"] = 255 * df["area"] / df["area"].max()  ###TODO:NEED TO FIX THIS

    print(f"{hex_res=}")
    top_crops = df.data.value_counts().head(60).to_frame("hex_cnt")
    top_crops["name"] = top_crops.index.map(CDL.int_to_crop)
    print(top_crops)
    CDL.int_to_crop(v)
    df = df[df["data"].isin(vals)]
    # print('udf://Hex_Reader_Example?val=2')
    # print('udf://Hex_Reader_Example?val=3')
    print(f"{df.shape=}")
    return df


def bounds_to_res(bounds, res_offset=1, max_res=11, min_res=3):
    z = common.estimate_zoom(bounds)
    return max(min(int(3 + z / 1.5 + res_offset), max_res), min_res)


def read_overview(hex_res, bounds, path):
    H3_From_To_Pos = fused.load("https://github.com/fusedlabs/fusedudfs/tree/5b022e0/H3_From_To_Pos/")
    df = H3_From_To_Pos.read_hexfile_bounds(bounds=bounds, url=f"{path}overview/hex{hex_res}.parquet", clip=1)
    # con = common.duckdb_connect()
    # df = con.sql(f'select * from read_parquet("{path}_hex{hex_res}")').df()
    # df["pct"] = 255 * df["area"] / df["area"].max()
    # df = common.filter_hex_bounds(df, bounds, col_hex="hex")
    return df


@fused.cache
def read_hex_table(hex_res, bounds, path, res_offset, base_res=7):
    import pandas as pd

    if hex_res <= base_res + 1:
        return read_overview(hex_res, bounds, path)
    else:
        con = common.duckdb_connect()
        bbox = bbox = common.to_gdf(bounds)
        df_meta = fused.get_chunks_metadata(path)
        df_meta = df_meta.sjoin(bbox)
        L = df_meta[["file_id", "chunk_id"]].values
        hex_bounds = common.bounds_to_hex(bounds, 7)
    
        def func(x):
            df = fused.get_chunk_from_table(
                path, x[0], x[1], columns=["cnt", "cnt_total", "data", "pos7", "pos8", "pos9", "pos10", "pos11"]
            )
            df = df[df.pos7.isin(hex_bounds.hex)]
            return df
    
        print(f"{len(L)=}")
        df = common.run_pool(func, L)
        df = pd.concat(df)
        H3_From_To_Pos = fused.load("https://github.com/fusedlabs/fusedudfs/tree/5738389/H3_From_To_Pos/")
        qr = H3_From_To_Pos.h3_from_pos_query("(select * from df)", columns="*", hex_res=hex_res, base_res=7)
        qr = f"""SELECT hex, data, (100*sum(cnt/cnt_total)/7^{11-hex_res})::FLOAT pct, (h3_cell_area(hex,'m^2')*pct/100) as area, (sum(cnt/cnt_total)*h3_cell_area(h3_cell_to_center_child(any_value(hex),11),'m^2'))::DOUBLE area2 FROM ({qr})  GROUP BY 1,2   """
        df = con.sql(qr).df()
    return df
