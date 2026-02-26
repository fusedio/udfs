@fused.udf
def udf(
    bounds: fused.types.Bounds = [-121.525, 37.70, -120.96, 38.06],
    path = "s3://fused-asset/misc/hex/soil_gssurgo_hex/",
    res_offset: int = 0, ## Lowering the offset makes the map load faster - set to 0 in canvas
    res = None):
    
    common = fused.load("https://github.com/fusedio/udfs/tree/36f4e97/public/common/").utils
    con = common.duckdb_connect()
    hex_reader = fused.load("https://github.com/fusedio/udfs/tree/8024b5c/community/joris/Read_H3_dataset/")

    df = hex_reader.read_h3_dataset(path, bounds, res=res)

    print(df)
    if "pct" not in df:
        df['pct'] = df.cnt/df.cnt_total
    df = df.sort_values("pct", ascending=False).drop_duplicates("hex")
    # print("Area (acre):", sum(df.area*0.000247105))
    df = add_taxsubgrp(df[df["data"] != 2147483647])

    return df 

    
    
def add_taxsubgrp(df0):
    df = df0.copy()
    kv_colors = mukey_to_taxsubgrp(return_color=True)
    kv_names = mukey_to_taxsubgrp(return_color=False)
    df["taxsubgrp"] = df["data"].apply(lambda k: kv_names.get(str(k), "NA2" if k != 2147483647 else "NA"))
    df["r"] = df["data"].apply(lambda k: kv_colors.get(str(k), (150, 150, 150) if k != 2147483647 else (200, 200, 200))[0])
    df["g"] = df["data"].apply(lambda k: kv_colors.get(str(k), (150, 150, 150) if k != 2147483647 else (200, 200, 200))[1])
    df["b"] = df["data"].apply(lambda k: kv_colors.get(str(k), (150, 150, 150) if k != 2147483647 else (200, 200, 200))[2])
    df = df[df["data"] != 2147483647]
    return df


@fused.cache
def mukey_to_taxsubgrp(return_color=False, cmap_name="tab20"):
    path = "s3://fused-asset/misc/gSSURGO/CONUS_Component.parquet"
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors

    df = pd.read_parquet(path, columns=["mukey", "taxsubgrp"])
    df["taxsubgrp"] = df["taxsubgrp"].fillna("Unknown")
    mapping = df.set_index("mukey")["taxsubgrp"].to_dict()

    if not return_color:
        return mapping

    # Assign distinct RGB colors to taxsubgrp values
    def assign_colors(text_list, cmap_name=cmap_name):
        cmap = plt.get_cmap(cmap_name)
        unique_text = list(dict.fromkeys(text_list))
        return {text: tuple(int(c * 255) for c in cmap(i % cmap.N)[:3]) for i, text in enumerate(unique_text)}

    color_map = assign_colors(mapping.values())
    return {mukey: color_map[taxsubgrp] for mukey, taxsubgrp in mapping.items()}