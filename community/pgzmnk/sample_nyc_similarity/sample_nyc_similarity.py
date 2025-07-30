@fused.udf
def udf(h3_res: int = 11, lat: float = 40.647395, lng: float = -73.788927):
    import duckdb
    import geopandas as gpd
    import h3
    import numpy as np
    import pandas as pd
    import shapely
    from shapely.geometry import Polygon

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    con = common.duckdb_connect()

    target_h3_cell = h3.latlng_to_cell(lat, lng, h3_res)
    print("target_h3_cell: ", target_h3_cell)

    @fused.cache
    def run(target_h3_cell=target_h3_cell):
        # Load hourly data from a public UDF
        df = fused.run("UDF_NYC_TLC_Hourly_2010", hourly=True, h3_res=h3_res)
        # reference: the hex that every other one will get compared to
        REFERENCE_HEX = df[df["hex"] == target_h3_cell]
        # i'm not sure why there are many values per hex per hour... did this include multiple days?
        # for now i will just combine them...
        REFERENCE_HEX_PICKUPS = REFERENCE_HEX.groupby(["hour"]).sum(["metric"])
        REFERENCE_HEX_PICKUPS = (
            REFERENCE_HEX_PICKUPS / REFERENCE_HEX_PICKUPS.metric.sum()
        )
        print("REFERENCE_HEX_PICKUPS", REFERENCE_HEX_PICKUPS.metric.sum())
        # Normalize
        dfg = df.groupby("hex").cnt.sum().to_frame("cnt_all").reset_index()
        df = df.merge(dfg)
        df["cnt"] = df["cnt"] / df["cnt_all"]
        all_hex_pickups = df  # .groupby(['hex', 'hour']).sum(['metric'])
        # df.groupby(['hex', 'hour']).sum(['metric']) # replaced

        # TODO: similarity metric
        compare = all_hex_pickups.join(
            REFERENCE_HEX_PICKUPS, on=["hour"], lsuffix="_REF", rsuffix=""
        )
        compare["ab"] = compare["metric"] * compare["metric_REF"]
        compare["a2"] = compare["metric"] * compare["metric"]
        compare["b2"] = compare["metric_REF"] * compare["metric_REF"]
        cos = compare.groupby(["hex"]).sum(["ab", "a2", "b2"])
        cos["sim"] = cos["ab"] / (
            cos["a2"].apply(np.sqrt) * cos["b2"].apply(np.sqrt)
        )  #
        gdf = cos.filter(items=["hex", "sim"]).sort_values(by="sim", ascending=True)
        return gdf.reset_index()

    gdf = run()

    # Introduce `geometry` column
    gdf = con.sql("""SELECT *, h3_cell_to_boundary_wkt(hex) geometry FROM gdf """).df()
    print(gdf.T)
    return gdf  # .sample(500_000)
