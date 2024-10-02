@fused.udf
def udf(bbox):
    import wbt

    # out_tif_name = "mainstem"
    # wbt_args = {
    #     "BreachDepressions": ["-i=dem.tif", "--fill_pits", "-o=dem_corr.tif"],
    #     "D8Pointer": ["-i=dem_corr.tif", "-o=fdir.tif"],
    #     "D8FlowAccumulation": ["-i=fdir.tif", "--pntr", "-o=d8accum.tif"],
    #     "FindMainStem": ["--d8_pntr=fdir.tif", "--streams=d8accum.tif", "-o=mainstem.tif"],
    # }
    # min_max = None

    out_tif_name = "twi"
    wbt_args = {
        "BreachDepressions": ["-i=dem.tif", "--fill_pits", "-o=dem_corr.tif"],
        "D8FlowAccumulation": [
            "-i=dem_corr.tif",
            "--out_type='specific contributing area'",
            "-o=sca.tif",
        ],
        "Slope": ["-i=dem_corr.tif", "--units=degrees", "-o=slope.tif"],
        "WetnessIndex": ["--sca=sca.tif", "--slope=slope.tif", f"-o={out_tif_name}.tif"],
    }
    min_max = (0, 15)
    return wbt.run(bbox, wbt_args, out_tif_name, extra_input_files=None, min_max=min_max)
