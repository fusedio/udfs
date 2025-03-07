# a. mainstem
wbt_args = {
    "BreachDepressions": ["-i=dem.tif", "--fill_pits", "-o=dem_corr.tif"],
    "D8Pointer": ["-i=dem_corr.tif", "-o=fdir.tif"],
    "D8FlowAccumulation": ["-i=fdir.tif", "--pntr", "-o=d8accum.tif"],
    "FindMainStem": ["--d8_pntr=fdir.tif", "--streams=d8accum.tif", "-o=output.tif"],
}
min_max = None

# b. TWI
wbt_args = {
    "BreachDepressions": ["-i=dem.tif", "--fill_pits", "-o=dem_corr.tif"],
    "D8FlowAccumulation": ["-i=dem_corr.tif","--out_type='specific contributing area'","-o=sca.tif",],
    "Slope": ["-i=dem_corr.tif", "--units=degrees", "-o=slope.tif"],
    "WetnessIndex": ["--sca=sca.tif", "--slope=slope.tif", f"-o=output.tif"],
}
min_max = (0, 15)

@fused.udf
def udf(bounds: fused.types.TileGDF, out_tif_name: str ='output', wbt_args: dict = wbt_args, min_max=min_max):
    import wbt
    wbt_args = json.loads(wbt_args) if isinstance(wbt_args, str) else wbt_args
    return wbt.run(bounds, wbt_args, out_tif_name, extra_input_files=None, min_max=min_max)
