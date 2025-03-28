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
def udf(bounds: fused.types.Bounds, out_tif_name: str ='output', wbt_args: dict = wbt_args, min_max=min_max):
    import wbt
    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)

    wbt_args = json.loads(wbt_args) if isinstance(wbt_args, str) else wbt_args
    return wbt.run(tile, wbt_args, out_tif_name, extra_input_files=None, min_max=min_max)
