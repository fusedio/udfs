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
def udf(bounds: fused.types.Bounds = [-77.595,38.250,-77.383,38.520], out_tif_name: str ='output', wbt_args: dict = wbt_args, min_max=min_max):
    # convert bounds to tile
    import wbt
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)
    wbt_args = json.loads(wbt_args) if isinstance(wbt_args, str) else wbt_args
    return wbt.run(tile, wbt_args, out_tif_name, extra_input_files=None, min_max=min_max)

def run(bounds, wbt_args: dict[str, list[str]], out_tif_name: str, extra_input_files: list[str] | None = None, min_max: tuple[float, float] | None = None):
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    import tempfile
    import rioxarray
    import numpy as np
    import pywbt
    import shutil

    extra_input_files = [] if extra_input_files is None else extra_input_files
    url = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1/TIFF/USGS_Seamless_DEM_1.vrt'
    dem = rioxarray.open_rasterio(url).squeeze(drop=True)
    total_bounds = bounds.to_crs(5070).buffer(1000).to_crs(4326).total_bounds
    dem = dem.rio.clip_box(*total_bounds)
    dem = dem.where(dem > dem.rio.nodata, drop=False).rio.write_nodata(np.nan)

    crs_proj = 5070
    bbox_proj = bounds.to_crs(crs_proj).total_bounds
    dem = dem.rio.reproject(crs_proj).rio.clip_box(*bbox_proj)
    
    with tempfile.TemporaryDirectory() as tmp:
        dem.rio.to_raster(f"{tmp}/dem.tif")
        for f in extra_input_files:
            shutil.copy(f, tmp)
        pywbt.whitebox_tools(tmp, wbt_args, save_dir=tmp, wbt_root="WBT", zip_path="wbt_binaries.zip")
        ds = rioxarray.open_rasterio(f"{tmp}/{out_tif_name}.tif").squeeze(drop=True)
        ds = ds.where(ds > ds.rio.nodata)
        arr = ds.rio.reproject(bounds.crs).rio.clip_box(*bounds.total_bounds).to_numpy()
    return common.arr_to_plasma(arr, min_max=min_max, reverse=False)

