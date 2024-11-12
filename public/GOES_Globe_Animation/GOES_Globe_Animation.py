@fused.udf  
def udf(
    n_frames: int = 144,
    datestr: str = "2024-10-01", 
    coarsen_factor: int = 10,  
    bounds: str = "-180,-10,-65,70",
    product_name: str = "ABI-L2-CMIPF",
    bucket_name: str = "noaa-goes18"
):  
    @fused.cache
    def run(n_frames= n_frames,
        datestr=datestr,
        coarsen_factor=coarsen_factor,
        bounds=bounds,
        product_name=product_name,
        bucket_name=bucket_name
    ): 
        import geopandas as gpd
        import shapely
        import json
        import pandas as pd
        import io
        import imageio
        import numpy as np

        utils = fused.load("https://github.com/fusedio/udfs/tree/7306c98/public/common/").utils  
    
        bounds = [float(i) for i in bounds.split(",")]

        @fused.cache
        def get_frame(i, datestr=datestr, coarsen_factor=coarsen_factor, bucket_name=bucket_name):
            return fused.run(udf_nail, i=i, datestr=datestr,coarsen_factor=coarsen_factor, bucket_name=bucket_name).image.values  
        runner = utils.PoolRunner(get_frame, range(n_frames))
        runner.get_result_all()
        out=runner.result
        dataframes = []
        for out0 in out:
            arr = np.stack([arr.squeeze().astype("uint8") for arr in [out0]])
            output = io.BytesIO()
            writer = imageio.get_writer(output, format="jpg", fps=60)
            for frame in arr:
                writer.append_data(frame)
            writer.close()
            df = pd.DataFrame({"Binary Data": [json.dumps([i for i in output.getvalue()])]})
            df["n_frame"] = len(arr)
            gdf = gpd.GeoDataFrame(df, geometry=[shapely.box(*bounds)], crs=4326)
            dataframes.append(gdf)
        gdf2 = pd.concat(dataframes)
        return gdf2
    r = run(
        n_frames=n_frames,
        datestr=datestr,
        coarsen_factor=coarsen_factor,
        bounds=bounds,
        product_name=product_name,
        bucket_name=bucket_name)
    return r

@fused.udf   
def udf_nail(i: int = 0, datestr:str ='2024-06-19',coarsen_factor:int=20, bounds:str ='-180,-10,-65,70' , product_name:str ='ABI-L2-CMIPF', bucket_name:str ='noaa-goes18', offset:str ='0', band: int=8, x_res: int=40, y_res: int=40): 
    utils = fused.load("https://github.com/fusedio/udfs/tree/7306c98/public/common/").utils  
    import geopandas as gpd 
    import shapely
    import json
    bounds=[float(i) for i in bounds.split(',')]
    vars={'ABI-L2-CMIPF':'CMI', 'ABI-L1b-RadF':'Rad',    'ABI-L2-RRQPEF':'RRQPE'}
    min_maxs={'ABI-L2-CMIPF':(1500,2800), 'ABI-L1b-RadF':(128,2000), 'ABI-L2-RRQPEF':(0,3000)}
    var=vars[product_name]
    min_max=min_maxs[product_name]     
    bbox=gpd.GeoDataFrame(geometry=[shapely.box(*bounds)], crs=4326).to_crs(3857)
    # @fused.cache
    def fn(bbox, i, product_name, var, datestr, offset, band, x_res, y_res,coarsen_factor):
        from utils import file_of_day
        import rioxarray
        file_path = file_of_day(i, bucket_name, product_name, band, datestr,offset) 
        filename = file_path.split('/')[-1]
        url = f'https://{bucket_name}.s3.amazonaws.com/{file_path}'
        path = fused.core.download(url,'geos18/'+filename)
        xds = rioxarray.open_rasterio(path)
        ds = xds_roi = (xds.coarsen(x=coarsen_factor, y=coarsen_factor, boundary='trim').max())
        return ds[var]  
    da = fn(bbox, i, product_name, var, datestr=datestr, offset=offset, band=band, x_res=x_res, y_res=y_res, coarsen_factor=coarsen_factor)
    arr = utils.arr_to_plasma(da.values.squeeze(), min_max=min_max, colormap='')
    return arr
    