@fused.udf
def udf(
    bounds: fused.types.Bounds = None, 
    chip_len=256, # 256 is great for visualization
    visualize: bool = True, # Change this to False when using this UDF for analysis
):
    """Visualize a given CHM"""
    import numpy as np
    import palettable
    
    common = fused.load('https://github.com/fusedio/udfs/tree/36f4e97/public/common/').utils

    image_id = fused.run("UDF_Meta_CHM_tiles_geojson", bounds=bounds, use_centroid_method=False)
    path_of_chm = f"s3://dataforgood-fb-data/forests/v1/alsgedi_global_v6_float/chm/{image_id['tile'].iloc[0]}.tif"
    print(f"Using {path_of_chm=}")
    
    tile = common.get_tiles(bounds, target_num_tiles=4, clip=True)
    arr = common.read_tiff(tile, path_of_chm, output_shape=(chip_len, chip_len))    

    print(f"{arr.max()=}")

    if visualize:
        vis = common.visualize(
            data=arr,
            mask = None,
            min=0,
            max=50,
            colormap=palettable.scientific.sequential.Bamako_20_r,
        )
        print(f"{vis=}")
    
        # unpacking bands
        return vis[:3]
    else:
        return arr