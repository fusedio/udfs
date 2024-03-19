@fused.udf
def udf(bbox: fused.types.Bbox=None):
    import geopandas as gpd
    import shapely
    

    default_bbox = shapely.box(-122.549, 37.681, -122.341, 37.818)
    tile_bbox_geom = bbox if bbox is not None else default_bbox
    tile_bbox_gdf = gpd.GeoDataFrame({'geometry': [tile_bbox_geom]}, crs='EPSG:4326')
    print(tile_bbox_geom)

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    from utils import (
    read_tiff,
    )
    
    print(bbox)
    print(tile_bbox_gdf)
    
         

    input_tiff_path = f"s3://sunlightdatabucket/World_DNI_GISdata_LTAy_AvgDailyTotals_GlobalSolarAtlas-v2_GEOTIFF/DNI.tif"
    data = read_tiff(bbox=tile_bbox_gdf, input_tiff_path=input_tiff_path, output_shape=(256, 256))
    print(data)
    print(type(data))
    
    
    filled_data = data.filled(fill_value=0)



    return utils.arr_to_plasma(filled_data, min_max=(0, 5), reverse=False)
    

 