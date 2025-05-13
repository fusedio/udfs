@fused.udf
def udf(bbox, context, year="2022", water_buffer:float = 100):
    if bbox.z[0] >= 5:
        from utils import (
            get_lulc,
            get_overture        
        )
        import numpy as np
        import rasterio
        import rasterio.features
        import shapely
        import geopandas as gpd
        
        data = get_lulc(bbox, year)
        
        transform = rasterio.transform.from_bounds(*bbox.total_bounds, 256, 256)
        shapes = rasterio.features.shapes(data, data == 1, transform=transform)
        geoms = []
        for shape, shape_value in shapes:
            shape_geom = shapely.geometry.shape(shape)
            # print(shape_geom)
            geoms.append(shape_geom)  
            
        water_gdf = gpd.GeoDataFrame({}, geometry=[shapely.geometry.MultiPolygon(geoms)],
                                     crs=bbox.crs)
        
        if len(geoms) and water_buffer:
            water_utm = water_gdf.estimate_utm_crs()
            print(water_utm)
            water_gdf = water_gdf.to_crs(water_utm)
            water_gdf.geometry = water_gdf.geometry.buffer(water_buffer)
            water_gdf = water_gdf.to_crs(bbox.crs)
            
                                
        df = get_overture(water_gdf)
        
        #return water_gdf
        return df
        print(data)

        # data = arr_to_color(data, color_map=LULC_IO_COLORS)
        
        return data * 10
    else:
        print("Please zoom in more.")
