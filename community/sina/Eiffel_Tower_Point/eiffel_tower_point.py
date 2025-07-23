@fused.udf
def udf():
    import geopandas as gpd
    from shapely.geometry import Point
    
    return gpd.GeoDataFrame(
        {"poi": ["top_of_Eiffel_Tower"]}, 
        geometry=[Point(2.2945008587019395, 48.85833092066099)]
    )
