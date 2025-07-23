@fused.udf
def udf():
    """Point taken from the Foursquare POI dataset (more info at https://source.coop/repositories/fused/fsq-os-places/description)"""
    import geopandas as gpd
    from shapely.geometry import Point
    
    return gpd.GeoDataFrame(
        {"poi": ["top_of_Eiffel_Tower"]}, 
        geometry=[Point(2.2945008587019395, 48.85833092066099)]
    )