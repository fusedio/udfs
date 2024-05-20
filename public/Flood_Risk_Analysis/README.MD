This User-Defined Function (UDF) operates on a given bounding box (`bbox`) and context to retrieve land use and land cover (LULC) data for a specified year, defaulting to 2022. If the zoom level of the bounding box is equal to or greater than 5, it imports necessary utilities such as `get_lulc` and `get_overture`, along with essential libraries like NumPy, rasterio, shapely, and geopandas. 

It then fetches LULC data for the given bounding box and year, transforms it into vector geometries, and creates a GeoDataFrame representing water bodies. If there are water geometries and a water buffer distance is specified, the function estimates the UTM CRS for the water GeoDataFrame, buffers the water bodies, and converts them back to the original CRS of the bounding box. 

Subsequently, it retrieves contextual data using the water GeoDataFrame through the `get_overture` function. Finally, it returns the resulting GeoDataFrame containing contextual data. If the zoom level of the bounding box is less than 5, it prints a message indicating that the user should zoom in further. 

This UDF is tailored to identify buildings that are within 300 feet of any water body. It's useful in identifying the buildings that are at risk of flooding. 
