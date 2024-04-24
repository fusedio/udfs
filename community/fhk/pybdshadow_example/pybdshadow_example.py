import geopandas as gpd
@fused.udf
def udf(
    bbox: fused.types.Bbox=None,
    polygon: gpd.GeoDataFrame = None,
    day: str = '2024-04-24',
    height: float = None):
    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib import cm
    import pybdshadow
    import shapely

    if polygon is None:
        gdf = gpd.read_file("s3://fused-users/fused/empirestate_ny.geojson", driver="GeoJSON")
    else:
        gdf = polygon

    if height is None:
        if 'height' not in gdf.columns:
            gdf['height'] = 10.0
    else:
        gdf['height'] = height
            
    #define analysis area
    bounds = bbox.exterior.bounds

    #filter the buildings
    gdf['x'] = gdf.centroid.x
    gdf['y'] = gdf.centroid.y
    buildings_analysis = gdf[(gdf['x'] > bounds[0]) &
                          (gdf['x'] <  bounds[2]) &
                          (gdf['y'] >  bounds[1]) &
                          (gdf['y'] <  bounds[3])]

    buildings_analysis['building_id'] = buildings_analysis.index
    #calculate sunshine time on the ground (set the roof to False)
    sunshine = pybdshadow.cal_sunshine(buildings_analysis,
                                       day=day,
                                       roof=False,
                                       accuracy=1,
                                       precision=900)

    cmap = plt.get_cmap('plasma')
    # Normalize sunlight values to range [0, 1]
    norm = plt.Normalize(vmin=0, vmax=24)
    print(sunshine['Hour'])
    # Calculate RGB values for each sunlight value

    rgb_values = (cmap(norm(sunshine['Hour']))[:, :3] * 255).astype(int)  # Extract only RGB values and convert to int
    
    # Add RGB values as new columns
    sunshine['r'], sunshine['g'], sunshine['b'] = rgb_values[:, 0], rgb_values[:, 1], rgb_values[:, 2]

    return sunshine
    

    
