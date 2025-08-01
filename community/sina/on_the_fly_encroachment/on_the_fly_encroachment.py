@fused.udf
def udf(
    bounds: fused.types.Bounds = [-74.11643834276329,40.65577687101264,-73.92053315843329,40.74705016056631],
    h_bucket_meters: float = 5,
    w_bucket_meters: float = 5,
    lines_buffer_size_m: float = 150,
    return_risk_array: bool = False, # True gives raster output, False returns lines with score
):
    import numpy as np
    import rasterio.features
    import rasterio.transform
    from scipy.ndimage import distance_transform_edt
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")

    ingested_utilities_udf = fused.load('https://github.com/fusedio/udfs/tree/0858846/public/ingested_utilities/')
    power_lines = fused.run(ingested_utilities_udf, bounds=list(bounds), min_zoom=0, buffer_size_m=0)

    if power_lines.shape[0] == 0:
        return None
    buffered_lines = power_lines.copy()
    buffered_lines.geometry = power_lines.to_crs(power_lines.estimate_utm_crs()).geometry.buffer(lines_buffer_size_m).to_crs(4326)
    
    stream_chm_udf = fused.load('https://github.com/fusedio/udfs/tree/ba469a3/public/get_CHM_from_meta/')
    chm = fused.run(stream_chm_udf, bounds=bounds, visualize=False) # This uses 256 chip len all the time

    height, width = chm.shape
    xmin, ymin, xmax, ymax = bounds
    transform = rasterio.transform.from_bounds(xmin, ymin, xmax, ymax, width, height)

    # Rasterize power lines
    line_raster = rasterio.features.rasterize(
        power_lines.geometry, 
        out_shape=(height, width),
        transform=transform,
        fill=0, 
        default_value=100,
        all_touched=True
    )

    # Getting this to be able to get pole height - CHM
    pole_heights = rasterio.features.rasterize(
        ((geom, value) for geom, value in zip(buffered_lines.geometry, buffered_lines['pole_height_m'])),
        out_shape=(height, width), 
        transform=transform,
        all_touched=True
    )
    # Make shift pixel res calculation on the fly
    pixel_res = get_pixel_res_in_meters(bounds, transform)
    print(f"{pixel_res=}")
    
    # Distance transform (in pixels * pixel_res) then convert to meters and bin
    distance_pixels = distance_transform_edt(~line_raster.astype(bool)) * pixel_res
    distance_binned = (distance_pixels // w_bucket_meters + 1)
   
    poles_minus_chm = pole_heights - chm
    trees_taller_poles = np.where(poles_minus_chm < 0, -poles_minus_chm, 0)
    height_binned = (trees_taller_poles // h_bucket_meters)

    # Risk score
    # Divide by distance so further away -> less risky
    risk_score = height_binned / distance_binned * 150
    binned_risk = (risk_score // h_bucket_meters)
    # return risk_score.astype(np.uint8)

    # This is a rough estimate of mask size
    mask = distance_pixels < (lines_buffer_size_m / 2)
    risk_score_masked = np.where(mask, binned_risk, 0)

    # Create RGBA where alpha=0 for zero values
    rgba_array = np.zeros((4, *risk_score_masked.shape), dtype=np.uint8)
    rgba_array[:3] = risk_score_masked  # RGB channels
    rgba_array[3] = np.where(risk_score_masked > 0, 255, 0)  # Alpha: 255 for data, 0 for zeros
    print(f"{rgba_array[:3].max()=}")
    if return_risk_array:
        return rgba_array.astype(np.uint8)

    # Finally, suming all values and assigning score to the line
    from tqdm import tqdm
    from rasterio.features import geometry_mask
    
    max_risks = []
    reprojected_buffered_lines = buffered_lines.copy()
    
    for geom in tqdm(reprojected_buffered_lines.geometry):
        mask = geometry_mask([geom], out_shape=risk_score_masked.shape, 
                           transform=transform, invert=True)
        masked_values = risk_score_masked[mask]
        max_risks.append(masked_values.max() if masked_values.size > 0 else 0)
    
    buffered_lines['max_risk'] = max_risks
    print(f"{max(max_risks)=}")
    
    # Return just the GeoDataFrame, not a tuple
    return buffered_lines[['geometry', 'max_risk', 'VOLTAGE', 'pole_height_m']]

def get_pixel_res_in_meters(bounds, transform):
    import geopandas as gpd
    import shapely
    
    utm_crs = gpd.GeoDataFrame(geometry=[shapely.box(*bounds)], crs=4326).estimate_utm_crs()
    
    # Get pixel dimensions in degrees from transform
    pixel_width_degrees = transform.a  # x-direction pixel size
    pixel_height_degrees = -transform.e  # y-direction pixel size (negative)
    
    # Create corner points in geographic coordinates
    xmin, ymin, xmax, ymax = bounds
    corner1 = shapely.Point(xmin, ymin)
    corner2 = shapely.Point(xmin + pixel_width_degrees, ymin)
    corner3 = shapely.Point(xmin, ymin + abs(pixel_height_degrees))
    
    # Project to UTM and calculate distances
    corners_gdf = gpd.GeoDataFrame(geometry=[corner1, corner2, corner3], crs=4326).to_crs(utm_crs)
    pixel_res_x = corners_gdf.geometry[0].distance(corners_gdf.geometry[1])
    pixel_res_y = corners_gdf.geometry[0].distance(corners_gdf.geometry[2])
    pixel_res = (pixel_res_x + pixel_res_y) / 2  # Average resolution in meters

    return round(pixel_res, 2)