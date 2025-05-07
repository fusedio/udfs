@fused.udf
def udf(bounds: fused.types.TileGDF=None):
    import geopandas as gpd
    from utils import get_meta, get_table, load_input_gdf
    import numpy as np
    import pandas as pd


    # 1. Load input table -- this is just to get the bounds
    # to determine Overture chunks that intersect. This data is
    # loaded again later in udf_nail to do the actual 
    # intersection
    path = "s3://fused-users/pilotfiber/pilot_footprints_20250421/"

    gdf_input = load_input_gdf(path)
    bounds = fused.utils.common.bounds_to_gdf(gdf_input.total_bounds)
    # return fused.utils.common.bounds_to_gdf(gdf_input.total_bounds)

    bounds_m = bounds.to_crs("EPSG:3857")

    # 3. Buffer by 60 miles (1 mile ≈ 1 609.344 m)
    buffer_distance_m = 60 * 1609.344
    gdf_buffered_m = bounds_m.buffer(buffer_distance_m)
    
    # 4. Put the buffered geometry back into a GeoDataFrame and reproject to WGS84
    gdf_buffered = (
        gpd.GeoDataFrame(geometry=gdf_buffered_m, crs="EPSG:3857")
           .to_crs("EPSG:4326")
    )

    # 2. Determine chunks of Overture Table that intersect
    chunks_metadata = get_meta()
    intersecting_chunks = chunks_metadata.sjoin(gdf_buffered, how='inner')
    # return intersecting_chunks

    # 3. Run UDF across intersecting chunks
    out = fused.submit(udf_nail, intersecting_chunks[['part_id', 'file_id', 'chunk_id']], collect=True)
    return out.sample(1000)

@fused.udf
def udf_nail(part_id: int=0, file_id: str= '16', chunk_id: int = 79, path: str = 's3://fused-users/pilotfiber/pilot_footprints_20250421/'): # todo one is str
    from utils import get_meta, get_table, load_input_gdf
    import geopandas as gpd
    import pandas as pd

    # 1. Get metadata bounding box for the chunk
    chunks_metadata = get_meta()

    chunks_metadata = chunks_metadata[
        (chunks_metadata['part_id'] == part_id) & 
        (chunks_metadata['file_id'] == file_id) &
        (chunks_metadata['chunk_id'] == chunk_id)
    ]

    # 2. Get overture table
    gdf_ov = get_table(part_id, file_id, chunk_id)
    gdf_ov['_src'] = 'OVERTURE'
    
    # 3. Load input table
    gdf = load_input_gdf(path)

    # 4. Use input table centroids to filter contained records
    gdf['centroid'] = gdf.geometry.centroid
    centroids_gdf = gpd.GeoDataFrame(gdf, geometry='centroid', crs=gdf.crs)
    gdf_input = centroids_gdf.sjoin(chunks_metadata, predicate='within').drop(columns=['index_right'], errors='ignore')

    # 5. Assign GERS id at centroid intersection, where applicable (workaround to 503 error)
    gdf_input = gdf_input.sjoin(gdf_ov[['id', 'geometry']], predicate='within')
    gdf_input=gdf_input.set_geometry('geometry') 
    gdf_input['_src'] = 'INPUT_TABLE'

    # 6. Determine Intersecting buildings, give precedence to input table's
    idx_input_intersecting, idx_overture_intersecting = gdf_ov.sindex.query(gdf_input.geometry, predicate="intersects")
    gdf_ov_nonoverlapping = gdf_ov.loc[~gdf_ov.index.isin(idx_overture_intersecting)]

    # 7. Concatenate "Overture non-overlapping" and "Input table" buildings
    gdf_conflated = pd.concat([gdf_ov_nonoverlapping, gdf_input], ignore_index=True)

    # TODO: IOU Score, keep above threshold

    return gdf_conflated[['id', 'origid', 'flightdeckid', 'salesforceid', '_src', 'geometry']]
    

