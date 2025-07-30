@fused.udf
def udf():
    import geopandas as gpd
    import numpy as np
    import pandas as pd


    # 1. Load input table
    # Option A: download
    path = 'https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/building-footprints-2015/exports/parquet?lang=en'

    # TODO: Option B: ingest https://www.fused.io/app/UDF_Fused_Ingest_Form
    
    gdf_input = load_input_gdf(path)
    # return fused.utils.common.bounds_to_gdf(gdf.total_bounds)

    # 2. Determine chunks of Overture Table that intersect
    chunks_metadata = get_meta()
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    bounds = common.bounds_to_gdf(gdf_input.total_bounds)
    intersecting_chunks = chunks_metadata.sjoin(bounds, how='inner')

    # 3. Run UDF across intersecting chunks
    out = fused.submit(udf_nail, intersecting_chunks[['part_id', 'file_id', 'chunk_id']], collect=True)
    return out#.sample(10000)

@fused.udf
def udf_nail(part_id: int=0, file_id: str= '16', chunk_id: int = 79, path: str = 'https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/building-footprints-2015/exports/parquet?lang=en'): # todo one is str
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

    return gdf_conflated[['id', '_src', 'geometry']]
    
@fused.cache
def get_meta(table_base_path = "s3://us-west-2.opendata.source.coop/fused/overture/2024-08-20-0/theme=buildings/type=building", n_parts=5):
    import pandas as pd
    table = lambda part: f'{table_base_path}/part={str(part)}/'
    a = []
    for part in range(n_parts):
        a.append(fused.get_chunks_metadata(table(part)))
        a[-1]['part_id'] = part
    return pd.concat(a)


@fused.cache
def get_table(part_id, file_id, chunk_id, table_base_path = "s3://us-west-2.opendata.source.coop/fused/overture/2024-08-20-0/theme=buildings/type=building"):
    table = lambda part: f'{table_base_path}/part={str(part)}/'
    return fused.get_chunk_from_table(table(part_id), file_id=file_id, chunk_id=chunk_id)


@fused.cache
def load_input_gdf(path):
    import geopandas as gpd
    path = fused.download(path, path)
    df = gpd.read_parquet(path)
    df=df.rename(columns={'geom': 'geometry'}).set_geometry('geometry')
    df = df.to_crs('EPSG:4326')
    return df



