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
    # path = fused.download(path, path)
    df = gpd.read_parquet(path)
    df=df.rename(columns={'geom': 'geometry'}).set_geometry('geometry')
    df = df.to_crs('EPSG:4326')
    return df


