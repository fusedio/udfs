
# Overture by chunks
table = lambda part: f's3://us-west-2.opendata.source.coop/fused/overture/2024-08-20-0/theme=buildings/type=building/part={str(part)}/'
# meta = pd.concat([fused.get_chunks_metadata(table(part)) for part in range(5)])
def get_meta():
    import pandas as pd
    a = []
    for part in range(5):
        a.append(fused.get_chunks_metadata(table(part)))
        a[-1]['part_id'] = part
    return pd.concat(a)


@fused.cache
def get_table(part_id, file_id, chunk_id):
    table = lambda part: f's3://us-west-2.opendata.source.coop/fused/overture/2024-08-20-0/theme=buildings/type=building/part={str(part)}/'
    return fused.get_chunk_from_table(table(part_id), file_id=file_id, chunk_id=chunk_id)


@fused.cache
def load_input_gdf(path):
    import geopandas as gpd
    path = fused.download(path, path)
    df = gpd.read_parquet(path)
    df=df.rename(columns={'geom': 'geometry'}).set_geometry('geometry')
    df = df.to_crs('EPSG:4326')
    return df


