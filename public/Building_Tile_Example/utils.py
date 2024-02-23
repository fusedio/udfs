def table_to_tile(bbox, table="s3://fused-asset/imagery/naip/", min_zoom=12, centorid_zoom_offset=0, use_columns=['geometry'], clip=False):
    version="0.2.4"
    import fused    
    import pandas as pd
    try:
        x,y,z = bbox[['x','y','z']].iloc[0]
        print(x,y,z)
    except:
        z=min_zoom
    df = fused.utils.get_chunks_metadata(table)
    df = df[df.intersects(bbox.geometry[0])]
    if z>=min_zoom:#z>=12:
        List = df[['file_id','chunk_id']].values
        if use_columns:
            if 'geometry' not in use_columns: use_columns+=['geometry']
            rows_df = pd.concat([fused.utils.get_chunk_from_table(table, fc[0], fc[1], columns=use_columns) for fc in List])
        else:
            rows_df = pd.concat([fused.utils.get_chunk_from_table(table, fc[0], fc[1]) for fc in List])
            print('avaiable columns:', list(rows_df.columns))
        df = rows_df[rows_df.intersects(bbox.geometry[0])]
        df.crs=bbox.crs 
        if z<min_zoom+centorid_zoom_offset:# switch to centroid for the last one zoom level before showing metadata
            df.geometry=df.geometry.centroid
        if clip: return df.clip(bbox).explode()
        else: return df
    else:
        if clip: return df.clip(bbox).explode()
        else: return df