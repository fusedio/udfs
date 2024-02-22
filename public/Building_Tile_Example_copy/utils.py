def table_to_tile(bbox, table="s3://fused-asset/imagery/naip/", min_zoom=12, use_columns=['geometry']):
    import fused    
    import pandas as pd 
    z = bbox['z'].values[0]
    df = fused.utils.get_chunks_metadata(table)
    df = df[df.intersects(bbox.geometry[0])]
    if not min_zoom: 
        min_zoom=z+1
    if z>=min_zoom:#z>=12:
        List = df[['file_id','chunk_id']].values
        rows_df = pd.concat([fused.utils.get_chunk_from_table(table, fc[0], fc[1]) for fc in List])
        print('Avaiable columns are:', list(rows_df.columns))
        if use_columns:
            if 'geometry' not in use_columns: use_columns+=['geometry']
            return rows_df[rows_df.intersects(bbox.geometry[0])][use_columns]
        else:
            rows_df=rows_df.sample(1000)
            return rows_df[rows_df.intersects(bbox.geometry[0])]
    else: 
        print(f'Please zoom more. cuurent zoom ({z=}) is less than {min_zoom=}')
        return df     