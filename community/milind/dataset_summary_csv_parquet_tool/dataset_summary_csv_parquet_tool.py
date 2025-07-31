@fused.udf
def udf(path: str = "s3://fused-asset/optimized_t2m_data_small(1).parquet"):
    """
    Simplified file inspector - returns key info needed for charting
    """
    import duckdb
    import pandas as pd
    from pathlib import Path 
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    # Initialize DuckDB connection
    con = duckdb.connect()

    
    # Determine file type
    file_ext = Path(path).suffix.lower()
    
    try:
        # Build queries based on file type
        if file_ext == '.csv':
            base_query = f"SELECT * FROM read_csv('{path}')"
        elif file_ext in ['.parquet', '.pq']:
            base_query = f"SELECT * FROM read_parquet('{path}')"
        elif file_ext == '.json':
            base_query = f"SELECT * FROM read_json('{path}')"
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
        
        # Get essential info
        schema_df = con.sql(f"DESCRIBE {base_query}").df()
        sample_df = con.sql(f"{base_query} LIMIT 10").df()
        row_count = con.sql(f"SELECT COUNT(*) as count FROM ({base_query})").df().iloc[0]['count']
        
        # Categorize columns for charting
        numeric_cols = []
        text_cols = []
        date_cols = []
        
        for _, row in schema_df.iterrows():
            col_name = row['column_name']
            col_type = str(row['column_type']).lower()
            
            if any(t in col_type for t in ['int', 'float', 'double', 'decimal', 'numeric']):
                numeric_cols.append(col_name)
            elif any(t in col_type for t in ['date', 'time', 'timestamp']):
                date_cols.append(col_name)
            else:
                text_cols.append(col_name)
        
        # Create simple summary
        print(f"FILE: {Path(path).name}")
        print(f"ROWS: {row_count:,} | COLUMNS: {len(schema_df)}")
        print(f"NUMERIC: {numeric_cols}")
        print(f"TEXT/CATEGORICAL: {text_cols}")
        print(f"DATES: {date_cols}")
        print("\nSAMPLE DATA:")
        print(sample_df.to_string(index=False, max_rows=5))
        
        # Return structured data for charting
        return {
            'file_name': Path(path).name,
            'rows': row_count,
            'columns': len(schema_df),
            'numeric_columns': numeric_cols,
            'text_columns': text_cols,
            'date_columns': date_cols,
            'sample_data': sample_df,
            'all_columns': schema_df['column_name'].tolist()
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'error': str(e), 'file_name': Path(path).name}