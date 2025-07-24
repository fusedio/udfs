@fused.udf
def udf(path: str = "s3://fused-users/fused/milind/ipinfo_lite.parquet"):
    """
    Inspect file schema and structure - supports CSV, Parquet, JSON, GeoJSON
    """
    import duckdb
    import pandas as pd
    from pathlib import Path
    
    # Initialize DuckDB connection with S3 support
    con = duckdb.connect()
    con.sql("install 'httpfs'; load 'httpfs';")
    con.sql("install 'spatial'; load 'spatial';")  # For GeoJSON support
    
    # Determine file type
    file_ext = Path(path).suffix.lower()
    
    try:
        # Handle different file formats
        if file_ext == '.csv':
            # Get schema
            schema_query = f"DESCRIBE SELECT * FROM read_csv('{path}')"
            sample_query = f"SELECT * FROM read_csv('{path}') LIMIT 5"
            count_query = f"SELECT COUNT(*) as row_count FROM read_csv('{path}')"
            
        elif file_ext in ['.parquet', '.pq']:
            # Get schema
            schema_query = f"DESCRIBE SELECT * FROM read_parquet('{path}')"
            sample_query = f"SELECT * FROM read_parquet('{path}') LIMIT 5"
            count_query = f"SELECT COUNT(*) as row_count FROM read_parquet('{path}')"
            
        elif file_ext == '.json':
            # Get schema
            schema_query = f"DESCRIBE SELECT * FROM read_json('{path}')"
            sample_query = f"SELECT * FROM read_json('{path}') LIMIT 5"
            count_query = f"SELECT COUNT(*) as row_count FROM read_json('{path}')"
            
        elif file_ext == '.geojson':
            # Try to read as GeoJSON first, fallback to JSON
            try:
                schema_query = f"DESCRIBE SELECT * FROM ST_Read('{path}')"
                sample_query = f"SELECT * FROM ST_Read('{path}') LIMIT 5"
                count_query = f"SELECT COUNT(*) as row_count FROM ST_Read('{path}')"
            except:
                # Fallback to JSON reader
                schema_query = f"DESCRIBE SELECT * FROM read_json('{path}')"
                sample_query = f"SELECT * FROM read_json('{path}') LIMIT 5"
                count_query = f"SELECT COUNT(*) as row_count FROM read_json('{path}')"
                
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
        
        # Execute queries
        schema_df = con.sql(schema_query).df()
        sample_df = con.sql(sample_query).df()
        row_count = con.sql(count_query).df().iloc[0]['row_count']
        
        # Analyze column types
        numeric_cols = []
        categorical_cols = []
        datetime_cols = []
        geometry_cols = []
        
        for _, row in schema_df.iterrows():
            col_name = row['column_name']
            col_type = str(row['column_type']).lower()
            
            if any(t in col_type for t in ['int', 'float', 'double', 'decimal', 'numeric']):
                numeric_cols.append(col_name)
            elif any(t in col_type for t in ['date', 'time', 'timestamp']):
                datetime_cols.append(col_name)
            elif any(t in col_type for t in ['geometry', 'point', 'polygon', 'linestring']):
                geometry_cols.append(col_name)
            else:
                categorical_cols.append(col_name)
        
        # Create comprehensive report
        schema_report = f"""
        FILE: {path} ({file_ext.upper()})
        ROWS: {row_count:,}
        COLUMNS: {len(schema_df)}
        
        COLUMN TYPES:
        - Numeric: {', '.join(numeric_cols) if numeric_cols else 'None'}
        - Categorical: {', '.join(categorical_cols) if categorical_cols else 'None'}
        - DateTime: {', '.join(datetime_cols) if datetime_cols else 'None'}
        - Geometry: {', '.join(geometry_cols) if geometry_cols else 'None'}
        
        SCHEMA:
        {schema_df.to_string(index=False)}
        
        SAMPLE DATA (first 5 rows):
        {sample_df.to_string(index=False)}
        """
        
        print(schema_report)
        
        # Return DataFrame with comprehensive info
        result = pd.DataFrame({
            'file_path': [path],
            'file_format': [file_ext.upper()],
            'row_count': [row_count],
            'column_count': [len(schema_df)],
            'columns': [', '.join(schema_df['column_name'].tolist())],
            'numeric_columns': [', '.join(numeric_cols) if numeric_cols else 'None'],
            'categorical_columns': [', '.join(categorical_cols) if categorical_cols else 'None'],
            'datetime_columns': [', '.join(datetime_cols) if datetime_cols else 'None'],
            'geometry_columns': [', '.join(geometry_cols) if geometry_cols else 'None'],
            'description': [schema_report.strip()]
        })
        
        return result
        
    except Exception as e:
        # Return error info as DataFrame
        error_df = pd.DataFrame({
            'file_path': [path],
            'file_format': [file_ext.upper()],
            'error': [str(e)],
            'description': [f"Error reading {path}: {str(e)}"]
        })
        print(f"Error reading {path}: {str(e)}")
        return error_df