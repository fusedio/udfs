@fused.udf
def udf(
    path: str,
):
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    
    print("Schema:")
    print(parquet_file.schema)
    
    column_names = parquet_file.schema.names
    print("\nColumn names:")
    print(column_names)
    
    first_rowgroup = parquet_file.read_row_group(0)
    df = first_rowgroup.to_pandas()
    
    print("\nFirst 5 rows:")
    print(df.head(5).T)
