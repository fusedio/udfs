@fused.udf
def udf(
    query: str = "show me water",  # Natural language query instead of direct vals
    bounds: fused.types.Bounds = [
        -125.82165127797666,21.313670812049978,-65.62955940309448,52.58604956417555
    ], # Default to global continental US (without Alaska)
    min_ratio: float = 0,  # Filter by minimum percentage coverage
    res_offset: int = 0,   # Adjust resolution dynamically
):
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    
    # Convert natural language query to crop values using embeddings
    crop_value_list = common.query_to_params(query)
    print(f"Crop values from query '{query}': {crop_value_list}")
    
    # Data path
    path = "s3://fused-asset/data/cdls/public_demo_2024/"
    
    # Calculate hex resolution dynamically based on bounds (instead of hard-coding)
    hex_res = bounds_to_res(bounds, res_offset)
    print(f"Using hex resolution: {hex_res}")
    
    # Read hex data
    df = read_hex_table(hex_res, bounds, path)
    
    # Filter by crop values from natural language query
    df = df[df["data"].isin(crop_value_list)]
    
    # Apply minimum ratio filter if specified
    if min_ratio > 0:
        df = df[df['pct'] > 100 * min_ratio]
    
    print(f"Filtered data shape: {df.shape}")
    print(df.head())
    
    return df

def bounds_to_res(bounds, res_offset=0, max_res=11, min_res=3):
    """Calculate optimal hex resolution based on geographic bounds"""
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    z = common.estimate_zoom(bounds)
    return max(min(int(3 + z / 1.5 - res_offset), max_res), min_res)

def read_overview(hex_res, bounds, path):
    """Read overview data for lower resolutions"""
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    con = common.duckdb_connect()
    df = con.sql(f'select * from read_parquet("{path}_hex{hex_res}")').df()
    df["pct"] = 255 * df["area"] / df["area"].max()
    df = common.filter_hex_bounds(df, bounds, col_hex="hex")
    return df

def read_hex_table(hex_res, bounds, path, base_res=7):
    """Read hex table data with dynamic resolution handling"""
    import pandas as pd
    
    if hex_res <= base_res:
        return read_overview(hex_res, bounds, path)
    
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    con = common.duckdb_connect()
    bbox = common.to_gdf(bounds)
    df_meta = fused.get_chunks_metadata(path)
    df_meta = df_meta.sjoin(bbox)
    L = df_meta[["file_id", "chunk_id"]].values
    hex_bounds = common.bounds_to_hex(bounds, 7)
    
    def fn(x):
        df = fused.get_chunk_from_table(
            path, x[0], x[1], 
            columns=["cnt", "cnt_total", "data", "pos7", "pos8", "pos9", "pos10", "pos11"]
        )
        df = df[df.pos7.isin(hex_bounds.hex)]
        return df
    
    df = common.run_pool(fn, L)
    df = pd.concat(df)
    
    # Use H3 conversion for higher resolution data
    H3_From_To_Pos = fused.load("https://github.com/fusedlabs/fusedudfs/tree/5738389/H3_From_To_Pos/")
    qr = H3_From_To_Pos.h3_from_pos_query("(select * from df)", columns="*", hex_res=hex_res, base_res=7)
    qr = f"""SELECT hex, data, 
             (100*sum(cnt/cnt_total)/7^{11-hex_res})::FLOAT pct, 
             (h3_cell_area(hex,'m^2')*pct/100) as area, 
             (sum(cnt/cnt_total)*h3_cell_area(h3_cell_to_center_child(any_value(hex),11),'m^2'))::DOUBLE area2 
             FROM ({qr})  
             GROUP BY 1,2"""
    df = con.sql(qr).df()
    return df

# If you want to implement the natural language query functionality
def query_to_params(query, num_match=1, rerank=True, embedding_path="s3://fused-users/fused/misc/embedings/CDL_crop_name.parquet"):
    """Convert natural language query to crop parameters using embeddings"""
    import pandas as pd
    from openai import OpenAI
    
    print(f"Processing query: '{query}'")
    df_crops = pd.read_parquet(embedding_path)
    api_key = fused.secrets["my_fused_key"] 
    
    client = OpenAI(api_key=api_key)
    response = client.embeddings.create(input=query, model="text-embedding-3-large")
    query_embedding = response.data[0].embedding
    
    # Calculate similarity scores
    df_crops['similarity'] = df_crops['embedding'].apply(lambda e: cosine_similarity(query_embedding, e))
    
    if not rerank:
        results = df_crops.sort_values('similarity', ascending=False).head(num_match)
        print(results[['value', 'description']])
        return results['value'].tolist()
    
    # Get top candidates for reranking
    candidates = df_crops.sort_values('similarity', ascending=False).head(10)
    
    try:
        # Create reranking prompt
        items = "\n".join([f"{i}) Value: {row['value']}, Description: {row['description']}" 
                         for i, (_, row) in enumerate(candidates.iterrows(), 1)])
        
        # Use LLM to rerank results
        response = client.chat.completions.create(
            model="gpt-4o",  # Updated model name
            messages=[{"role": "user", "content": 
                f"Query: {query}\nRate relevance (0-100):\n{items}\n\nComma-separated scores only:"}],
            temperature=0
        )
        
        scores = [int(s.strip()) for s in response.choices[0].message.content.split(',')]
        if len(scores) != len(candidates):
            raise ValueError(f"Got {len(scores)} scores for {len(candidates)} candidates")
            
        candidates.loc[:, 'rerank_score'] = scores
        relevant = candidates[candidates['rerank_score'] > 40].sort_values('rerank_score', ascending=False)
        
        if len(relevant) == 0 and len(candidates) > 0:
            relevant = candidates.sort_values('rerank_score', ascending=False).head(1)
            
        print(relevant[['value', 'description', 'rerank_score']])
        return relevant['value'].tolist()
        
    except Exception as e:
        print(f"Reranking failed: {e}, using embedding search")
        results = candidates.head(num_match)
        print(results[['value', 'description']])
        return results['value'].tolist()

def cosine_similarity(a, b):
    """Calculate cosine similarity between two vectors"""
    dot_product = sum(x*y for x, y in zip(a, b))
    norm_a = sum(x*x for x in a)**0.5
    norm_b = sum(y*y for y in b)**0.5
    return dot_product / (norm_a * norm_b)