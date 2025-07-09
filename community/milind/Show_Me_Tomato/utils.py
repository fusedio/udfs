def query_to_params(query, num_match=1, rerank=True, embedding_path="s3://fused-users/fused/misc/embedings/CDL_crop_name.parquet"):
    import pandas as pd
    from openai import OpenAI
    
    print(f"this is the '{query}'")
    df_crops = pd.read_parquet(embedding_path)
    api_key = fused.secrets["my_fused_key"] 
    
    client = OpenAI(api_key=api_key)
    response = client.embeddings.create(input=query, model="text-embedding-3-large")
    query_embedding = response.data[0].embedding
    
    df_crops['similarity'] = df_crops['embedding'].apply(lambda e: cosine_similarity(query_embedding, e))
    
    if not rerank:
        results = df_crops.sort_values('similarity', ascending=False).head(num_match)
        print(results[['value', 'description']])
        return results['value'].tolist()
    
    candidates = df_crops.sort_values('similarity', ascending=False).head(10)
    
    try:
        items = "\n".join([f"{i}) Value: {row['value']}, Description: {row['description']}" 
                         for i, (_, row) in enumerate(candidates.iterrows(), 1)])
        
        response = client.chat.completions.create(
            model="gpt-4.1",
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
    dot_product = sum(x*y for x, y in zip(a, b))
    norm_a = sum(x*x for x in a)**0.5
    norm_b = sum(y*y for y in b)**0.5
    return dot_product / (norm_a * norm_b)
