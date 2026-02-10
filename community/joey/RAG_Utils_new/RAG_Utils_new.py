# Constants
DEFAULT_LANCEDB_PATH = "/mount/lancedb/"
DEFAULT_QWEN_EMBEDDING_MODEL = "qwen/qwen3-embedding-8b"
DEFAULT_OPENAI_EMBEDDING_MODEL = "text-embedding-3-large"
DEFAULT_LLM_MODEL = "openai/gpt-oss-120b"
DEFAULT_MODEL_PROVIDER = "groq"
DEFAULT_ENHANCE_PROMPT = "Enhance descriptions for better search. Add relevant keywords, age ranges, demographic terms. Under 50 words."

variable_list = [
  "apple", "banana", "car", "dog", "elephant", "forest", "guitar", "house",
"island", "jungle",
  "kite", "lemon", "mountain", "notebook", "ocean", "piano", "queen", "river",
"sunset", "tree",
  "umbrella", "violin", "waterfall", "xylophone", "yacht", "zebra", "quantum",
"nebula", "pixel", "algorithm",
  "galaxy", "robot", "dragon", "castle", "wizard", "adventure", "mystery",
"journey", "discovery", "innovation",
  "harmony", "symphony", "breeze", "whisper", "echo", "shadow", "light",
"crystal", "dream",
]


@fused.udf
def udf(
  variable_list: list = variable_list,
  table_name: str = "my_list",
  enhance_prompt: str = DEFAULT_ENHANCE_PROMPT,
  overwrite: bool = True,
  verbose: bool = True,
):
  import pandas as pd

    
  df = pd.DataFrame(variable_list, columns=["variable"])
  df = add_context(
      df, enhance_prompt, input_col="variable", output_col="context",
verbose=verbose
  )
  df = add_embedding(df, variable_col="context", verbose=verbose)
  table_path = write_vector_table(
      df, table_name, overwrite=overwrite, verbose=verbose
  )
  if verbose:
      print(df.columns)
  df2 = read_vector_table(table_name, verbose=verbose)
  return df2


@fused.cache
def enhance_text(
  text: str,
  enhance_prompt: str,
  keep_original: bool = False,
  provider: str = "openrouter",
  model: str = DEFAULT_LLM_MODEL,
  model_provider: str = DEFAULT_MODEL_PROVIDER,
  verbose: bool = False,
  max_tokens: int = 500,
  temperature: float = 0.3,
) -> str:
  """
  Enhance text using an LLM.

  Args:
      text: The text to enhance
      enhance_prompt: System prompt for enhancement
      keep_original: If True, return "original | enhanced"
      provider: API provider - "openai" or "openrouter"
      model: Model name (default: "openai/gpt-oss-120b" - OpenAI's OSS model on
OpenRouter)
      model_provider: For OpenRouter, the serving provider (default: "groq")
      verbose: Print debug information
      max_tokens: Maximum tokens in response
      temperature: Sampling temperature

  Returns:
      Enhanced text string
  """
  if not enhance_prompt:
      return text

  if provider == "openai":
      from openai import OpenAI

      client = OpenAI(api_key=fused.secrets["openai_fused"])
      response = client.chat.completions.create(
          model=model,
          messages=[
              {"role": "system", "content": enhance_prompt},
              {"role": "user", "content": f"Enhance: {text}"},
          ],
          max_tokens=max_tokens,
          temperature=temperature,
      )
      context = response.choices[0].message.content

  elif provider == "openrouter":
      import requests

      headers = {
          "Content-Type": "application/json",
          "Authorization": f"Bearer {fused.secrets['openrouter_api_key']}",
      }

      payload = {
          "model": model,
          "messages": [
              {"role": "system", "content": enhance_prompt},
              {"role": "user", "content": f"Enhance: {text}"},
          ],
          "max_tokens": max_tokens,
          "temperature": temperature,
      }

      if model_provider:
          payload["provider"] = {
              "order": [model_provider],
              "allow_fallbacks": True,
          }
          payload["reasoning"] = {
              "enabled": True,
              "effort": 'minimal'
          }

      response = requests.post(
          "https://openrouter.ai/api/v1/chat/completions",
          headers=headers,
          json=payload,
      )
      response.raise_for_status()
      context = response.json()["choices"][0]["message"]["content"]

  else:
      raise ValueError(f"Unknown provider: {provider}. Use 'openai' or 'openrouter'")

  if verbose:
      print(f"Enhanced text: {text} -> {context}")

  if keep_original:
      return f"{text} | {context}"
  else:
      return context


@fused.cache
def add_context(
  df,
  enhance_prompt: str = DEFAULT_ENHANCE_PROMPT,
  input_col: str = "variable",
  output_col: str = "context",
  provider: str = "openrouter",
  model: str = DEFAULT_LLM_MODEL,
  model_provider: str = DEFAULT_MODEL_PROVIDER,
  max_tokens: int = 500,
  temperature: float = 0.3,
  keep_original: bool = False,
  max_workers: int = 256,
  verbose: bool = False,
):
  """
  Add LLM-enhanced context to a dataframe column.

  Args:
      df: Input dataframe
      enhance_prompt: System prompt for enhancement
      input_col: Column name to enhance
      output_col: Column name for enhanced output
      provider: API provider - "openai" or "openrouter"
      model: Model name (default: "openai/gpt-oss-120b")
      model_provider: For OpenRouter, the serving provider (default: "groq")
      max_tokens: Maximum tokens in response
      temperature: Sampling temperature
      keep_original: If True, return "original | enhanced"
      max_workers: Number of parallel workers
      verbose: Print debug information

  Returns:
      DataFrame with added context column
  """

  common = fused.load(
      "https://github.com/fusedio/udfs/tree/2528576/public/common/"
  ).utils
  enhanced_texts = common.run_pool(
      lambda text: enhance_text(
          text,
          enhance_prompt,
          provider=provider,
          model=model,
          model_provider=model_provider,
          keep_original=keep_original,
          temperature=temperature,
          max_tokens=max_tokens,
          verbose=verbose,
      ),
      df[input_col],
      max_workers=max_workers,
  )
  df[output_col] = enhanced_texts
  if verbose:
      print(f"Added context to {len(df)} rows")
  return df


@fused.cache
def embed_text(
  text: str,
  embedding_model: str = None,
  provider: str = "qwen",
  verbose: bool = False,
) -> list:
  """Embed a single text string.

  Args:
      text: Text to embed
      embedding_model: Model name (defaults based on provider)
      provider: "qwen" (via openrouter) or "openai"
      verbose: Print progress

  Returns:
      List of floats (embedding vector)
  """
  # Set default model based on provider
  if embedding_model is None:
      embedding_model = DEFAULT_QWEN_EMBEDDING_MODEL if provider == "qwen" else DEFAULT_OPENAI_EMBEDDING_MODEL

  if provider == "qwen":
      return embed_text_qwen(text, embedding_model=embedding_model, verbose=verbose)

  from openai import OpenAI
  client = OpenAI(api_key=fused.secrets["openai_fused"])
  response = client.embeddings.create(model=embedding_model, input=text)
  if verbose:
      print(f"Embedded text: {text[:50]}...")
  return response.data[0].embedding


@fused.cache
def embed_text_qwen(
  text: str,
  embedding_model: str = DEFAULT_QWEN_EMBEDDING_MODEL,
  verbose: bool = False,
) -> list:
  """Embed a single text string using Qwen via OpenRouter.

  Args:
      text: Text to embed
      embedding_model: Defaults to "qwen/qwen3-embedding-8b" (4096 dims)
      verbose: Print progress

  Returns:
      List of floats (embedding vector)
  """
  import requests

  response = requests.post(
      "https://openrouter.ai/api/v1/embeddings",
      headers={
          "Authorization": f"Bearer {fused.secrets['openrouter_api_key']}",
          "Content-Type": "application/json",
      },
      json={"model": embedding_model, "input": text}
  )
  response.raise_for_status()

  if verbose:
      print(f"Embedded text (qwen): {text[:50]}...")
  return response.json()["data"][0]["embedding"]


def embed_texts_batch(
  texts: list,
  embedding_model: str = None,
  provider: str = "qwen",
  batch_size: int = 100,
  verbose: bool = False,
) -> list:
  """Batch embed multiple texts in fewer API calls.

  Args:
      texts: List of texts to embed
      embedding_model: Model name (defaults based on provider)
      provider: "qwen" (via openrouter) or "openai"
      batch_size: Texts per API call (default 100 for OpenAI, 400 for Qwen)
      verbose: Print progress

  Returns:
      List of embedding vectors
  """
  import time

  # Set defaults based on provider
  if embedding_model is None:
      embedding_model = DEFAULT_QWEN_EMBEDDING_MODEL if provider == "qwen" else DEFAULT_OPENAI_EMBEDDING_MODEL

  if provider == "qwen":
      batch_size = min(batch_size, 400)  # Qwen supports larger batches

  all_embeddings = []
  total = len(texts)

  for i in range(0, total, batch_size):
      batch = list(texts[i:i + batch_size])

      if verbose:
          print(f"Embedding batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} ({len(batch)} texts)")

      if provider == "qwen":
          embeddings = _embed_batch_qwen(batch, embedding_model)
      else:
          embeddings = _embed_batch_openai(batch, embedding_model)

      all_embeddings.extend(embeddings)

      # Small delay between batches to avoid rate limiting
      if i + batch_size < total:
          time.sleep(0.2)

  if verbose:
      print(f"Embedded {len(all_embeddings)} texts total")

  return all_embeddings


def _embed_batch_openai(
  texts: list,
  embedding_model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
) -> list:
  """Internal: Batch embed using OpenAI API."""
  from openai import OpenAI

  client = OpenAI(api_key=fused.secrets["openai_fused"])
  response = client.embeddings.create(model=embedding_model, input=texts)
  return [item.embedding for item in response.data]


def _embed_batch_qwen(
  texts: list,
  embedding_model: str = DEFAULT_QWEN_EMBEDDING_MODEL,
) -> list:
  """Internal: Batch embed using Qwen via OpenRouter."""
  import requests

  response = requests.post(
      "https://openrouter.ai/api/v1/embeddings",
      headers={
          "Authorization": f"Bearer {fused.secrets['openrouter_api_key']}",
          "Content-Type": "application/json",
      },
      json={"model": embedding_model, "input": texts}
  )
  response.raise_for_status()
  return [item["embedding"] for item in response.json()["data"]]


@fused.cache
def add_embedding(
  df,
  variable_col: str = "context",
  embedding_col: str = "embedding",
  embedding_model: str = None,
  provider: str = "qwen",
  batch_size: int = 100,
  max_workers: int = 16,
  verbose: bool = False,
):
  """Add embeddings to dataframe using parallel batch processing.

  Args:
      df: DataFrame with text column
      variable_col: Column containing text to embed
      embedding_col: Output column for embeddings
      embedding_model: Model name (defaults based on provider)
      provider: "qwen" (via openrouter, default) or "openai"
      batch_size: Texts per API call (default 100)
      max_workers: Parallel batch workers (default 16)
      verbose: Print progress

  Returns:
      DataFrame with embedding column added
  """
  common = fused.load(
      "https://github.com/fusedio/udfs/tree/2528576/public/common/"
  ).utils

  # Set default model based on provider
  if embedding_model is None:
      embedding_model = DEFAULT_QWEN_EMBEDDING_MODEL if provider == "qwen" else DEFAULT_OPENAI_EMBEDDING_MODEL

  # Qwen supports larger batches
  if provider == "qwen":
      batch_size = min(batch_size, 400)

  texts = list(df[variable_col])
  total = len(texts)

  # Split into batches
  batches = [texts[i:i + batch_size] for i in range(0, total, batch_size)]
  num_batches = len(batches)

  if verbose:
      print(f"Embedding {total} texts in {num_batches} batches (batch_size={batch_size}, provider={provider})")

  # Define batch embedding function
  def embed_batch(batch_texts):
      if provider == "qwen":
          return _embed_batch_qwen(batch_texts, embedding_model)
      else:
          return _embed_batch_openai(batch_texts, embedding_model)

  # Run batches in parallel using run_pool
  batch_results = common.run_pool(
      embed_batch,
      batches,
      max_workers=max_workers,
  )

  # Flatten results back to single list
  embeddings = []
  for batch_embeddings in batch_results:
      embeddings.extend(batch_embeddings)

  df[embedding_col] = embeddings
  if verbose:
      print(f"Added {len(embeddings)} embeddings ({len(embeddings[0])} dims)")
  return df


def infer_schema(
  df,
  embedding_col: str = "embedding",
  vector_dim: int = None,
):
  """Infer PyArrow schema from DataFrame with special handling for embedding
column."""
  import pyarrow as pa

  # Auto-generate schema from DataFrame
  table_data = pa.Table.from_pandas(df)
  schema = table_data.schema

  # If vector_dim not provided, infer from first embedding
  if vector_dim is None and embedding_col in df.columns:
      vector_dim = len(df[embedding_col].iloc[0])

  # Ensure embedding column has correct type (list of floats with vector_dim)
  if embedding_col in df.columns:
      fields = []
      for field in schema:
          if field.name == embedding_col:
              # Replace with correct vector type
              fields.append(
                  pa.field(embedding_col, pa.list_(pa.float64(), vector_dim))
              )
          else:
              # Keep other fields as-is
              fields.append(field)
      schema = pa.schema(fields)
  return schema


@fused.cache
def write_vector_table(
  df,
  table_name: str,
  embedding_col: str = "embedding",
  metric: str = "cosine",
  base_path: str = DEFAULT_LANCEDB_PATH,
  overwrite: bool = False,
  verbose: bool = False,
) -> str:
  import lancedb
  import pyarrow as pa

  db = lancedb.connect(base_path)

  if table_name in db.table_names():
      if not overwrite:
          raise ValueError(
              f"Table '{table_name}' already exists. "
              f"Set overwrite=True to replace it."
          )
      else:
          db.drop_table(table_name)
          if verbose:
              print(f"Table '{table_name}' was successfully replaced.")

  vector_dim = len(df[embedding_col].iloc[0])
  if verbose:
      print(f"{vector_dim=}")  # (3072 or 1536)

  # Use the new infer_schema function
  schema = infer_schema(df, embedding_col=embedding_col, vector_dim=vector_dim)

  if verbose:
      print(f"Inferred schema: {schema}")

  # Create table with auto-generated schema
  table_data = pa.table(df, schema=schema)
  db.create_table(table_name, table_data)

  table = db.open_table(table_name)

  if len(df) >= 256:  # lancedb does not support vector index less than 256 rows
      table.create_index(metric=metric, vector_column_name=embedding_col)
      if verbose:
          print(f"Vector index created on '{embedding_col}' column")
  else:
      if verbose:
          print(f"Skipping index - need 256+ rows, have {len(df)}")

  table_path = f"{base_path}{table_name}"
  return table_path


def read_vector_table(
  table_name: str,
  base_path: str = DEFAULT_LANCEDB_PATH,
  verbose: bool = False,
):
  import lancedb

  db = lancedb.connect(DEFAULT_LANCEDB_PATH)
  table = db.open_table(table_name)
  if verbose:
      print("table columns:", table.schema)
  return table.to_pandas()


def search_vector_table(
  table_name: str,
  query_vec: list,
  top_k: int,
  verbose: bool = False,
):
  import numpy as np
  import lancedb

  db = lancedb.connect(DEFAULT_LANCEDB_PATH)
  table = db.open_table(table_name)

  try:
      results = (
          table.search(np.array(query_vec), vector_column_name="embedding")
          .limit(top_k)
          .to_pandas()
      )
      results["similarity"] = 1.0 - results["_distance"]
      if verbose:
          print("Used vector index search")
  except Exception:
      df = table.to_pandas()
      embeddings = np.stack(df["embedding"].values)
      similarities = np.dot(embeddings, query_vec) / (
          np.linalg.norm(embeddings, axis=1) * np.linalg.norm(query_vec)
      )
      idx = np.argsort(similarities)[-top_k:][::-1]
      results = df.iloc[idx].copy()
      results["similarity"] = similarities[idx]
      if verbose:
          print("Used manual similarity search")

  return results

# =============================================================================
# MILVUS INTEGRATION (OPTIONAL)
# =============================================================================
# These functions provide an alternative to LanceDB for users who prefer
# to use Milvus/Zilliz Cloud for vector storage and search.
# =============================================================================

def batch_upload_to_milvus(
  batch_data: list,
  collection_name: str,
  milvus_url: str,
  api_key_secret: str = "milvus-zilliz",
  verbose: bool = False,
) -> dict:
  """
  Upload multiple embedding vectors to Milvus/Zilliz Cloud database in a single
batch.

  This is an optional alternative to LanceDB for users who prefer Milvus for
  vector storage. Requires a Zilliz Cloud account and API key stored in
fused.secrets.

  Args:
      batch_data: List of dicts, each with 'code' (or your text field) and
'vector' keys
                  Example: [{"code": "text content", "vector": [0.1, 0.2, ...]},
...]
      collection_name: Name of the Milvus collection to insert into
      milvus_url: Full Milvus/Zilliz insert endpoint URL (required)
                  Example:
"https://xxx.serverless.aws-region.cloud.zilliz.com/v2/vectordb/entities/insert"
      api_key_secret: Name of the fused.secrets key containing the Milvus API
key
      verbose: Print debug information

  Returns:
      dict with status, milvus_response, batch_size, and debug info

  Example:
      >>> data = [{"code": "hello world", "vector": embedding_vec}]
      >>> url = "https://xxx.cloud.zilliz.com/v2/vectordb/entities/insert"
      >>> result = batch_upload_to_milvus(data, "my_collection", milvus_url=url)
      >>> print(result["status"])  # "success" or "error"
  """
  import requests
  import json

  if not milvus_url:
      raise ValueError(
          "milvus_url is required. Provide your Zilliz Cloud endpoint URL, e.g.:\n"
          "'https://xxx.serverless.aws-region.cloud.zilliz.com/v2/vectordb/entities/insert'"
      )

  payload = {
      "collectionName": collection_name,
      "data": batch_data
  }

  headers = {
      "Authorization": f"Bearer {fused.secrets[api_key_secret]}",
      "Accept": "application/json",
      "Content-Type": "application/json"
  }

  try:
      response = requests.post(milvus_url, data=json.dumps(payload),
headers=headers)
      response.raise_for_status()

      result = response.json()

      if verbose:
          print(f"✓ Uploaded {len(batch_data)} vectors to Milvus collection '{collection_name}'")

      return {
          "status": "success",
          "milvus_response": result,
          "batch_size": len(batch_data),
          "vector_dimensions": len(batch_data[0]['vector']) if batch_data else 0
      }

  except Exception as e:
      if verbose:
          print(f"✗ Milvus upload failed: {e}")
      return {
          "status": "error",
          "error": str(e)
      }


def search_milvus(
  query_vector: list,
  collection_name: str,
  milvus_url: str,
  top_k: int = 10,
  output_fields: list = None,
  api_key_secret: str = "milvus-zilliz",
  verbose: bool = False,
) -> dict:
  """
  Search for similar vectors in a Milvus/Zilliz Cloud collection.

  This is an optional alternative to LanceDB for users who prefer Milvus for
  vector search. Requires a Zilliz Cloud account and API key stored in
fused.secrets.

  Args:
      query_vector: List of floats representing the query embedding vector
      collection_name: Name of the Milvus collection to search
      milvus_url: Full Milvus/Zilliz search endpoint URL (required)
                  Example:
"https://xxx.serverless.aws-region.cloud.zilliz.com/v2/vectordb/entities/search"
      top_k: Number of results to return (default: 10)
      output_fields: List of fields to return. If None, returns all fields ["*"]
      api_key_secret: Name of the fused.secrets key containing the Milvus API
key
      verbose: Print debug information

  Returns:
      dict with status and either 'results' (list of matches) or 'error'

  Example:
      >>> query_vec = embed_text("how do I use fused?")
      >>> url = "https://xxx.cloud.zilliz.com/v2/vectordb/entities/search"
      >>> results = search_milvus(query_vec, "fused_docs", milvus_url=url,
top_k=5)
      >>> for match in results["results"]:
      ...     print(match["code"], match["distance"])
  """
  import requests
  import json

  if not milvus_url:
      raise ValueError(
          "milvus_url is required. Provide your Zilliz Cloud endpoint URL, e.g.:\n"
          "'https://xxx.serverless.aws-region.cloud.zilliz.com/v2/vectordb/entities/search'"
      )

  if output_fields is None:
      output_fields = ["*"]

  payload = {
      "collectionName": collection_name,
      "data": [query_vector],
      "limit": top_k,
      "outputFields": output_fields
  }

  headers = {
      "Authorization": f"Bearer {fused.secrets[api_key_secret]}",
      "Accept": "application/json",
      "Content-Type": "application/json"
  }

  try:
      response = requests.post(milvus_url, data=json.dumps(payload),
headers=headers)
      response.raise_for_status()

      result = response.json()

      # Extract the search results
      results = result.get("data", [])

      if verbose:
          print(f"✓ Found {len(results)} results in Milvus collection '{collection_name}'")

      return {
          "status": "success",
          "results": results,
          "collection_name": collection_name,
          "num_results": len(results)
      }

  except Exception as e:
      if verbose:
          print(f"✗ Milvus search failed: {e}")
      return {
          "status": "error",
          "error": str(e)
      }
