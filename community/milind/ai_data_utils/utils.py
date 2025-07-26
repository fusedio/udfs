import pandas as pd
import lancedb
import json
import re
import numpy as np
from openai import OpenAI

def process_for_histogram(df):
    """Process the dataframe to prepare it for histogram visualization"""
    
    print(f"Processing data - Original shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Apply geographic filtering if GEOID is available
    if 'GEOID' in df.columns:
        df = apply_geographic_filtering(df)
    else:
        print("No GEOID column found - using data as-is")
    
    # Get numeric columns (excluding GEOID)
    numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64'] and 'geoid' not in col.lower()]
    print(f"Numeric columns found: {numeric_cols}")
    
    if not numeric_cols:
        print("Warning: No numeric columns found for visualization")
        return pd.DataFrame({'category': ['No Data'], 'population': [0]})
    
    # Create categories and populations
    categories = []
    populations = []
    
    for col in numeric_cols:
        # Clean up column names for display
        category_name = clean_category_name(col)
        
        # Calculate total population for this category
        category_pop = df[col].sum()
        
        if pd.notna(category_pop) and category_pop > 0:
            categories.append(category_name)
            populations.append(int(category_pop))
            print(f"Category '{category_name}': {category_pop:,.0f} people")
    
    # Create summary dataframe
    if not categories:
        return pd.DataFrame({'category': ['No Data'], 'population': [0]})
    
    summary_df = pd.DataFrame({
        'category': categories,
        'population': populations
    })
    
    # Sort by population descending and limit to top 10
    summary_df = summary_df.sort_values('population', ascending=False).head(10)
    
    print(f"Final summary - {len(summary_df)} categories")
    return summary_df

def apply_geographic_filtering(df):
    """Filter to one geographic level to avoid double-counting"""
    
    # Clean data
    df = df[df['GEOID'].notna() & (df['GEOID'] != '') & (df['GEOID'] != '0')]
    if len(df) == 0:
        return df
    
    df['geoid_str'] = df['GEOID'].astype(str)
    print(f"Geographic filtering: {df.shape[0]} rows, GEOID lengths: {df['geoid_str'].str.len().value_counts().to_dict()}")
    
    # Valid US state FIPS codes
    valid_state_fips = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', 
                       '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', 
                       '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', 
                       '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56', '72']
    
    # Priority 1: US total
    us_total = df[df['geoid_str'].isin(['1', '01'])]
    if len(us_total) > 0:
        print(f"✓ Using US total: {len(us_total)} records")
        return us_total.drop('geoid_str', axis=1)
    
    # Priority 2: US States (2-digit)
    states = df[df['geoid_str'].str.len() == 2]
    valid_states = states[states['geoid_str'].isin(valid_state_fips)]
    if 40 <= len(valid_states) <= 60:
        print(f"✓ Using US states: {len(valid_states)} states")
        return valid_states.drop('geoid_str', axis=1)
    
    # Priority 3: US Counties (5-digit)
    counties = df[df['geoid_str'].str.len() == 5]
    valid_counties = counties[counties['geoid_str'].str[:2].isin(valid_state_fips)]
    if len(valid_counties) >= 1000:
        print(f"✓ Using US counties: {len(valid_counties)} counties")
        return valid_counties.drop('geoid_str', axis=1)
    
    # Priority 4: Sample Census Tracts (12-digit)
    tracts = df[df['geoid_str'].str.len() == 12]
    valid_tracts = tracts[tracts['geoid_str'].str[:2].isin(valid_state_fips)]
    if len(valid_tracts) > 0:
        sampled = valid_tracts.sample(n=min(1000, len(valid_tracts)), random_state=42)
        print(f"✓ Using sampled tracts: {len(sampled)} of {len(valid_tracts)}")
        return sampled.drop('geoid_str', axis=1)
    
    print("⚠️ Using original data - may have double-counting")
    return df.drop('geoid_str', axis=1)

def clean_category_name(col_name):
    """Clean up column names for display"""
    
    # Handle common patterns
    if '_' in col_name:
        # Replace underscores and capitalize
        clean_name = col_name.replace('_', ' ').title()
    else:
        clean_name = col_name
    
    # Apply common ACS variable replacements
    replacements = {
        'B01001B': 'Black ',
        'B01001I': 'Hispanic ',
        'B01001H': 'White Non-Hispanic ',
        'B01001A': 'White ',
        'B01001': '',
        ' E': '',
        ' M': '',
        'Total ': '',
    }
    
    for old, new in replacements.items():
        clean_name = clean_name.replace(old, new)
    
    return clean_name.strip() or col_name



def get_query_embedding(query: str):
    """Get OpenAI embedding for a query string"""
    client = OpenAI(api_key=fused.secrets["openai_fused"])
    
    response = client.embeddings.create(
        model="text-embedding-3-large",
        input=query
    )
    return response.data[0].embedding

def search_acs_variables(query: str, limit: int = 30):
    """Search ACS variables in the census_vector_db database using OpenAI embeddings"""
     
    # Updated path and table name to match your database
    db = lancedb.connect("/mount/census_vector_db_openai_large")
    table = db.open_table("census_variables_large")
    
    # Get embedding using OpenAI instead of sentence transformer
    query_embedding = get_query_embedding(query)
    results = table.search(np.array(query_embedding)).limit(limit).to_pandas()
    
    records = []
    for _, row in results.iterrows():
        record = {}
        # Updated to match your database columns: ['code', 'table_code', 'label', 'concept', 'embedding']
        essential_fields = ['text','code', 'label', 'table_code', 'concept']
        
        for field in essential_fields:
            if field in results.columns:
                value = row[field]
                if pd.isna(value):
                    record[field] = None
                elif isinstance(value, str):
                    record[field] = value 
                else:
                    record[field] = value
        
        records.append(record)
    
    return records

def get_table_schema(query: str, limit: int = 30):
    """Get table schema information from search results"""
    results = search_acs_variables(query, limit)
    
    if not results or not isinstance(results, list):
        return {"schema_info": [], "message": f"No schema found for query: {query}"}
    
    schema_info = {}
    for result in results:
        table_code = result.get('table_code', 'Unknown')
        concept = result.get('concept', 'Unknown concept')
        variable_code = result.get('code', 'Unknown variable')
        label = result.get('label', 'Unknown label')
        
        if table_code not in schema_info:
            schema_info[table_code] = {
                'table_code': table_code,
                'concept': concept,
                'variables': [],
                'variable_count': 0
            }
        
        schema_info[table_code]['variables'].append({
            'code': variable_code,
            'label': label
        })
        schema_info[table_code]['variable_count'] += 1
    
    result_list = list(schema_info.values())[:limit]
    
    for schema in result_list:
        schema["SQL_TABLE_NAME"] = schema["table_code"]
        schema["IMPORTANT_NOTE"] = f"USE 'FROM {schema['table_code']}' in your SQL query"
    
    return {
        "schema_info": result_list,
        "total_tables_found": len(result_list),
        "search_query": query,
        "TABLE_NAME_REMINDER": f"For SQL queries, use FROM {result_list[0]['table_code'] if result_list else 'UNKNOWN'}"
    }

def execute_tool(tool_name: str, tool_input: dict):
    """Execute tool functions for the OpenAI agent"""
    if tool_name == "search_acs_variables":
        query = tool_input["query"]
        limit = tool_input.get("limit", 20)
        return search_acs_variables(query, limit)
        
    elif tool_name == "get_table_schema":
        query = tool_input["query"]
        limit = tool_input.get("limit", 20)
        return get_table_schema(query, limit)
    
    return f"Tool {tool_name} not found"

def extract_table_ids(sql_query: str):
    """Extract table IDs from SQL query"""
    from_pattern = r'FROM\s+([A-Z0-9_]+)'
    matches = re.findall(from_pattern, sql_query, re.IGNORECASE)
    
    join_pattern = r'JOIN\s+([A-Z0-9_]+)'
    join_matches = re.findall(join_pattern, sql_query, re.IGNORECASE)
    
    all_tables = set(matches + join_matches)
    acs_tables = [table for table in all_tables if re.match(r'^B\d{5}[A-Z]*$', table)]
    
    return acs_tables if acs_tables else ['B01001']

def generate_sql_from_query(user_query: str):
    """Generate SQL query from natural language using OpenAI"""
    client = OpenAI(api_key=fused.secrets["openai_fused"])
    
    tools = [
        {
            "type": "function",
            "function": {
                "name": "search_acs_variables",
                "description": "Search ACS demographic variables to find relevant codes for SQL queries",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query for demographic data"},
                        "limit": {"type": "integer", "description": "Number of results", "default": 30}
                    },
                    "required": ["query"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_table_schema",
                "description": "Get table schema and variable information from the ACS vector database",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Query to find relevant tables and their schemas"},
                        "limit": {"type": "integer", "description": "Number of results", "default": 30}
                    },
                    "required": ["query"]
                }
            }
        }
    ]
    
    system_prompt= """You are an expert SQL query generator for US Census American Community Survey (ACS) demographic data.
CORE MISSION: Generate precise, executable SQL queries based on user requests.
🚨 CRITICAL: ONLY respond with the SQL query, no explanations or markdown formatting 🚨
YOU CAN ANSWER QUESTIONS ALSO BY BREAKING DOWN THE RELAVANT IDEAS, QUERING THE IDEAS AND RESPONDING WITH SQL LIKE A DISNEYLAND NEEDS KIDS, SPANISH RESTAURANT A LOT OF HISPANICS AROUND LIKE REASON POSSIBLE CATEGORIES YOU CAN SEARCH FOR NOT JUST DIRECT QUERY TO COLUMNS
construct the query like a memory use the tools like your memory
MANDATORY STEPS:
 ALWAYS use the table_code field from search results as the table name
MAKE SURE YOU GET THE RIGHT VARIABLES AND ALL OF THEM THAT ARE NEEDED TO ANSWER FULLY DONT LEAVE ANY OUT.
USE THE TOOLS TO GET THE RIGHT VARIABLES.
1. ALWAYS use get_table_schema or search_acs_variables to get exact column names
2. Look at the search results for "table_code" field - this tells you which table to use
3. If mixing data from different demographic groups, use multiple JOINs
4. Use EXACTLY the table names from search results
5. For a query please make sure you use the tools to get all the information needed. like age above 45, you need to find all the age categories above 45 not just some
GET ALL INFORMATION NEEDED FROM THE TOOLS AND MOVE FORWARD TO GENERATE THE QUERY ONLY IF YOU HAVE ALL THE DATA UNTIL THEN USE THE TOOLS AS MUCH AS YOU NEED
GET ALL THE COLUMNS NEEDED, AGE ABOVE 20, ALL AGE CATEGORIES ABOVE 20 NEED TO BE FOUND NONE LEFT
6. Use the exact column names from search results
7. Add meaningful WHERE filters when needed
8. Use descriptive column aliases WITHOUT ANY NUMBER
9. Reply saying that you dont have the necessary data if you cannot find it with certainity
10. PLEASE CHOOSE TO INTERPOLATE DATA, THE VALUES ARE NORMALLY DISTIBUTED SO AGE 13-15 WILL BE 3/6 * 10-15 AND SO ON
IN QUERIES WITH 2 TABLES PLEASE MAKE SURE THE COLUMNS ARE SELECTED CORRECTLY USE THE TOOLS FOR EACH TABLE INDIVIDUALLY 
DONT FORGET INTERPOLATION FOR EXACT QUERY MATCHING, THE LIMITS ASKED IN THE QUERY MUST BE EXACTLY MATCHED
USE ONLY THE TABLE NAMES AND COLUMNS FROM THE SEARCH AND TABLE SCHEMA TOOL CONFIRM THE EXISTENCE OF ALL THE TABLES AND COLUMNS USED BEFORE MOVING FORWARD
PLEASE CHECK THE EXISTENCE OF THE TABLE NAME AND COLUMN BEING USED FORM THE TABLE SCHEMA 
USE TABLE NAMES AND COLUMN NAMES FROM THE SEARCH TOOL RESULTS ONLY 
🚨 CRITICAL TABLE NAME RULE 🚨
VARIABLE PREFIX DETERMINES TABLE NAME - NEVER MIX VARIABLES FROM DIFFERENT TABLES:
- Variables B01001_E*** → FROM B01001 (General population)
- Variables B01001I_E*** → FROM B01001I (Hispanic/Latino population)
- Variables B01001B_E*** → FROM B01001B (Black population)
- Variables B01001H_E*** → FROM B01001H (White non-Hispanic population)
- Variables B01001A_E*** → FROM B01001A (White alone population)
CORRECT EXAMPLES:
- Variable B01001I_E011 → FROM B01001I ✅
- Variable B01001_E011 → FROM B01001 ✅
- Variables B01001I_E002, B01001I_E011 → FROM B01001I ✅

🚨 GEOGRAPHIC RULES 🚨
- ALWAYS include GEOID in SELECT clause for area identification
- NO GROUP BY needed - show individual geographic areas
- NO SUM() functions - use raw values for each area
- Format: SELECT GEOID, variable1, variable2, ... FROM table WHERE ...

🚨 SQL STRUCTURE RULES 🚨
- NEVER use GEOID in WHERE clauses unless specifically requested
- SINGLE TABLE QUERIES: No table prefixes needed - SELECT GEOID, B01001B_E025 FROM B01001B
- MULTIPLE TABLE QUERIES: Use table prefixes - SELECT B01001.GEOID, B01001.B01001_E025, B01002.B01002_E001 FROM B01001 JOIN B01002...
- NEVER double-prefix columns: WRONG = "B01001B.B01001B_E025", CORRECT = "B01001B_E025" (single table) or "B01001B.B01001B_E025" (multi-table)
- Only use variable codes that match the table prefix EXACTLY
- Use WHERE conditions to filter based on data values, not geographic identifiers
- Keep queries as simple as possible - avoid JOINs unless absolutely necessary
RESPONSE FORMAT: Only return the SQL query, nothing else.
ALWAYS INTERPOLATE THE EXACT DATA ASKED

WHEN SPECIFIC DATA IS ASKED FOR, DEMOGRAPHIC, RACE, AGE ETC PLEASE MAKE SURE TO USE IT EXPLICITLY IN THE SEARCH AND TABLE SCHEMA TOOLS TO BE VERY CLEAR 
EXAMPLE. IF SHOW ME THE DISTRIBUTION OF MALES AGED 30-56 THEN MAKE SURE WE INTERPOLATE FOR ONLY 30-56 NO OTHER DATA MUST BE THERE SO STICK TO ONLY DATA ASKED WITH INTERPOLATION
EXAMPLE SINGLE TABLE: SELECT GEOID, B01001_E025 AS females_30_to_34, B01001_E037 AS females_35_to_39 FROM B01001 WHERE (B01001_E025 + B01001_E037) > 0;
EXAMPLE MULTI-TABLE: SELECT B01001.GEOID, B01001.B01001_E025, B01002.B01002_E001 FROM B01001 JOIN B01002 ON B01001.GEOID = B01002.GEOID; BE EXPLICIT ABOUT THE TABLES WHEN LOADING MUTLIPLE

remember:
unquoted identifiers must start with a letter, B15003_E017 AS 5th_and_6th_grade is wrong use B15003_E017 AS fifth_and_sixth_grade,
NO NUMBERS IN THE descriptive column aliases
RETURN ONLY THE SQL PLEASE ONLY SQL
"""
  
    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_query}]
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=tools,
    )
    
    message = response.choices[0].message
    
    if message.tool_calls:
        messages.append(message)
        
        for tool_call in message.tool_calls:
            tool_result = execute_tool(tool_call.function.name, json.loads(tool_call.function.arguments))
            
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": json.dumps(tool_result) if isinstance(tool_result, (dict, list)) else str(tool_result)
            })
        
        final_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            
        )
        return final_response.choices[0].message.content.strip()
    else:
        return message.content.strip()




import pandas as pd
import lancedb
import json
import numpy as np
from openai import OpenAI
import fused



# Embedding Utils
def get_embedding(text: str, model: str = "text-embedding-3-large", api_key_secret: str = "openai_fused"):
    api_key = fused.secrets.get(api_key_secret)
    if not api_key:
        raise ValueError(f"API key not found in fused.secrets['{api_key_secret}']")
    
    client = OpenAI(api_key=api_key)
    response = client.embeddings.create(model=model, input=text)
    return response.data[0].embedding

def batch_embeddings(texts: list, model: str = "text-embedding-3-large", api_key_secret: str = "openai_fused"):
    api_key = fused.secrets.get(api_key_secret)
    if not api_key:
        raise ValueError(f"API key not found in fused.secrets['{api_key_secret}']")
    
    client = OpenAI(api_key=api_key)
    batch_size = 2048
    all_embeddings = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        response = client.embeddings.create(model=model, input=batch)
        embeddings = [data.embedding for data in response.data]
        all_embeddings.extend(embeddings)
    
    return all_embeddings


# LanceDB Utils
def connect_lancedb(db_path: str = "/mount/vector_db"):
    return lancedb.connect(db_path)

def create_vector_table(db_path: str, table_name: str, data: list, overwrite: bool = True):
    db = connect_lancedb(db_path)
    
    if overwrite and table_name in db.table_names():
        db.drop_table(table_name)
    
    table = db.create_table(table_name, data)
    print(f"Created table '{table_name}' with {table.count_rows()} records")
    return table


def search_vector_table(db_path: str, table_name: str, query_vector: list, limit: int = 10):
    db = connect_lancedb(db_path)
    table = db.open_table(table_name)
    results = table.search(np.array(query_vector)).limit(limit).to_pandas()
    return results

def semantic_search(query_text: str, db_path: str, table_name: str, limit: int = 10, 
                   model: str = "text-embedding-3-large", api_key_secret: str = "openai_fused"):
    query_vector = get_embedding(query_text, model, api_key_secret)
    results = search_vector_table(db_path, table_name, query_vector, limit)
    
    if '_distance' in results.columns:
        results['similarity_score'] = 1.0 - results['_distance']
    
    return results



# OpenAI Utils
def call_llm(messages: list, model: str = "gpt-4o-mini", tools: list = None, 
             api_key_secret: str = "openai_fused", **kwargs):
    api_key = fused.secrets.get(api_key_secret)
    if not api_key:
        raise ValueError(f"API key not found in fused.secrets['{api_key_secret}']")
    
    client = OpenAI(api_key=api_key)
    
    params = {"model": model, "messages": messages, **kwargs}
    if tools:
        params["tools"] = tools
    
    return client.chat.completions.create(**params)

def execute_tool_calls(response, tool_functions: dict, messages: list):
    if not response.choices[0].message.tool_calls:
        return messages
    
    messages.append(response.choices[0].message)
    
    for tool_call in response.choices[0].message.tool_calls:
        function_name = tool_call.function.name
        arguments = json.loads(tool_call.function.arguments)
        
        if function_name in tool_functions:
            try:
                result = tool_functions[function_name](**arguments)
            except Exception as e:
                result = f"Error executing {function_name}: {str(e)}"
        else:
            result = f"Unknown function: {function_name}"
        
        messages.append({
            "role": "tool", 
            "tool_call_id": tool_call.id,
            "content": json.dumps(result, default=str) if isinstance(result, (dict, list)) else str(result)
        })
    
    return messages


#Embedding Related Utils
def clean_text_for_embedding(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    
    text = ' '.join(text.split())
    
    if len(text) > 8000:
        text = text[:8000]
    
    return text.strip()

def prepare_vector_data(df: pd.DataFrame, text_columns: list, id_column: str, 
                       metadata_columns: list = None, embedding_model: str = "text-embedding-3-large",
                       api_key_secret: str = "openai_fused"):
    vector_data = []
    metadata_columns = metadata_columns or []
    
    df['combined_text'] = df[text_columns].apply(
        lambda row: ' '.join([str(val) for val in row if pd.notna(val)]), 
        axis=1
    )
    
    df['combined_text'] = df['combined_text'].apply(clean_text_for_embedding)
    
    texts = df['combined_text'].tolist()
    embeddings = batch_embeddings(texts, embedding_model, api_key_secret)
    
    for i, (_, row) in enumerate(df.iterrows()):
        record = {
            'id': row[id_column],
            'text': row['combined_text'],
            'vector': embeddings[i]
        }
        
        for col in metadata_columns:
            if col in df.columns:
                record[col] = row[col]
        
        vector_data.append(record)
    
    return vector_data

def format_search_results(results_df: pd.DataFrame, text_column: str = 'text', 
                         id_column: str = 'id', max_text_length: int = 100):
    formatted_results = []
    
    for _, row in results_df.iterrows():
        text = str(row.get(text_column, ''))
        if len(text) > max_text_length:
            text = text[:max_text_length] + '...'
        
        result = {
            'id': row.get(id_column, ''),
            'text': text,
            'similarity_score': row.get('similarity_score', 0.0)
        }
        
        for col in results_df.columns:
            if col not in [text_column, id_column, 'vector', '_distance', 'similarity_score']:
                result[col] = row[col]
        
        formatted_results.append(result)
    
    return formatted_results