@fused.udf
def udf(name: str = "world"):
    import pandas as pd

    return pd.DataFrame({"hello": [name]})

def download_file(url, destination):
    import requests

    try:
        response = requests.get(url)
        with open(destination, "wb") as file:
            file.write(response.content)
        return f"File downloaded to '{destination}'."
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"
        
@fused.cache
def run_pool_tiffs(bounds, df_tiffs, output_shape):
    import numpy as np

    columns = df_tiffs.columns

    @fused.cache
    def fn_read_tiff(tiff_url, bounds=bounds, output_shape=output_shape):
        read_tiff = fused.load("https://github.com/fusedio/udfs/tree/364f5dd/public/common/").read_tiff
        return read_tiff(bounds, tiff_url, output_shape=output_shape)

    tiff_list = []
    for band in columns:
        for i in range(len(df_tiffs)):
            tiff_list.append(df_tiffs[band].iloc[i])

    arrs_tmp = run_pool(fn_read_tiff, tiff_list)
    arrs_out = np.stack(arrs_tmp)
    arrs_out = arrs_out.reshape(
        len(columns), len(df_tiffs), output_shape[0], output_shape[1]
    )
    return arrs_out

def tiff_bbox(url):
    import rasterio
    import shapely
    import geopandas as gpd
    with rasterio.open(url) as dataset:
        gpd.GeoDataFrame(geometry=[shapely.box(*dataset.bounds)],crs=dataset.crs)
        return list(dataset.bounds)

def read_module(url, remove_strings=[]):
    import requests

    content_string = requests.get(url).text
    if len(remove_strings) > 0:
        for i in remove_strings:
            content_string = content_string.replace(i, "")
    module = {}
    exec(content_string, module)
    return module
    
class AsyncRunner:
    '''
    ## Usage example:
    async def fn(n): return n**2
    runner = AsyncRunner(fn, range(10))
    runner.get_result_now()
    '''
    def __init__(self, func, args_list, delay_second=0, verbose=True):
        import asyncio
        if isinstance(args_list, pd.DataFrame):
            self.args_list=args_list.T.to_dict().values()
        elif isinstance(args_list, list) or isinstance(args_list, range):     
            self.args_list=args_list
        else:
            raise ValueError('args_list need to be list, pd.DataFrame, or range')
        self.func = func
        self.verbose = verbose
        self.delay_second = delay_second
        self.loop = asyncio.get_running_loop()
        self.run_async()
    
    def create_task(self, args):
        import time
        import json
        time.sleep(self.delay_second)
        if type(args)==str:
            args=json.loads(args)
        if isinstance(args, dict):
            task = self.loop.create_task(self.func(**args))
        else:
            task = self.loop.create_task(self.func(args))
        task.set_name(json.dumps(args))
        return task
        
    def run_async(self):
        tasks = []
        for args in self.args_list:
            tasks.append(self.create_task(args))
        self.tasks=tasks
    
    def is_done(self):
        return [task.done() for task in self.tasks]
    
    def get_task_result(self, r):
        if r.done():
            import pandas as pd
            try:
                return r.result()
            except Exception as e:
                return str(e)
        else:
            return 'pending'
        
    def get_result_now(self, retry=True):
        if retry:
            self.retry()
        if self.verbose:
            print(f"{sum(self.is_done())} out of {len(self.is_done())} are done!")
        import json
        import pandas as pd
        df = pd.DataFrame([json.loads(task.get_name()) for task in self.tasks])
        df['result']= [self.get_task_result(task) for task in self.tasks]
        def fn(r):
            if type(r)==str:
                if r=='pending':
                    return 'running'
                else:
                    return 'faild'
            else:
                return 'done'
        df['status']=df['result'].map(fn)
        return df            
    
    def retry(self):
        def _retry_task(task, verbose):
            if task.done():
                task_exception = task.exception()
                if task_exception:
                    if verbose: print(task_exception)
                    return self.create_task(task.get_name()) 
                else:
                    return task
            else:
                return task            
        self.tasks = [_retry_task(task, self.verbose) for task in self.tasks]
    
    async def get_result_async(self):
        import asyncio
        return await asyncio.gather(*self.tasks)

    def __repr__(self):
        if self.verbose:
            print(f'tasks_done={self.is_done()}')
        if (sum(self.is_done())/len(self.is_done()))==1:
            return f"done!"
        else:
            return "running..."
    
class PoolRunner:
    '''
    ## Usage example:
    def fn(n): return n**2
    runner = PoolRunner(fn, range(10))
    runner.get_result_now()
    runner.get_result_all()
    '''
    def __init__(self, func, args_list, delay_second=0.01, verbose=True, max_retry=3):
        import asyncio
        import pandas as pd
        import concurrent.futures
        if isinstance(args_list, pd.DataFrame):
            self.args_list=args_list.T.to_dict().values()
        elif isinstance(args_list, list) or isinstance(args_list, range):     
            self.args_list=args_list
        else:
            raise ValueError('args_list need to be list, pd.DataFrame, or range')
        self.func = func
        self.delay_second = delay_second
        self.verbose = verbose
        self.max_retry=max_retry
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1024)
        self.run_pool()
        self.result=[]

    def create_task(self, args, n_retry):
        import time
        time.sleep(self.delay_second)
        if isinstance(args, dict):
            task = self.pool.submit(self.func, **args)
        else:
            task = self.pool.submit(self.func, args)
        return [task, args, n_retry]
        
    def run_pool(self):
        tasks = []
        for args in self.args_list:
            tasks.append(self.create_task(args, n_retry=0))
        self.tasks=tasks
    
    def is_success(self):
        return [task[0].done() for task in self.tasks]
    
    def get_task_result(self, task):
        if task[0].done() or task[2]>self.max_retry:
            try:
                return task[0].result()
            except Exception as e:
                if task[2]<self.max_retry: 
                    return str(e)
                else:
                    return f'Exceeded max_retry ({task[2]-1}|{self.max_retry}): '+str(e)
        else:
            # if task[2]<self.max_retry: 
                return f'pending'
            # else:
            #     return f'Exceeded max_retry'
        
    def get_result_now(self, retry=True, sleep_second=1):
        if retry:
            self.retry()
        else:
            import time
            time.sleep(sleep_second)
        if self.verbose:
            n1=sum(self.is_success())
            n2=len(self.is_success())
            print(f"\r{n1/n2*100:.1f}% ({n1}|{n2}) complete", end='\n')
        import json
        import pandas as pd
        df = pd.DataFrame([task[1] for task in self.tasks])
        self.result=[self.get_task_result(task) for task in self.tasks]
        df['result']=self.result
        def fn(r):
            if type(r)==str:
                if str.startswith(r, 'Exceeded max_retry'):
                    return 'error'
                else:#elif r=='pending':
                    return 'running'#'error_retry'
            else:
                return 'success'
        df['status']=df['result'].map(fn)
        return df            
    
    def retry(self):
        def _retry_task(task, verbose):
            if task[0].done():
                task_exception = task[0].exception()
                if task_exception:
                    if verbose: 
                        print(task_exception)
                    if (task[2]<self.max_retry):
                        if verbose: 
                            print(f'Retry {task[2]+1}|{self.max_retry} for {task[1]}.')
                        return self.create_task(task[1], n_retry=task[2]+1) 
                    else:
                        if verbose: 
                            print(f'Exceeded Max retry {self.max_retry} for {task[1]}.')
                        return [task[0], task[1], task[2]+1]
                else:
                    return task
            else:
                return task            
        self.tasks = [_retry_task(task, self.verbose) for task in self.tasks]
    
    def get_result_all(self, timeout=120):
        import time
        for i in range(timeout):
            df=self.get_result_now(retry=True)
            # if (df.status=='success').mean()==1:
            if (df.status=='running').mean()==0:
                break
            else:
                time.sleep(1)
        if self.verbose:
            print(f"Done!")
        return df

    def get_error(self, sleep_second=3):
        df=self.get_result_now(retry=False, sleep_second=sleep_second)
        if self.verbose:
            print('Status Summary:', df.status.value_counts().to_dict())
            error_mask=df.status=='error'
            if sum(error_mask)==0:
                print('No error.')
            else:
                df_error = df[error_mask]
                for i in range(len(df_error)): 
                    print(f'\nROW: {i} | ERROR {"="*30}')
                    for col in df_error.columns:  
                        print(f'{str(col).upper()}: {df_error.iloc[i][col]}')
        return df
    def get_concat(self, sleep_second=0, verbose=None):
        if verbose != None:
            self.verbose=verbose
        import pandas as pd
        df = self.get_error(sleep_second=sleep_second)
        mask=df.status=='success'
        if mask.sum()==0:
            return 
        else:
            results=[i for i in df[mask].result if i is not None]
            if self.verbose:
                print(f"{100*mask.mean().round(3)} percent success.")
                if len(results)!=mask.sum():
                    print(f"Warnning: {mask.sum()-len(results)}/{mask.sum()} are None values.")
        return pd.concat(results)
    def __repr__(self):
        if self.verbose:
            print(f'tasks_success={self.is_success()}')
        if (sum(self.is_success())/len(self.is_success()))==1:
            return f"success!"
        else:
            return "running..."
            

def run_submit_with_defaults(udf_token, cache_length="9999d", default_params_token=None):
    """
    Uses fused.submit() to run a UDF over:
    - A UDF that returns a pd.DataFrame of test arguments (`default_params_token`)
    - Or default params (expectes udf.utils.submit_default_params to return a pd.DataFrame)
    """
    
    # Assume people know what they're doing 
    try:
        # arg_token is a UDF that returns a pd.DataFrame of test arguments
        arg_list = fused.run(default_params_token)

        if 'bounds' in arg_list.columns:
            # This is a hacky workaround for now as we can't pass np.float bounds to `fused.run(udf, bounds) so need to convert them to float
            # but fused.run() returns bounds as `np.float` for whatever reason
            arg_list['bounds'] = arg_list['bounds'].apply(lambda bounds_list: [float(x) for x in bounds_list])
            
        print(f"Loaded default params from UDF {default_params_token}... Running UDF over these")
    except Exception as e:
        print(f"Couldn't load UDF {udf_token} with arg_token {default_params_token}, trying to load default params...")
        
        try:
            udf = fused.load(udf_token)
            
            # Assume we have a funciton called 'submit_default_params` inside the main UDF which returns a pd.DataFrame of test arguments
            # TODO: edit this to directly use `udf.submit_default_params()` once we remove utils
            if hasattr(udf.utils, "submit_default_params"):
                print("Found default params for UDF, using them...")
                arg_list = udf.utils.submit_default_params()
            else:
                raise ValueError("No default params found for UDF, can't run this UDF")

        except Exception as e:
            raise ValueError("Couldn't load UDF, can't run this UDF. Try with another UDF")
        
        #TODO: Add support for using the default view state

    return fused.submit(
        udf_token,
        arg_list,
        cache_max_age=cache_length,
        wait_on_results=True,
    )

def test_udf(udf_token, cache_length="9999d", arg_token=None):
    """
    Testing a UDF:
    1. Does it run and return successful result for all its default parameters?
    2. Are the results identical to the cached results?

    Returns:
    - all_passing: True if the UDF runs and returns successful result for all its default parameters
    - all_equal: True if the results are identical to the cached results
    - prev_run: Cached UDF output
    - current_run: New UDF output
    """
    import pickle

    cached_run = run_submit_with_defaults(udf_token, cache_length, arg_token)
    current_run = run_submit_with_defaults(udf_token, "0s", arg_token)

    # Check if results are valid
    all_passing = (current_run["status"] == "success").all()
    # Check if result matches cached result
    all_equal = pickle.dumps(cached_run) == pickle.dumps(current_run)
    return (bool(all_passing), all_equal, cached_run, current_run)

  
def save_to_agent(
    agent_json_path, udf, agent_name, udf_name, mcp_metadata, overwrite=True,
):
    import json
    import os
    """
    Save UDF to agent of udf_ai directory
    Args:
        agent_json_path (str): Absolute path to the agent.json file
        agent_name (str): Name of the agent
        udf (AnyBaseUdf): UDF to save
        udf_name (str): Name of the UDF
        mcp_metadata (dict[str, Any]): MCP metadata
        overwrite (bool): If True, overwrites any existing UDF directory with current `udf`
    """
    # load agent.json
    if os.path.exists(agent_json_path):
        agent_json = json.load(open(agent_json_path))
    else:
        agent_json = {"agents": []}
    repo_dir = os.path.dirname(agent_json_path)

    # save udf to repo
    udf.metadata = {}
    udf.name = udf_name
    if not mcp_metadata.get("description") or not mcp_metadata.get("parameters"):
        raise ValueError("mcp_metadata must have description and parameters")
    udf.metadata["fused:mcp"] = mcp_metadata
    udf.to_directory(f"{repo_dir}/udfs/{udf_name}", overwrite=overwrite)

    if agent_name in [agent["name"] for agent in agent_json["agents"]]:
        for agent in agent_json["agents"]:
            if agent["name"] == agent_name:
                # Only append udf_name if it doesn't already exist in the agent's udfs list
                if udf_name not in agent["udfs"]:
                    agent["udfs"].append(udf_name)
                break
    else:
        agent_json["agents"].append({"name": agent_name, "udfs": [udf_name]})

    # save agent.json
    json.dump(agent_json, open(agent_json_path, "w"), indent=4)

def generate_local_mcp_config(config_path, agents_list, repo_path, uv_path='uv', script_path='main.py'):
    """
    Generate MCP configuration file based on list of agents from the udf_ai directory
    Args:
        config_path (str): Absolute path to the MCP configuration file.
        agents_list (list[str]): List of agent names to be included in the configuration.
        repo_path (str): Absolute path to the locally cloned udf_ai repo directory.
        uv_path (str): Path to `uv`. Defaults to `uv` but might require your local path to `uv`.
        script_path (str): Path to the script to run. Defaults to `run.py`.
    """
    if not os.path.exists(repo_path):
        raise ValueError(f"Repository path {repo_path} does not exist")

    # load agent.json containing agent to udfs mapping
    agent_json = json.load(open(f"{repo_path}/agents.json", "rt"))

    # create config json for all agents
    config_json = {"mcpServers": {}}

    for agent_name in agents_list:
        agent = next(
            (agent for agent in agent_json["agents"] if agent["name"] == agent_name),
            None,
        )
        if not agent or not agent["udfs"]:
            raise ValueError(f"No UDFs found for agent {agent_name}")

        agent_config = {
            "command": uv_path,
            "args": [
                "run",
                "--directory",
                f"{repo_path}",
                f"{script_path}",
                "--runtime=local",
                f"--udf-names={','.join(agent['udfs'])}",
                f"--name={agent_name}",
            ],
        }
        config_json["mcpServers"][agent_name] = agent_config

    # save config json
    json.dump(config_json, open(config_path, "w"), indent=4)

def get_query_embedding(client, query, model="text-embedding-3-large"):
    """Generate embeddings for a query using OpenAI API
    client = OpenAI(api_key=fused.secrets["my_fused_key"])"""
    embedding = list(map(float, client.embeddings.create(
        input=query, model=model
    ).data[0].embedding))
    return embedding

def query_to_params(query, num_match=1, rerank=True, embedding_path="s3://fused-users/fused/misc/embedings/CDL_crop_name.parquet"):
    import fused
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


def submit_job(udf, df_arg, cache_max_age='12h'):
    import fused
    import pandas as pd
    udf_nail_json = udf_to_json(udf)
    
    #TODO: fix dill issue in local machine
    # def runner(args: dict, udf_nail_json: str):
    #     udf_nail = fused.models.udf.udf.GeoPandasUdfV2.parse_raw(udf_nail_json)
    #     return fused.run(udf_nail, **args, engine="local")
    # runner = func_to_udf(runner)
    runner = fused.load('@fused.udf(cache_max_age="' + cache_max_age + '")\n' + 
    '''def udf(args:dict, udf_nail_json:str):
    udf_nail = fused.models.udf.udf.GeoPandasUdfV2.parse_raw(udf_nail_json)
    return fused.run(udf_nail, ** args, engine='local')
    ''')
    arg_list = df_arg.to_dict(orient="records")
    job = runner(arg_list=arg_list, udf_nail_json=udf_nail_json)
    return job
