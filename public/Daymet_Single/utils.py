def read_module(url, remove_strings=[]):
    import requests
    content_string = requests.get(url).text
    if len(remove_strings)>0:
        for i in remove_strings:
            content_string = content_string.replace(i,'')
    module = {}
    exec(content_string, module)
    return module

def run_async(fn, arr_args, delay=0, max_workers=32):
    import asyncio
    import nest_asyncio
    nest_asyncio.apply()
    import numpy as np
    import concurrent.futures
    loop = asyncio.get_event_loop()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    async def fn_async(pool, fn, *args):
        try:
            
                result = await loop.run_in_executor(pool, fn, *args)
                return result
        except OSError as error:
            print(f"Error: {error}")
            return None
   
    async def fn_async_exec(fn, arr, delay):
        tasks = []
        await asyncio.sleep(delay*np.random.random())
        if type(arr[0])==list or type(arr[0])==tuple:
            pass
        else:
            arr = [[i] for i in arr]
        for i in arr:
            tasks.append(fn_async(pool,fn,*i))
        return await asyncio.gather(*tasks)

    return loop.run_until_complete(fn_async_exec(fn, arr_args, delay))
