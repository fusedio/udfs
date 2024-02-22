import fused
def run_async(fn, arr_args):
    import asyncio
    import nest_asyncio
    nest_asyncio.apply()    
    a=[]
    for i in arr_args:
        a.append(asyncio.to_thread(fn, i))
    async def main():
        return await asyncio.gather(*a)
    return asyncio.run(main())

def runner(params):
    try:
        i=params['i']
        datestr=params['datestr'] 
        band=params['band']
        product_name=params['product_name']
        return fused.utils.run_file("sina@fused.io", "geos_18_minute_team", product_name=product_name, 
                                        i=i, datestr=datestr, band=band)
    except: return None
def frame_cache(datestr, start_i, end_i, band=8, product_name='ABI-L2-CMIPF'):
    arg_list=[{'i':i, 'datestr':datestr, 'band':band, 'product_name':product_name} 
            for i in range(start_i, end_i)]
    return run_async(runner, arg_list)