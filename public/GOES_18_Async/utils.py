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
        roi_wkt=params['roi_wkt']
        crs=params['crs']
        partition_str=params['partition_str']
        udf_name=params['udf_name']
        email=params['email']
        colormap=params['colormap']
        min_pixel_value=params['min_pixel_value']
        max_pixel_value=params['max_pixel_value']
        max_pixel_value
        return fused.utils.run_file(email, udf_name, product_name=product_name, 
                                        i=i, datestr=datestr, colormap=colormap, min_pixel_value=min_pixel_value, max_pixel_value=max_pixel_value, band=band, roi_wkt=roi_wkt, crs=crs, partition_str=partition_str)
    except: return None
def run_batch(datestr, start_i, end_i, partition_str, roi_wkt, crs, udf_name, email, colormap, min_pixel_value, max_pixel_value, band=8, product_name='ABI-L2-CMIPF'):
    arg_list=[{'i':i, 'datestr':datestr, 'band':band, 'product_name':product_name, 'crs':crs, 'roi_wkt':roi_wkt, 'partition_str':partition_str, 'udf_name':udf_name, 'email':email, 'colormap':colormap,"min_pixel_value":min_pixel_value, "max_pixel_value":max_pixel_value} 
            for i in range(start_i, end_i)]
    return run_async(runner, arg_list)