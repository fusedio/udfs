
@fused.udf
def udf():
    """common utils for Fused App. Please do not run this as UDF."""
    return 

async def async_button(label, spinner):
        import streamlit as st    
        import asyncio
    
        status = st.empty()
        if st.button(label): 
            with status.status(spinner, expanded=False):
                await asyncio.sleep(0.0025)
            status.empty() 
            return True
        else:
            return False


def to_sync(fn, *args, _wait_second=0.1, **kwargs):
    """
    async def func(x):
        await asyncio.sleep(x)
        return x
    r = to_sync(func, x=2)
    print(r)
    """
    loop = asyncio.get_event_loop()
    f = loop.run_until_complete(fn(*args, **kwargs))
    while not f.done():
        time.sleep(_wait_second)
    return f.result()