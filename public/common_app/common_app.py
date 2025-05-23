@fused.udf
def udf():
    """common utils for Fused App. Please do not run this as UDF."""
    return


async def async_status(status_label="Running...", end_label=None, st_status=None):
    import streamlit as st
    import asyncio

    if not st_status:
        st_status = st.empty()
    with st_status.status(status_label, expanded=False):
        await asyncio.sleep(0.0025)
    if end_label:
        with st_status.status(end_label, expanded=False):
            pass
    else:
        st_status.empty()


async def async_button(button_label, status_label, end_label=None, st_button=None, st_status=None):
    import streamlit as st

    if not st_button:
        st_button = st.empty()
    if not st_status:
        st_status = st.empty()
    if st_button.button(button_label):
        await async_status(status_label=status_label, end_label=end_label, st_status=st_status)
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
