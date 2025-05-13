@fused.udf
def udf():
    """common utils for Fused App. Please do not run this as UDF."""
    return 

def button(label='Run Comparison', spinner='spinner....'):
    import streamlit as st
    import time
    status = st.empty()
    if st.button(label):
        with status.status(spinner, expanded=False):
            time.sleep(0.005)
        status.empty()
        return True
    else:
        return False
