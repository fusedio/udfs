@fused.udf
def inner(val):
    import pandas as pd
    return pd.DataFrame({'val':[val]})

@fused.udf
def udf():
    job = inner(arg_list=[0,1,2,3,4])
    job.run_remote()