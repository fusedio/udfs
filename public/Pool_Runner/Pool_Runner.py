"""
Run a UDF concurrently across an array of parameter-dictionaries.

The target UDF should accept and un-stringify (json.loads) a `params` parameter.
Pass its UDF's token to this function, along with an array of params.
"""
import json

params_json = json.dumps([{"a": 1}, {"b": 2}])
target_token = ""


@fused.udf
def udf(target_token: str = target_token, params_json: str = params_json):
    import json

    import pandas as pd

    params_list = json.loads(params_json)
    print(params_list)

    def fn(params):
        return fused.run(target_token, params=json.dumps(params))

    output = [
        each
        for each in fused.utils.common.run_pool(fn, params_list)
        if each is not None
    ]

    if len(output) == 0:
        return None
    else:
        return pd.concat(output, ignore_index=True)
