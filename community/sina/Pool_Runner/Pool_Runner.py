# Run a UDF concurrently across an array of parameter-dictionaries. The target UDF should accept and un-stringify (json.loads) a `params` parameter. Pass its UDF's token to this function, along with an array of params.
param_list = [{"param1":10, "param2":True},
               {"param1":20, "param2":False},
              {"param1":20, "param2":False}]
param_json = param_list[0]
@fused.udf
def udf(param_list: dict = param_list):
    import pandas as pd
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    df = fused.run(udf_nail, engine='realtime')
    output = utils.run_pool(lambda x:fused.run(udf_nail,param_json=x, 
                                                            engine='local'), param_list)
    df = pd.concat(output)
    print(df)
    return df

@fused.udf
def udf(param_json: dict = param_json):
    import pandas as pd
    return pd.DataFrame([param_json])
