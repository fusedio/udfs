@fused.udf
def udf(bbox: fused.types.TileGDF):
    df = simple_fct(input_text='hello')
    print(df)
    
    return 

@fused.cache
def simple_fct(input_text: str = "Me too"):
    import pandas as pd
    
    print(f"{input_text=} | from inside cache")
    
    return pd.DataFrame({"I'm a dumb return": [input_text]})