@fused.udf
def udf(name: str = "Changelog viewers!"):
    import pandas as pd, random, datetime as dt
    flairs = random.sample(["WOW", "YAY", "NICE", "COOL", "AWESOME"], 5)
    return pd.DataFrame({"hello": [f"{f} {name}!" for f in flairs]})