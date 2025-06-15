@fused.udf
def udf(path: str, preview: bool=False):
    path = fused.download(path,path)
    import json
    with open(path, 'r') as f:
        data = json.load(f)
    return data
    