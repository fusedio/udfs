@fused.udf
def udf(
    bounds: fused.types.Bounds = [-74.556, 40.400, -73.374, 41.029],  # Default to full NYC
    res: int = None,
):
    path = "s3://fused-asset/hex/copernicus-dem-90m/"
    
    hex_reader = fused.load("https://github.com/fusedio/udfs/tree/8024b5c/community/joris/Read_H3_dataset/")
    df = hex_reader.read_h3_dataset(path, bounds, res=res)

    return df