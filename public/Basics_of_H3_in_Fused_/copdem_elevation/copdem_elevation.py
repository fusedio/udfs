@fused.udf
def udf(
    # California 
    bounds:fused.types.Bounds=[-125,32,-114,42],
    res: int = 6, 
):
    path = "s3://fused-asset/hex/copernicus-dem-90m/"
    
    hex_reader = fused.load("https://github.com/fusedio/udfs/tree/8024b5c/community/joris/Read_H3_dataset/")
    df = hex_reader.read_h3_dataset(path, bounds, res=res)

    return df