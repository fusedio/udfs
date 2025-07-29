@fused.udf
def udf(path: str='s3://fused-asset/misc/jennings/funshot.png', res:int=8):
    import imageio.v3 as iio
    import s3fs
    import pandas as pd
    filepath = s3fs.S3FileSystem().open(path) if path.startswith('s3') else path
    im = iio.imread(filepath)
    transposed_image = im.transpose(2, 0, 1)[:,:]
    bounds=image_to_bound(transposed_image)
    arr = 0.299 * im[:,:,0] + 0.587 * im[:,:,1] + 0.114 * im[:,:,2]
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    df = common.arr_to_h3(arr, bounds=bounds, res=max(min(res,9),6))
    df['value'] = df.agg_data.map(lambda x: pd.Series(x).min())
    del df['agg_data']
    return df

def image_to_bound(image):
    shape = image.shape
    w, h = (shape[-1], shape[-2])
    if w > h:
        return (0, 0, 1, 1 / (w / h))
    else:
        return (0, 0, 1 / (h / w), 1)