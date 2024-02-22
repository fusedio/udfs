@fused.udf
def udf(datestr='2024-02-05', start_i=0, end_i=6 , band=8, product_name='ABI-L2-CMIPF'):
    start_i=int(start_i); end_i=int(end_i); band=int(band);
    from utils import frame_cache
    import pandas as pd
    import numpy as np
    frames = frame_cache(datestr, start_i, end_i, band=band, product_name=product_name)
    frames_raw=[i.image.values[1:-1,1:-1] for i in frames if i is not None]
    print(len(frames_raw))
    frames_all = np.stack(frames_raw)
    df = pd.DataFrame({'arr': [frames_all.flatten()]})
    df['shape'] = [frames_all.shape]
    return df
## how to use: 
# df = fused.utils.run_file("sina@fused.io", "geos_18_async", )
# arr = df['arr'].apply(lambda x: x.reshape(df['shape'][0]))[0]