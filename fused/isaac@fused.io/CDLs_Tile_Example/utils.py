import fused
read_tiff = fused.public.utils.read_tiff
def filter_crops(arr, crop_type, verbose=True):
        import numpy as np
        values_to_keep = crop_to_int(crop_type, verbose=verbose)
        if len(values_to_keep)>0:
            mask = np.isin(arr, values_to_keep, invert=True)
            arr[mask] = 0
            return arr
        else:
            print(f'{crop_type=} was not found in the list of crops')
            return arr

def crop_counts(arr):
    import numpy as np
    import pandas as pd
    import requests
    unique_values, counts = np.unique(arr, return_counts=True)
    df = pd.DataFrame(counts, index=unique_values,columns=['n_pixel']).sort_values(by='n_pixel', ascending=False)
    url = 'https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json'
    df_meta = pd.DataFrame(requests.get(url).json()['summaries']['eo:bands'][0]['gee:classes'])
    df = df_meta.set_index('value').join(df)[['description','color','n_pixel']]
    df['color']='#'+df['color']
    df.columns=['crop_type','color','n_pixel']
    df = df.sort_values('n_pixel', ascending=False)
    return df.dropna()

def crop_to_int(crop_type='corn', verbose=True):
    import pandas as pd
    import requests
    url = 'https://storage.googleapis.com/earthengine-stac/catalog/USDA/USDA_NASS_CDL.json'
    df_meta = pd.DataFrame(requests.get(url).json()['summaries']['eo:bands'][0]['gee:classes'])
    mask = df_meta.description.map(lambda x:crop_type.lower() in x.lower())
    df_meta_masked = df_meta[mask]
    if verbose:
        print(f'{df_meta_masked=}')
        print('crop type options', df_meta.description.values)
    return df_meta_masked.value.values