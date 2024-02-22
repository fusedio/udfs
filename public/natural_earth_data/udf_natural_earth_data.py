#ToDo: Do zonal stats
#Ref: https://www.naturalearthdata.com/downloads/10m-cultural-vectors/
@fused.udf 
def udf(bbox=None, country_name=""):
    import geopandas as gpd
    import fused
    path = fused.utils.download('https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip', 'file.zip')
    df = gpd.read_file(path).explode() 
    mask = df['NAME'].map(lambda x:country_name.lower() in x.lower())
    if mask.sum()>0:
        df = df[mask]
    else:
        print(f'{country_name} not in the dataset.')
    return df