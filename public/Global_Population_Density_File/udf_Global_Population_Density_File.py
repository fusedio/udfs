@fused.udf
def udf(lat=-10, lng=30, dataset='general', version='1.5.4'):
    from utils import arr_to_plasma
    import rioxarray
    lat2= int(lat//10)*10
    lng2 = int(lng//10)*10
    cog_url = f"s3://dataforgood-fb-data/hrsl-cogs/hrsl_{dataset}/v1.5/cog_globallat_{lat2}_lon_{lng2}_{dataset}-v{version}.tif"
    try:
        rds = rioxarray.open_rasterio(
            cog_url, 
            masked=True,
            overview_level=4
        )
        print('Data is loaded.')
        return arr_to_plasma(rds.values.squeeze()), rds.rio.bounds()
    except:
        print('Error: There is no data for this location.')