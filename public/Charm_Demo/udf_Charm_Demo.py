@fused.udf
def udf(short='CORN, GRAIN - ACRES HARVESTED', resource='Corn', price=60, density_min=10):
    price=int(price);density_min=int(density_min)
    import utils
    #Get Data
    counties = utils.get_counties()
    usda_crops = utils.get_usda_crops(short=short)
    billionton = utils.get_billionton_county()
    epa_ozone_8h = utils.get_epa_ozone_8h()

    #Filter data
    billionton = billionton[billionton['Resource'] == resource]
    print(f'Price options:{list(billionton["Biomass Price"].unique())}')
    billionton = billionton[billionton['Biomass Price'] == price]

    #Merge Data
    counties_crops = counties.merge(usda_crops, on='COUNTY_FIPS', how='outer')
    counties_crops_price = counties_crops.merge(billionton, on='COUNTY_FIPS', how='outer', suffixes=['usda', 'billionton'])
    results = counties_crops_price.sjoin(epa_ozone_8h, how='left')
    results = results[~results.geometry.isna()]
    results = results.dropna(subset=['Production Density'])
    results = results.set_index('NAME').drop(columns=['COUNTY_FIPS','index_right'])
    
    #Filter Production Density
    results = results[results['Production Density'].astype(int) > density_min]
    if len(results)==0:
        print('No results. Please expand your filter.')
        return None
    return results