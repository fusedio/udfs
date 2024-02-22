import pandas as pd
import geopandas as gpd
import fused

@fused.cache
def get_counties():
    counties = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_20m.zip')
    counties['COUNTY_FIPS'] = counties.apply(lambda x: f"{x['STATEFP']:02}{x['COUNTYFP']:03}", axis=1)
    return counties[['geometry', 'COUNTY_FIPS', 'NAME']]

@fused.cache
def get_usda_crops(short: str | None = None):
    usda_crops = pd.read_parquet('s3://fused-asset/misc/crop/2017_cdqt_data.crops.parquet')
    usda_crops = usda_crops[~usda_crops['COUNTY_NAME'].isna()]
    usda_crops['COUNTY_FIPS'] = usda_crops.apply(lambda x: f"{int(x['STATE_FIPS_CODE']):02}{int(x['COUNTY_CODE']):03}", axis=1)

    if short:
        # print(f'short options:{usda_crops["SHORT_DESC"].unique()}')
        usda_crops = usda_crops[usda_crops['SHORT_DESC'] == short]
        
    usda_crops = usda_crops[usda_crops['CENSUS_TABLE'] == 25]
    usda_crops = usda_crops[usda_crops['VALUE'] != '(D)']
    usda_crops['VALUE'] = usda_crops['VALUE'].apply(lambda x: float(x.replace(',', '')))
    
    return usda_crops[['VALUE', 'COUNTY_FIPS']]
   
@fused.cache
def get_billionton_county():
    billionton = pd.read_csv('s3://fused-asset/misc/crop/billionton_county_download20240209-071405.csv')
    billionton['COUNTY_FIPS'] = billionton['County Fips Code'].apply(lambda x: f"{x:05}")
    return billionton[['Scenario', 'Biomass Price', 'Resource','Production Density', 'COUNTY_FIPS']]

@fused.cache
def get_epa_ozone_8h():
    df = gpd.read_file('https://www3.epa.gov/airquality/greenbook/shapefile/ozone_8hr_2015std_naa_shapefile.zip')
    return df[['AREA_NAME','geometry']]
