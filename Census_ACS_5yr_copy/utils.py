import fused
@fused.cache 
def acs_5yr_bbox(bounds, census_variable='population', suffix='simplify_01', year=2022):
    if int(year) not in (2021, 2022):
        raise ValueError('The only available years are 2021 and 2022')
    import shapely  
    import geopandas as gpd 
    bbox = gpd.GeoDataFrame(geometry=[shapely.box(*bounds)], crs=4326)
    table_to_tile = fused.utils.common.table_to_tile
    fused.utils.common.import_env()
    tid=search_title(census_variable)  
    df = acs_5yr_table(tid, year=year)
    df['GEOID'] = df.GEO_ID.map(lambda x:x.split('US')[-1])
    df = df[['GEOID']+[i for i in df.columns if '_E' in i]]
    name_dict = acs_5yr_meta(short=False).set_index('Unique ID').to_dict()['Full Title']
    # print(name_dict)
    df.columns = ['GEOID']+[name_dict[i.replace('_E',"_")] for i in df.columns[1:]]    
    table_path='s3://fused-asset/infra/census_bg_us'
    if suffix:
        table_path+=f'_{suffix}'
    print(table_path)
    gdf = table_to_tile(bbox, table_path, use_columns=['GEOID','geometry'], min_zoom=12)
    if len(gdf)>0:
        gdf2 = gdf.merge(df)
        return gdf2
    else:
        print('No geometry is intersecting with the given bbox.')
        return gpd.GeoDataFrame({})
    
@fused.cache
def acs_5yr_meta(short=True): 
    import pandas as pd
    #Filter only records with cencus block groups data
    tmp = pd.read_excel('https://www2.census.gov/programs-surveys/acs/summary_file/2021/sequence-based-SF/documentation/tech_docs/ACS_2021_SF_5YR_Appendices.xlsx')
    table_ids_cbgs = tmp[tmp['Geography Restrictions'].isna()]['Table Number']
    #Get the list of tables and filter by only totals (the first row of each table)
    df_tables = pd.read_csv('https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/ACS20225YR_Table_Shells.txt', delimiter='|')
    if short:
        df_tables2 = df_tables.drop_duplicates('Table ID')
    else:
        df_tables2 = df_tables
        df_tables2['Full Title']=df_tables2['Label']+' | '+df_tables2['Title']+' | '+df_tables2['Unique ID']
    df_tables2 = df_tables2[df_tables2['Table ID'].isin(table_ids_cbgs)]
    return df_tables2 


@fused.cache
def acs_5yr_table(tid, year=2022):
    import pandas as pd
    url=f'https://www2.census.gov/programs-surveys/acs/summary_file/{year}/table-based-SF/data/5YRData/acsdt5y{year}-{tid.lower()}.dat'
    return pd.read_csv(url, delimiter='|')

def search_title(title):
    df_meta=acs_5yr_meta() 
    #search for title in the list of tables 
    search_column = 'Title' #'Title' #'Topics'
    meta_dict = df_meta[['Table ID', search_column]].set_index(search_column).to_dict()['Table ID']
    List = [[meta_dict[i], i] for i in meta_dict.keys() if title.lower() in i.lower()]
    print(f'Chosen: {List[0]}\nfrom: {List[:20]}')
    return List[0][0]

