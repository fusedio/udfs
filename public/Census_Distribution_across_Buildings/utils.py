import geopandas as gpd
import concurrent.futures
import fused
import geopandas as gpd
import shapely
from shapely import Point, Polygon
import h3

# Function to Geometry to H3 polygon from centroid location
def geometry_to_hexagon(geom, resolution=10):
    centroid = geom.centroid
    h3_index = h3.latlng_to_cell(centroid.y, centroid.x, resolution)
    hex_boundary = h3.cell_to_boundary(h3_index)
    return Polygon([(b[1], b[0]) for b in hex_boundary])

# Converting Overture Maps Buildings to Hexagons
@fused.cache
def get_buildings_h3(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-03-12-alpha-0",
    resolution: int = 10
):
    import pandas as pd
    from shapely.geometry import box

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    theme = "buildings"
    overture_type = "building"
    min_zoom = 12
    num_parts = 5

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return utils.table_to_tile(
                bbox, table=part_path, min_zoom=min_zoom
            )
        except ValueError:
            return None

    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        dfs = [get_part(0)]

    dfs = [df for df in dfs if df is not None]

    if len(dfs):
        gdf = pd.concat(dfs)
    else:
        print("Failed to get any data")
        return None

    hex_polygons = []

    if 'geometry' in gdf.columns:
        gdf['hexagon'] = gdf['geometry'].apply(geometry_to_hexagon)
        hex_gdf = gpd.GeoDataFrame(gdf, geometry='hexagon', crs='epsg:4326')
    else:
        hex_gdf = gpd.GeoDataFrame(gdf)

    return hex_gdf


# Census Data UDF Functions
@fused.cache 
def acs_5yr_bbox(bounds, census_variable='population', suffix='simplify_01', year=2022):
    if int(year) not in (2021, 2022):
        raise ValueError('The only available years are 2021 and 2022')
    
    import shapely  
    import geopandas as gpd 
    bbox = gpd.GeoDataFrame(geometry=[shapely.box(*bounds)], crs=4326)
    table_to_tile = fused.utils.common.table_to_tile
    fused.utils.common.import_env()
    tid = search_title(census_variable)  
    df = acs_5yr_table(tid, year=year)
    df['GEOID'] = df.GEO_ID.map(lambda x: x.split('US')[-1])
    df = df[['GEOID'] + [i for i in df.columns if '_E' in i]]
    name_dict = acs_5yr_meta(short=False).set_index('Unique ID').to_dict()['Full Title']
    df.columns = ['GEOID'] + [name_dict[i.replace('_E', "_")] for i in df.columns[1:]]
    df = df.rename(columns={df.columns[1]: 'cnt'})  # Rename population column to 'cnt'
    table_path = 's3://fused-asset/infra/census_bg_us'
    print(df['GEOID'] ,  "there are the geoIDS")
    if suffix:
        table_path += f'_{suffix}'
    print("meow", df.columns)
    gdf = table_to_tile(bbox, table_path, use_columns=['GEOID', 'geometry'], min_zoom=12)
    gdf['h3_index'] = gdf['geometry'].apply(lambda x: h3.latlng_to_cell(x.centroid.y, x.centroid.x, 11))
    print(gdf)


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
    print(df_tables2, df_tables)
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


import geopandas as gpd

@fused.cache
def get_census(bbox, census_variable='Total Pop', scale_factor=200, is_density=True, year=2022):
    from utils import acs_5yr_bbox
    import h3
    import shapely
    from shapely import Point, Polygon
    #different geometry details per zoom level
    if bbox.z[0]>12:
        suffix=None
    elif bbox.z[0]>10:
        suffix='simplify_0001'
    elif bbox.z[0]>8:
        suffix='simplify_001'
    elif bbox.z[0]>5:
        suffix='simplify_01'
    else:
        suffix='centroid'
    print(suffix)

    #read the variables
    gdf=acs_5yr_bbox(bbox.total_bounds, census_variable=census_variable, year=year)
    if len(gdf)==0:
        return None
    
    #shorten the column name
    gdf.columns = gdf.columns.map(lambda x:(str(x.split('|')[0])+str(x.split('|')[-1])) if '|' in x else x)
    print(gdf.columns)


    def geometry_to_hexagon(geom):
        centroid = geom.centroid
        h3_index = h3.latlng_to_cell(centroid.y, centroid.x, 10)
        hex_boundary = h3.cell_to_boundary(h3_index)
        return Polygon([(b[1], b[0]) for b in hex_boundary])

    if 'geometry' in gdf.columns:
        gdf['hexagon'] = gdf['geometry'].apply(geometry_to_hexagon)
        hex_gdf = gpd.GeoDataFrame(gdf, geometry='hexagon', crs='epsg:4326')
    else:
        hex_gdf = gpd.GeoDataFrame(gdf)


    return hex_gdf



