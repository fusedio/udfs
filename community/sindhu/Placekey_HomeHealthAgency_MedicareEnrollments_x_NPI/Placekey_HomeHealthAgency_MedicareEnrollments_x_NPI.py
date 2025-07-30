@fused.udf
def udf(bbox: fused.types.Tile, preview: bool=False):
    #import necessary things
    import pandas as pd
    import geopandas as gpd
    import shapely

    #get two placekey'd datasets; choose these based on what's available on the module tab
    df1 = get_placekeyd_dataset(bbox, "national-provider-identifier")
    df2 = get_placekeyd_dataset(bbox, "home-health-agency-medicare-enrollments")

    #join the datasets on placekey
    return placekey_merge(df1, df2)


@fused.cache
def placekey_df_validate(df):
    required_columns = ['placekey', 'address_placekey', 'building_placekey', 'geometry']
    for column in required_columns:
        if column not in df.columns:
            raise ValueError("Dataset doesn't have " + column + ".")

@fused.cache
def get_placekeyd_dataset(bbox, name: str):
    utils = fused.load('https://github.com/fusedio/udfs/tree/19b5240/public/common/').utils
    # pick one of these datasets!!
    datasets = set(['dc-healthy-corner-stores','home-health-agency-medicare-enrollments', 'home-infusion-therapy-provider-medicare-enrollments', 'hospice-medicare-enrollments', 'hospital-medicare-enrollments', 'national-downloadable-files-from-the-doctors-and-clinicians-data-section', 'skilled-nursing-facility-medicare-enrollments', 'rural-health-clinic-medicare-enrollments',
                   'national-provider-identifier', 'department-of-labor-wage-and-hour-compliance'])
    if name in datasets:
        path = 's3://placekey-free-datasets/'+name+'/fused/_sample'
        base_path = path.rsplit('/', maxsplit=1)[0] if path.endswith('/_sample') or path.endswith('/_metadata') else path
        return utils.table_to_tile(bbox, table=base_path, use_columns=None)
    else:
        raise ValueError(name + " is either not available through Placekey or is not onboarded onto fused.io. Contact Placekey if you would like it to be! https://www.placekey.io/contact-sales")    

@fused.cache
def placekey_merge(df1, df2):
    import pandas as pd
    placekey_df_validate(df1)
    placekey_df_validate(df2)
    
    #join the datasets on placekey
    result = df1.merge(df2, on="placekey", how="outer", suffixes=("_1", "_2"))

    #set the colors to visualize the joins. Bring the overlaps to the front
    result['belonging'] = result.apply(in_what, axis=1)
    result = result.sort_values(by='belonging', ascending=False)

    #combine geometries and drop unnecessary ones
    result['geometry'] = result['geometry_1'].combine_first(result['geometry_2'])
    result = result.drop(columns=['geometry_1', 'geometry_2'])
    result.set_geometry("geometry", inplace=True)
    return result

@fused.cache
def in_what(row):
    import pandas as pd
    if pd.isna(row['geometry_1']) and not pd.isna(row['geometry_2']):
        return 'zdf1'
    elif not pd.isna(row['geometry_1']) and pd.isna(row['geometry_2']):
        return 'zdf2'
    else:
        return 'both'