"""
The FCC Broadband map shows every location in the US and lets you query an address.

However, the resolution/granularity of the map make it hard to examine the types of service offered.

Further it is hard to conduct analysis to determine which blocks might be over reported.

This map combines the data at the census block level and lets users query the data.

e.g.

1. Which blocks would be unserved if the definition of broadband changed to 100 down and 100 up.
2. Show me the reported fiber footprint for AT&T
3. Show me the national fixed wireless reported coverage

This data was created with the following approach:

Use Python and DuckDB in Colab notebook and Google Drive storage.

The data is processed to parquet files and then uploaded and ingested
into the Fused platform.

Each step is broken out as a seperate notebook:

1. GetFCCData.ipynb - https://colab.research.google.com/drive/1dDN1v0kXhn_Z-sVuvqQ1wDc_dkTST24r#scrollTo=Vd1DyLS2H9Cf
2. ProcessFCCData.ipynb - https://colab.research.google.com/drive/1E0S3xn5Wtu9ZdvQMf1v9NAi_h2BQO852?usp=sharing
3. MapFCCData.ipynb - https://colab.research.google.com/drive/16zD8trsk3hpICmU4QeFXtLqtHsct1VIC#scrollTo=GxMl-gvJGMA_

"""
@fused.udf
def udf(
    bbox: fused.types.Tile = None,
    use_columns: list = None,
    providers=None, # e.g. ['AT&T'] Filter the data to show only these, a list of strings as shown in the data
    unserved="[25,3]", # The [download speed, upload speed]
    underserved="[100,20]", # The [download speed, upload speed]
    highlight='underserved', # "unserved" or "underserved" Change the color to red to show which blocks meet the above criteria
    exclude_providers=None, # e.g. ['Starlink'] Filter the data to exclude these
    tech_codes="[10,11,12,20,30,40,41,42,43,50]"): # Filter the data to only include these, https://www.fcc.gov/general/technology-codes-used-fixed-broadband-deployment-data
    # 10,11,12,20,30,40,41,42,43,50,60,70,90,0
    import json
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    unserved = json.loads(unserved)
    underserved = json.loads(underserved)
    tech_codes = json.loads(tech_codes)

    table_path = f"s3://fused-asset/data/fcc_bdc_map_latest/"
    table_path = table_path.rstrip("/")
    
    gdf = common.table_to_tile(bbox, table=table_path, use_columns=use_columns, min_zoom=8)

    gdf['brand_info'] = gdf.brand_info.apply(json.loads)

    gdf['brands'] = gdf['brand_info'].apply(lambda x: ', '.join([k for k in x.keys()]))

    if providers is not None:
        gdf['brand_info'] = gdf.brand_info.apply(lambda x: {k: v if k in providers else {'': ''} for k, v in x.items()})
    
    if exclude_providers is not None:
        gdf['brand_info'] = gdf.brand_info.apply(lambda x: {k: v if k not in exclude_providers else {'': ''} for k, v in x.items()})

    if tech_codes is not None:
        test = gdf.brand_info.apply(lambda x: {k: v if len(set(v.get('tech_list', [])) & set([int(t) for t in tech_codes])) > 0 else {'': ''} for k, v in x.items()})
        gdf['brand_info'] = test
    gdf['max_down'] = gdf.brand_info.apply(lambda x: max([0] + [v.get('max_download_speed', 0) for v in x.values()]))
    gdf['max_up'] = gdf.brand_info.apply(lambda x: max([0] + [v.get('max_upload_speed', 0) for v in x.values()]))

    gdf['underserved'] = False
    gdf.loc[(gdf['max_down'] < int(underserved[0]))|(gdf['max_up'] < int(underserved[1])), 'underserved'] = True

    gdf['unserved'] = False
    gdf.loc[(gdf['max_down'] < int(unserved[0]))|(gdf['max_up'] < int(unserved[1])), 'unserved'] = True

    gdf['r'] = 0
    gdf['g'] = 0
    gdf['b'] = 0

    gdf.loc[gdf[highlight] == True, 'r'] = 255

    return gdf[['max_down', 'max_up', 'brands', 'brand_info', 'unserved', 'underserved', 'r', 'g', 'b', 'geometry']]
