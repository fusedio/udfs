@fused.udf
def udf(city='Boston', resolution=11):
    import duckdb
    from utils import load_h3_duckdb
    import shapely
    import geopandas as gpd
    import requests
    
    import re


    # set a resolution limit 
    if resolution > 11:resolution=11
    
    city = city.lower().replace(' ','-')
    # downloading main file list
    listings = requests.get(
        "https://insideairbnb.com/get-the-data/"
    )
    url = None
    if listings.status_code == 200:
        html = listings.text
        regexp = "(https:\/\/data.insideairbnb.com\/\w+\S+\/listings.csv.gz)"
        # parsing the list to find the required city listings URL
        for m in re.findall(regexp, html):
            if m.split('/')[5] == city:
                url = m
                print('Data file located at ', url)
                break
    if url:
        
        con = duckdb.connect(config = {'allow_unsigned_extensions': True})
        load_h3_duckdb(con)
        con.sql(f"""INSTALL httpfs; LOAD httpfs;""")
        # reading data with duckDB and generating H3 cells
        def read_data(url, resolution):
            query = """
            SELECT h3_h3_to_string(h3_latlng_to_cell(latitude, longitude, $resolution)) cell_id,
                   h3_cell_to_boundary_wkt(cell_id) boundary,
                   h3_cell_to_lat(cell_id) cell_lat,
                   h3_cell_to_lng(cell_id) cell_lng,
                   count(1) cnt
            FROM read_csv($url) 
            GROUP BY cell_id
            ;
            """
            df = con.sql(query, params={'url': url,'resolution': resolution}).df()
            return df
    
        
        df = read_data(
            url=m, 
            resolution=resolution
        )
        # calculating an average num per cell to be used for styling
        sum = [df.cnt.sum()/len(df)]*len(df)

        df.insert(5, "val", sum, True)
        gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        
        print("Total of", df.cnt.sum(), "locations")
        return gdf
    else:
        print('Sorry, no data file identified for your location')
        
