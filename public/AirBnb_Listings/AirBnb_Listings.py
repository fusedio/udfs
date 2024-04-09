@fused.udf
def udf(city='Paris', resolution=11):
    import duckdb
    from utils import load_h3_duckdb
    import shapely
    import geopandas as gpd
    import requests
    import re
    # set a resolution limit 
    if resolution > 11:resolution=11
        
    @fused.cache
    def get_city_data(city):
        # downloading main file list
        print('Downloading listings...')
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
                    return url
        return None
    
    city = city.lower().replace(' ','-')
    url = get_city_data(city)

    if url:
        out_path = f'{city}.csv.gz'
        csv_file = fused.core.download(url=url, file_path=out_path)
        
        con = duckdb.connect(config = {'allow_unsigned_extensions': True})
        load_h3_duckdb(con)
        con.sql(f"""INSTALL httpfs; LOAD httpfs;""")
        # reading data with duckDB and generating H3 cells
        @fused.cache
        def read_data(url, resolution):
            
            query = """
            SELECT h3_h3_to_string(h3_latlng_to_cell(latitude, longitude, $resolution)) cell_id,
                   h3_cell_to_boundary_wkt(cell_id) boundary,
                   
                   count(1) cnt
            FROM read_csv($url) 
            
            GROUP BY cell_id
            ;
            """
            df = con.sql(query, params={'url': url,'resolution': resolution}).df()
            return df
    
        
        df = read_data(
            url=str(csv_file), 
            resolution=resolution
        )
        sum = [round(df.cnt.sum()/len(df),1)]*len(df)
        df.insert(3, "val", sum, True)
        gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
        
        print("Total of", df.cnt.sum(), "locations")
        return gdf
    else:
        print('Sorry, no data file identified for your location')
        
