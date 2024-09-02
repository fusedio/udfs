@fused.udf
def udf(
    bbox: fused.types.TileGDF, 
    n_trips_threshold: int = 0
):
    import geopandas as gpd
    import shapely
    import duckdb
    utils = fused.load("https://github.com/fusedio/udfs/tree/19b5240/public/common/").utils

    @fused.cache
    def load_data(bbox, n_trips_threshold=n_trips_threshold):
        # 1. Load weights
        # FAF5 Total Truck Flows by Commodity_2022.csv
        con = duckdb.connect()
        @fused.cache
        def load_weights():
            return con.sql("""
                SELECT 
                    ID::INTEGER AS ID, 
                    "AB Tons_22 All", 
                    "BA Tons_22 All", 
                    "AB Trips_22 All", 
                    "BA Trips_22 All", 
                    "TOT Tons_22 All", 
                    "TOT Trips_22 All" 
                FROM read_csv('s3://fused-users/fused/plinio/charm/FAF5_Truck_Flows/FAF5 Total Truck Flows by Commodity_2022.csv') 
                """).df()        
        df_weight = load_weights()
    
        # 2. Load links in the FAF network
        path = 's3://fused-users/fused/plinio/charm/faf_links_chunk100.parquet/'
        # TODO
        use_columns = ['ID', 'LENGTH', 'DIR', 'DATA1', 'Class', 'Class_Description', 'Road_Name','Speed_Limit','geometry']
        gdf = utils.table_to_tile(bbox, table=path, use_columns=use_columns, min_zoom=3)
        df_weight['ID'] = df_weight['ID'].astype('int64')
        df_weight['ID'] = df_weight['ID'].astype('int64')
        gdf['ID'] = gdf['ID'].astype('int64')
    
    
        # 3. Join
        @fused.cache
        def perform_join(bbox):
            out = gdf.join(df_weight.set_index('ID'), on='ID', how='inner', lsuffix='_left', rsuffix='_right')
            out.columns = out.columns.str.replace(' ', '')
            return out
        out = perform_join(bbox)
        out = out[out['BATrips_22All'] >= n_trips_threshold]
        print(out)
        if len(out) > 10_000:
            out['geometry'] = out['geometry'].simplify(0.1)
        elif len(out) > 1_000:
            out['geometry'] = out['geometry'].simplify(0.01)
        
        return out


    out = load_data(bbox)
    return out[['geometry', 'ID', 'TOTTrips_22All']]
