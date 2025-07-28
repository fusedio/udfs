
@fused.udf
def udf(
    from typing import Literal
    output_format: Literal['data', 'chart-url'] = 'data',
    analysis_start_year: int = 2021, 
    lat: float=49.2806, 
    lon: float=-123.1259,
    buffer_amount: float = 100,
):
    """Returns crime statistics for any lat / lon in Vancouver based on the Vancouver open data portal"""
    import pandas as pd

    if output_format == 'data':
        import datetime
        import geopandas as gpd
        import pandas as pd
        import shapely
        from shapely.geometry import Point
    
        current_year = int(datetime.datetime.now().year)
    
        if analysis_start_year < 2020:
            print(f"Can only process up until 2020, truncating to 2020")
            analysis_start_year = 2020
    
        list_of_years_to_process = [year for year in range(analysis_start_year, current_year)]
        print(f"{list_of_years_to_process=}")
    
        yearly_type_summaries = {}
        yearly_crime_location = {}
    
        @fused.cache()
        def getting_yearly_data(year):
            path = f"s3://fused-asset/maxar-ted-demo/crimedata_csv_AllNeighbourhoods_{str(year)}.csv"
            
            df = pd.read_csv(path)
            df["geometry"] = gpd.points_from_xy(df["X"], df["Y"], crs="EPSG:32610")
        
            gdf = gpd.GeoDataFrame(df)
            print(gdf.shape)
            gdf.to_crs(4326, inplace=True)
        
            aoi_gdf = gpd.GeoDataFrame(geometry=[Point(lon, lat)], crs="EPSG:4326")
            
            # Project to a local UTM projection for accurate buffering in meters
            # Get UTM zone from longitude
            utm_zone = int(((lon + 180) / 6) % 60) + 1
            hemisphere = 'north' if lat >= 0 else 'south'
            utm_crs = f"+proj=utm +zone={utm_zone} +{hemisphere} +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
            
            gdf_utm = aoi_gdf.to_crs(utm_crs)
            gdf_utm['geometry'] = gdf_utm.buffer(buffer_amount)
            gdf_buffered = gdf_utm.to_crs("EPSG:4326")
        
            clipped_gdf = gpd.sjoin(gdf, gdf_buffered, predicate='intersects', how='inner')
    
            # Getting stats
            number_of_crimes = pd.DataFrame({f"number_crimes": [clipped_gdf.shape[0]]})
            grouped_by_type = clipped_gdf.groupby("TYPE").size().reset_index(name="count")
    
            return clipped_gdf, grouped_by_type
        
        for year in list_of_years_to_process:
            clipped_gdf, grouped_by_type = getting_yearly_data(year)
            yearly_crime_location[year] = clipped_gdf
            yearly_type_summaries[year] = grouped_by_type
    
        df_all_years = pd.concat(
            [df.assign(year=year) for year, df in yearly_type_summaries.items()],
            ignore_index=True
        )
        
        df = pd.concat([df.assign(year=year) for year, df in yearly_type_summaries.items()], ignore_index=True)
        print(f"{df_all_years=}")
        
        return df_all_years

    elif output_format == 'chart-url':
        # UDF that return html link
        url = f"https://staging.fused.io/server/v1/realtime-shared/fsh_4T1tQnPuRSDGpDVke0ltee/run/file?dtype_out_raster=png&dtype_out_vector=parquet&analysis_start_year={str(analysis_start_year)}&lat={lat}&lon={lon}&buffer_amount={buffer_amount}"
        print(url)
        return url

    else:
        raise NotImplementedError
    
