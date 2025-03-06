@fused.cache
def load_target_data():
    import pandas as pd
    # Bring in target data
    years = ["2015", "2016", "2017", "2018", "2019", "2020"]
    dfs = []
    # path = 's3://soldatanasasifglobalifoco2modis1863/USDA/data/Actual_yields/USDA_crop_yields_2018.csv'
    # Fetch for each year
    for year in years:
        path = f's3://soldatanasasifglobalifoco2modis1863/USDA/data/Actual_yields/USDA_crop_yields_{year}.csv'
        df = pd.read_csv(path)
        
        # Select only the relevant columns
        cols = [
            'Value',  # Bushels / acre
            'Year',
            'County ANSI',
        ]
        # df = df[cols]
        
        dfs.append(df)
    
    # Concatenate all DataFrames into a single DataFrame
    df_actuals = pd.concat(dfs, ignore_index=True)
    print(df_actuals)
    return df_actuals