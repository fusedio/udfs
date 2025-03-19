"""
Requirements
- Pull these weather features/bands: ["pr", "tmmn", "tmmx", "srad", "rmax", "rmin", "sph"]
- Date range will be from **2014-2024**

Data:
- .nc files are here: https://www.northwestknowledge.net/metdata/data/
"""

@fused.udf
def udf(bbox, year: str = "2016", var: str="tmmn"):
    import pandas as pd
    import xarray

    xy_cols = ['lon', 'lat']
    
    # Dynamically construct the path based on the year and month
    path=f'https://www.northwestknowledge.net/metdata/data/{var}_{year}.nc'
    
    path = fused.download(path, path) 
    ds = xarray.open_dataset(path, engine='h5netcdf')
    print('ds', ds)
    return
    