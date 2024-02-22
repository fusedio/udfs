
# Fused Multi-Step Job

## Get started
```python
# Import UDFs
from udf_GOES_18_Partitions import GOES_18_Partitions

# Instantiate individual jobs
job_GOES_18_Partitions = GOES_18_Partitions(url='https://noaa-goes18.s3.amazonaws.com/ABI-L2-CMIPF/2024/031/01/OR_ABI-L2-CMIPF-M6C08_G18_s20240310140216_e20240310149524_c20240310149597.nc', roi_wkt='{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[-4100000.0, 2400000.0], [3200000.0, 2400000.0], [3200000.0, -1500000.0], [-4100000.0, -1500000.0], [-4100000.0, 2400000.0]]]}}]}', crs='PROJCRS["WGS84 / Lambert_Conformal_Conic_2SP",BASEGEOGCRS["WGS84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],CONVERSION["unnamed",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of false origin",33,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",-125,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",21,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Easting at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["easting",east,ORDER[1],LENGTHUNIT["Meter",1]],AXIS["northing",north,ORDER[2],LENGTHUNIT["Meter",1]]]', chunk_len=1200)

# Instantiate multi-step job
job = fused.job([job_GOES_18_Partitions])

# Run locally
job.run_local(file_id=0, chunk_id=0)

# Run remotely
job.run_remote(output_table='output_table_name')
```
