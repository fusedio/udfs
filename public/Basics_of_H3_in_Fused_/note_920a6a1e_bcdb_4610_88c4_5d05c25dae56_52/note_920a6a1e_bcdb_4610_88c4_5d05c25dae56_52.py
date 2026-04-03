The datasets we have are:

- Temperature (ERA5, the closest H3 resolution to the original 27km resolution is hex 4)
- Elevation (Copernicus 90m, the closest H3 resolution is hex 9)

We can join these datasets on the `hex` column if they are brought to the same resolutions. As shown in part B we can easily aggregate data to lower H3 resolutions so we will bring both dataset to hex res 4.

DuckDB allows an efficient join on the `hex` column directly