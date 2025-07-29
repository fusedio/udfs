<!--fused:pin=14-->
<!--fused:tags-->
Tags: `usgs` `hillshade`

<!--fused:readme-->
# Sentinel-1 Edge Detection

## Description
This UDF processes Sentinel-1 radar data to detect edges and boundaries in landscapes using a multi-directional image shifting technique. By calculating pixel-wise standard deviation across shifted images, it highlights coastlines, urban boundaries, transportation networks, and topographic features. The radar-based approach works regardless of cloud cover or time of day, making it ideal for consistent monitoring of structural elements in the landscape.

## External links
* [Sentinel-1 Documentation](https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-1-sar)
* [Microsoft Planetary Computer STAC](https://planetarycomputer.microsoft.com/dataset/sentinel-1-rtc)

## Run this in any Jupyter Notebook
```python
import fused
udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/Sentinel1_Edge_Detection")
arr = fused.run(udf, time_of_interest="2023-06-01/2023-12-31")
arr
```
