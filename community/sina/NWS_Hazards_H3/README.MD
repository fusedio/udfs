<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/nws_hazard_preview.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags: `h3-vector` `geojson` `noaa`

<!--fused:readme-->
# Overview
This Fused.io UDF fetches National Weather Service (NWS) / National Oceanic and Atmospheric Admin (NOAA) weather & hazard boundary polygons, converts their geometries to h3 cells at the users desired resolution (default res=7), and returns a GeoDataFrame.

# Terminology

**Watch**
: A watch is used when the risk of a hazardous weather or hydrologic event has increased significantly, but its occurrence, location, and/or timing is still uncertain. It is intended to provide enough lead time so that those who need to set their plans in motion can do so.

**Warning**
: A warning is issued when a hazardous weather or hydrologic event is occurring, is imminent, or has a very high probability of occurring. A warning is used for conditions posing a threat to life or property.

**Advisory**
: The expected weather condition has a pretty good chance of occurring, even a likely chance of occurring, but typically an advisory is used for less severe type of weather conditions. A Wind Advisory might be issued or a Freezing Rain Advisory issued instead of a High Wind Warning or an ice Storm Warning.

# External links
* [NWS Weather & Hazard Data Viewer](https://www.wrh.noaa.gov/map/)
* [NWS Watch/Warning/Advisory (WWA) FeatureServer](https://mapservices.weather.noaa.gov/eventdriven/rest/services/WWA/watch_warn_adv/FeatureServer)
* [h3-py Polygon Tutorial](https://uber.github.io/h3-py/polygon_tutorial.html)
