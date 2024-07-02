<!--fused:preview-->
<p align="center"><img src="https://fused-magic.s3.us-west-2.amazonaws.com/thumbnails/udfs-staging/DEM_Tile_Example.png" width="600" alt="UDF preview image"></p>

<!--fused:tags-->
Tags:  `Band algebra` `AWS` `MSFT-PC` `STAC`

<!--fused:readme-->

#### Overview
The Copernicus DEM is a digital surface model (DSM), which represents the surface of the Earth including buildings, infrastructure, and vegetation. This DSM is based on radar satellite data acquired during the TanDEM-X Mission, which was funded by a public-private partnership between the German Aerospace Centre (DLR) and Airbus Defence and Space.


Copernicus DEM is available at both 30-meter and 90-meter resolution.


See the [Product Handbook](https://object.cloud.sdsc.edu/v1/AUTH_opentopography/www/metadata/Copernicus_metadata.pdf) for more information.

See the dataset page on [OpenTopography](https://doi.org/10.5069/G9028PQB).
#### STAC Collections
*  [AWS Earth Search](https://earth-search.aws.element84.com/v1/collections/cop-dem-glo-30)
* [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/api/stac/v1/collections/cop-dem-glo-30)


## Run this in any Jupyter Notebook

```python
import fused
import geopandas as gpd

udf_url = "https://github.com/fusedio/udfs/tree/main/public/DEM_Tile_Example"
udf = fused.load(udf_url)

bbox = gpd.GeoDataFrame.from_features(
    {"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"x": 1311, "y": 3164, "z": 13}, "geometry": {"type": "Polygon", "coordinates": [[[-124.1455078125, 41.77131167976406], [-124.1455078125, 41.73852846935915], [-124.189453125, 41.73852846935915], [-124.189453125, 41.77131167976406], [-124.1455078125, 41.77131167976406]]]}}]}
)

arr = udf.run_local(bbox=bbox)
arr
```
