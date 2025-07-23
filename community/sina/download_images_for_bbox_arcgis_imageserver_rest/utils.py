import json
import os

import fused
import geopandas as gpd
import pandas as pd
import rasterio
import requests
import shapely
from osgeo import gdal


def get_arcgis_services_to_pd(base_url: str) -> pd.DataFrame:
    """Fetch services from ArcGIS REST endpoint and return as DataFrame.

    Args:
        base_url (str): The base URL of the ArcGIS REST endpoint.

    Returns:
        pd.DataFrame: A DataFrame containing the services with columns
        ['category', 'service_name', 'year', 'type', 'name'].
    """
    # Get the response
    response = requests.get(f"{base_url}?f=json")
    data = response.json()

    # Convert to DataFrame
    services_df = pd.DataFrame(data["services"])

    # Clean up the data
    if "name" in services_df.columns:
        # Split the name field if it contains '/'
        if services_df["name"].str.contains("/").any():
            services_df[["category", "service_name"]] = services_df["name"].str.split(
                "/", expand=True
            )
        else:
            services_df["category"] = ""
            services_df["service_name"] = services_df["name"]

    # Add year information if it exists in the name
    services_df["year"] = services_df["service_name"].str.extract(r"(\d{4})")

    # Reorder columns to a more logical sequence
    cols = ["category", "service_name", "year", "type", "name"]
    services_df = services_df[cols]

    # Sort by category and name
    services_df = services_df.sort_values(["category", "service_name"]).reset_index(
        drop=True
    )
    print(services_df)
    return services_df


def gather_file_params_from_service_url(
    service_url: str,
    service_name: str,
    output_directory: str,
    bbox_gpkg_path: str,
    outSRS: int = 3857,
):
    query_url = f"{service_url}/{service_name}/ImageServer/query"
    download_url = f"{service_url}/{service_name}/ImageServer/download"
    file_endpoint_url = f"{service_url}/{service_name}/ImageServer/file"
    image_server_url = f"{service_url}/{service_name}/ImageServer"

    arcgis_rest_urls = {
        "query_url": query_url,
        "download_url": download_url,
        "file_endpoint_url": file_endpoint_url,
        "image_server_url": image_server_url,
    }

    if bbox_gpkg_path is not None:
        gdf = gpd.read_file(bbox_gpkg_path)
        if outSRS:
            if outSRS != gdf.crs.to_epsg():
                gdf = gdf.to_crs(outSRS)
        bbox = gdf.total_bounds
        query_params = {
            "where": "1=1",
            "geometry": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",  # bbox returns tuple of (minx,miny,maxx,maxy)
            "geometryType": "esriGeometryEnvelope",
            "inSR": outSRS,  # Specifying input coordinate system
            "spatialRel": "esriSpatialRelIntersects",
            "returnGeometry": "false",
            "returnIdsOnly": "true",
            "outSR": outSRS,  # Specifying output coordinate system
            "f": "json",
        }
    else:
        # if no bbox is provided, download all tiles
        query_params = {"where": "1=1", "f": "json", "returnIdsOnly": "true"}
    response_query_all_tiles_one_service = requests.get(query_url, params=query_params)
    data_all_tiles_one_service = response_query_all_tiles_one_service.json()
    print(data_all_tiles_one_service)
    if not data_all_tiles_one_service["objectIds"]:
        raise RuntimeError("No data available with these parameters.")

    # Download service metadata
    metadata_params = {"f": "pjson"}
    metadata_response = requests.get(image_server_url, params=metadata_params)
    metadata_filename = f"{service_name}.json"
    metadata_filepath = os.path.join(output_directory, metadata_filename)
    with open(metadata_filepath, "w") as metadata_file:
        json.dump(metadata_response.json(), metadata_file)

    file_param_list = {}

    # Get parameters for relevant files
    for tile_id in data_all_tiles_one_service["objectIds"]:
        download_params = {"rasterIds": str(tile_id), "f": "json"}

        response_download_one_tile = requests.get(download_url, download_params)
        response_download_one_tile.raise_for_status()
        data_one_tile = response_download_one_tile.json()
        if "error" in data_one_tile:
            err = data_one_tile["error"]
            print(f'Error details: {data_one_tile["error"]["details"][0]}')
            raise RuntimeError(f'{err["code"]}: {err["message"]}')

        tile_filepath = data_one_tile["rasterFiles"][0]["id"]

        # Skip files that start with "Ov_" -> these are overview tiles that we don't need
        filename = tile_filepath.split("\\")[-1]
        if not filename.startswith("Ov_"):
            file_params = {
                "id": tile_filepath,
                "rasterId": str(tile_id),
            }

            file_param_list[filename] = file_params

    return file_param_list, arcgis_rest_urls


def create_cog(
    input_path: str,
    output_path: str,
    category: str = "orthophoto",
    outSRS: int = None,
    out_nodata_value: int | float = None,
) -> None:
    """Create a Cloud Optimized GeoTIFF (COG) from a GeoTIFF file.

    Args:
        input_path (str): The file path to the input GeoTIFF file.
        output_path (str): The file path where the COG will be created.
        category (str, optional): The type of dataset, either 'orthophoto' or 'elevation'. Defaults to 'orthophoto'.
        outSRS (int, optional): The desired output spatial reference system (EPSG code). If not provided, the input SRS will be used.
        out_nodata_value (int | float, optional): The defined nodata value for the output. If not provided, the input nodata value will be used.

    Raises:
        ValueError: If the specified category is not recognized.
        RuntimeError: If the translation or warping process fails.

    Returns:
        None
    """
    # Enable GDAL exceptions
    gdal.UseExceptions()

    # Read source nodata value and original SRS
    ds = gdal.Open(input_path)
    srcNodata = ds.GetRasterBand(1).GetNoDataValue()
    original_srs = ds.GetProjection()  # Get the original projection from the input file

    if not out_nodata_value:
        out_nodata_value = srcNodata
    # Set up creation options based on category
    if category == "orthophoto":
        compression = "JPEG"
        photometric = "YCBCR" if ds.RasterCount > 1 else "MINISBLACK"
        predictor = "1"
    elif category == "elevation":
        compression = "LZW"
        photometric = "MINISBLACK"
        predictor = "2"
    else:
        raise ValueError(f"Category '{category}' not known.")

    # Determine the output SRS
    dstSRS = (
        f"EPSG:{outSRS}" if outSRS else original_srs
    )  # Use original SRS if outSRS is None

    # TODO: Add mask layer for JPEG compressed output if needed to avoid artifacts
    warp_options = {
        "dstSRS": dstSRS,
        "format": "GTiff",
        "srcNodata": srcNodata,
        "dstNodata": out_nodata_value,
        "resampleAlg": "bilinear",
        "creationOptions": [
            f"COMPRESS={compression}",
            f"PHOTOMETRIC={photometric}",
            f"PREDICTOR={predictor}",
            "TILED=YES",
            "BLOCKXSIZE=256",
            "BLOCKYSIZE=256",
            "num_threads=20",
            "BIGTIFF=YES",
        ],
    }
    # print(srcNodata)
    # print(out_nodata_value)
    # Use gdal.Warp to handle both reprojection and COG creation
    result = gdal.Warp(output_path, input_path, **warp_options)

    if result is None:
        raise RuntimeError(f"Failed to create COG from {input_path} to {output_path}")

    # Close the dataset to ensure it's properly written
    result = None

    # Open for overview building
    ds = gdal.Open(output_path, 1)
    if ds is None:
        raise RuntimeError(f"Failed to open {output_path} for building overviews")

    ds.BuildOverviews("AVERAGE", [2, 4, 8, 16, 32])
    ds = None

    # Verify output exists and can be opened
    if not os.path.exists(output_path):
        raise RuntimeError("COG file was not created")

    with rasterio.open(output_path) as src:
        print(f"Final raster shape: {src.shape}")
        print(f"Final raster CRS: {src.crs}")
        print(f"Final resolution: {src.res}")

    return


def get_request(
    file_endpoint_url: str, file_params: dict, retries: int = 1, max_retry: int = 5
):
    try:
        return requests.get(file_endpoint_url, file_params)
    except Exception as e:
        print(f"Request failed: {e}")
        if retries >= max_retry:
            raise Exception from e
        retries += 1
        print("Repeating request ...")
        return get_request(retries=retries)


@fused.cache
def get_urls_from_arcgis_imageserver_rest(
    service_url: str = "https://gis.stmk.gv.at/image/rest/services/OGD_DOP",
    outSRS: int = 3857,
    bbox_gpkg_path: str = "test_bbox.gpkg",
    output_directory: str = "download_test",
    service_name: str = "Flug_2008_2011_RGB",
):
    import time

    import pandas as pd
    from utils import gather_file_params_from_service_url, get_arcgis_services_to_pd

    services_df = get_arcgis_services_to_pd(service_url)
    if service_name == "":
        service_name = services_df.loc[8].service_name
        print(service_name + " will be used for testing purposes!")
    bbox_gpkg_path = fused.file_path(bbox_gpkg_path)
    output_directory = fused.file_path(output_directory)

    beginning_time = time.time()
    file_param_list, arcgis_rest_urls = gather_file_params_from_service_url(
        service_url, service_name, output_directory, bbox_gpkg_path, outSRS
    )

    print(file_param_list)
    print(arcgis_rest_urls)
    end_process_1 = time.time()
    process_time_1 = round(end_process_1 - beginning_time, 2)
    print(f"{process_time_1=}")

    # Create a DataFrame to store the results
    results_df = pd.DataFrame(
        {"file_param_list": [file_param_list], "arcgis_rest_urls": [arcgis_rest_urls]}
    )

    return results_df


def download_single_image_func(
    file_params: dict, filename: str, output_directory: str, arcgis_rest_urls_dict: dict
):
    import json
    import os
    from pathlib import Path

    import requests
    from utils import get_request

    output_filepath = Path(os.path.join(output_directory, filename))
    output_filepath = fused.file_path(str(output_filepath))

    # Check if tile is already downloaded
    if os.path.isfile(output_filepath):
        return print(f"Output file {output_filepath} was already downloaded.")

    file_endpoint_url = arcgis_rest_urls_dict["file_endpoint_url"]
    image_server_url = arcgis_rest_urls_dict["image_server_url"]
    response_file_endpoint_one_tile = get_request(file_endpoint_url, file_params)

    # Download tile metadata
    metadata_params = {"f": "pjson"}
    metadata_url = f"{image_server_url}/{file_params['rasterId']}"
    metadata_response = requests.get(metadata_url, params=metadata_params)
    metadata_filename = f"{os.path.splitext(filename)[0]}.json"
    metadata_filepath = os.path.join(output_directory, metadata_filename)
    metadata_filepath = fused.file_path(str(metadata_filepath))

    # Save tile data
    with open(output_filepath, "wb") as output_file:
        output_file.write(response_file_endpoint_one_tile.content)
    # Save tile metadata
    with open(metadata_filepath, "w") as metadata_file:
        json.dump(metadata_response.json(), metadata_file)

    return print(output_filepath + " successfully downloaded!")


def bounds_fused_to_gdf(bounds: fused.types.Bounds):
    bounds_gdf = gpd.GeoDataFrame(
        {"geometry": [shapely.box(bounds[0], bounds[1], bounds[2], bounds[3])]},
        crs="EPSG:4326",
    )
    return bounds_gdf
