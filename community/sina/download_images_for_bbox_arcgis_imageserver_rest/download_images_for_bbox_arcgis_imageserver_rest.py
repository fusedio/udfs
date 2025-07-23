@fused.udf
def download_single_image(
    file_params: dict = {
        "id": ".\\12_2010_Nordost\\RGB\\5527-11.tif",
        "rasterId": "897",
    },
    filename: str = "5527-11.tif",
    output_directory: str = "download_test",
    arcgis_rest_urls_dict: dict = {
        "query_url": "https://gis.stmk.gv.at/image/rest/services/OGD_DOP/Flug_2008_2011_RGB/ImageServer/query",
        "download_url": "https://gis.stmk.gv.at/image/rest/services/OGD_DOP/Flug_2008_2011_RGB/ImageServer/download",
        "file_endpoint_url": "https://gis.stmk.gv.at/image/rest/services/OGD_DOP/Flug_2008_2011_RGB/ImageServer/file",
        "image_server_url": "https://gis.stmk.gv.at/image/rest/services/OGD_DOP/Flug_2008_2011_RGB/ImageServer",
    },
):
    from utils import download_single_image_func

    download_single_image_func(
        file_params, filename, output_directory, arcgis_rest_urls_dict
    )
    return


@fused.udf
def udf(
    bounds: fused.types.Bounds = [
        14.567609453698061,
        47.5123572642387,
        14.618900043386601,
        47.55116996253098,
    ],
    service_url: str = "https://gis.stmk.gv.at/image/rest/services/OGD_DOP",
    outSRS: int = 3857,
    bbox_gpkg_path: str = None,  # 'test_bbox.gpkg',
    output_directory: str = "download_test",
    service_name: str = "Flug_2008_2011_RGB",
    max_workers: int = 24,
):
    import os
    import sys

    from utils import bounds_fused_to_gdf, get_urls_from_arcgis_imageserver_rest

    if (bounds is not None) & (bbox_gpkg_path is None):
        utils = fused.load(
            "https://github.com/fusedio/udfs/tree/e74035a1/public/common/"
        ).utils
        bounds_gdf = bounds_fused_to_gdf(bounds)
        print(bounds_gdf)
        temp_bounds_gpkg_file_pth = fused.file_path(
            os.path.join(output_directory, "download_bounds.gpkg")
        )
        bounds_gdf.to_file(temp_bounds_gpkg_file_pth, driver="GPKG")
        bbox_gpkg_path = temp_bounds_gpkg_file_pth
    elif (bbox_gpkg_path is not None) & (bounds is None):
        bbox_gpkg_path = fused.file_path(bbox_gpkg_path)
    else:
        print("Please provide either bounds or path to a gpkg with a single bbox")
        sys.exit()

    result_df = get_urls_from_arcgis_imageserver_rest(
        service_url=service_url,
        outSRS=outSRS,
        bbox_gpkg_path=bbox_gpkg_path,
        output_directory=output_directory,
        service_name=service_name,
    )

    file_param_dict = result_df["file_param_list"].iloc[0]
    arcgis_rest_urls_dict = result_df["arcgis_rest_urls"].iloc[0]

    arg_list = []
    for filename, file_params in file_param_dict.items():
        arg_list.append(
            {
                "file_params": file_params,
                "filename": filename,
                "output_directory": output_directory,
                "arcgis_rest_urls_dict": arcgis_rest_urls_dict,
            }
        )

    # Run multiple workers to download images
    job_pool = fused.submit(
        "download_single_image",
        arg_list,
        max_workers=max_workers,
        wait_on_results=True,
        debug_mode=False,
    )

    print("success")

    # Todo: Convert downloaded files to COG -> use this UDF:
    # fused.load(hkristen@posteo.at/create_cog_from_file)
    # fused.run(create_cogs_from_tiffs_in_folder(input_dir=output_directory))
    return
