@fused.udf
def create_cog_from_file(input_path: str = "download_test/5527-11.tif"):
    import pandas as pd
    from utils import create_cog

    common = fused.utils.common

    input_path = fused.file_path(input_path)
    output_path = input_path.replace(".tif", "_cog.tif")
    print(output_path)

    create_cog(
        input_path=input_path,
        output_path=output_path,
        category="orthophoto",
        outSRS=3857,
        out_nodata_value=0,
    )

    return


@fused.udf
def udf(input_dir: str = "convert_to_cog_test"):
    from pathlib import Path

    input_dir = fused.file_path(input_dir)
    tif_files = [
        f for f in Path(input_dir).glob("*.tif") if not f.name.endswith("_cog.tif")
    ]

    # Create a list of parameter dictionaries for each tif file
    arg_list = [{"input_path": str(f)} for f in tif_files]
    print("Converting the following files to COG \n")
    print(arg_list)

    # Submit all jobs using fused.submit
    if arg_list:
        job_pool = fused.submit(
            "create_cog_from_file",  # Assuming this is the name of your UDF
            arg_list,
            max_workers=16,
            debug_mode=False,
        )

        return print("\n Converted all files to COG")
    else:
        return print("\n No TIFF files found to process")
