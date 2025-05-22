import fused
import math
from fused.models.udf import AnyBaseUdf
import mercantile
from typing import Any
import os
import pytest
import ast


UDFS_THAT_ERROR = [
    # server_rt2.endpoints.secret_manager.SecretKeyNotFound: ARRAYLAKE_TOKEN
    "Arraylake_Example",
    # Network timeout
    "Overture_Places_Embedding_Clusters",
    "Create_A_Placekey_Join",
    # Protobuf parsing failed
    "Airplane_Detection_AOI_v2",
    "DL4EO_Airplane_Detection",
    # RasterIO error
    "NLCD_Tile_Example",
    # Broken links
    "REM_with_HyRiver",
    "Coverage_Model_ibis",
    "Five_Minutes_Away_in_Bushwick_Brooklyn",
    # Duckdb home directory not set
    "Ibis_DuckDB_Overturemaps",
    # No data available for params
    "download_images_for_bbox_arcgis_imageserver_rest",
    # Missing creds
    "NEON_Hyperspectral_GEE",
    # Multiple errors
    "NLCD_Tile_Hexify",
]

PUBLIC_UDFS_PATH = os.path.join(os.path.abspath(os.curdir), "public")

def get_public_udf_folders():
    """
    Get the list of public UDF folders from the local repo as pytest params.
    """
    changed_udfs = get_changed_udfs()

    for item in os.listdir(PUBLIC_UDFS_PATH):
        folder_abs_path = os.path.join(PUBLIC_UDFS_PATH, item)
        if os.path.isdir(folder_abs_path):
            yield pytest.param(
                item,
                folder_abs_path,
                id=item,
                marks=[pytest.mark.skipif(
                    item in UDFS_THAT_ERROR, reason="UDF is not working"
                ),
                pytest.mark.skipif(
                    changed_udfs is not None and item not in changed_udfs,
                    reason="UDF is not changed",
                ),
            ]
            )

def get_changed_udfs() -> list[str] | None:
    """
    Get the list of changed UDFs from the environment variable CHANGED_FILES
    If the environment variable is not set, return None
    """
    files = os.environ.get("CHANGED_FILES")
    print(f"Changed files: {files}")
    if files:
        file_names = files.split(",")
        # public/UDF_NAME/UDF_NAME.py
        udf = [file.split("/", 2)[1] for file in file_names]
        return udf
    return None


def get_bbox_for_udf(metadata: dict[str, Any]):
    """
    Get the bounding box for the UDF based on it's metadata
    """
    view = metadata.get("fused:defaultViewState")
    if view:
        x, y, z = mercantile.tile(
            view["longitude"], view["latitude"], math.ceil(view["zoom"])
        )
    else:
        # Using San Francisco as the default
        x, y, z = 9647, 12320, 15
    return (x, y, z)


@pytest.mark.parametrize("udf_name,udf_path", get_public_udf_folders())
def test_public_udfs(udf_name: str, udf_path: str):
    try:
        udf = fused.load(udf_path)
        metadata = udf.metadata
        udftype = metadata.get("fused:udfType")
        if udftype == "auto":
            udftype = _infer_udf_type_from_code(udf)
        if udftype in ["vector_tile", "raster"]:
            x, y, z = get_bbox_for_udf(metadata)
            fused.run(udf, x=x, y=y, z=z, engine="remote", cache_max_age="0s")
        elif udftype in ["vector_single","raster_single", "vector_single_none"]:
            fused.run(udf, engine="remote", cache_max_age="0s")
        else:
            return pytest.skip("Unable to infer UDF type")
    except Exception as e:
        raise Exception(f"Failed to run {udf_name}: {repr(e)}")


def _infer_udf_type_from_code(udf: AnyBaseUdf) -> str:
    """
    Infer the UDF type from the code based on whether the entrypoint has bounds as an arg or not
    """
    code_str = udf.code
    entrypoint_name = udf.entrypoint
    try:
        tree = ast.parse(code_str)
        for node in tree.body: # Iterate through top-level nodes only
            if (
                isinstance(node, ast.FunctionDef) # is method name
                and node.name == entrypoint_name # method name matches entrypoint
                and len(node.decorator_list) >0 # has some (fused) decorator
            ):
                # Check positional and keyword-only arguments
                all_args = node.args.args + node.args.kwonlyargs
                for arg in all_args:
                    if arg.arg == 'bounds':
                        return "vector_tile"
                return "vector_single_none"
    except SyntaxError as e:
        print(f"Error parsing UDF code: {e}")
    return "auto" # Indicate parsing failed
