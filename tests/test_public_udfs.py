import fused
import math
from fused.models.udf import AnyBaseUdf
import mercantile
from typing import Any
import os
import requests
import pytest
import ast


UDFS_THAT_ERROR = [
    # server_rt2.endpoints.secret_manager.SecretKeyNotFound: ARRAYLAKE_TOKEN
    "Arraylake_Example",
    # Misconfigured UDFs not present in repo
    "GOES_18_Runner",
    "GOES_18_Async",
    "GOES_18_Partitions",
    # None GDFs being returned
    "Overture_Places_Embedding_Clusters",
    # Network timeout
    "Create_A_Placekey_Join"
]


def get_public_udf_folders():
    """
    Get the list of public UDF folders from the UDFs repo as pytest params.
    Fetches the list based on the env variable "GITHUB_SHA".
    """

    headers = {}
    params = {}
    if os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ.get('GITHUB_TOKEN')}"
    if os.environ.get("GITHUB_SHA"):
        params["ref"] = os.environ.get("GITHUB_SHA")
        print(f"fetching UDFs from commit: {params['ref']}")

    response = requests.get(
        "https://api.github.com/repos/fusedio/udfs/contents/public",
        headers=headers,
        params=params,
    )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch UDFs: {response.status_code}")
    contents = response.json()

    for item in contents:
        if item["type"] == "dir":
            yield pytest.param(
                item["name"],
                item["html_url"],
                id=item["name"],
                marks=pytest.mark.skipif(
                    item["name"] in UDFS_THAT_ERROR, reason="UDF is not working"
                ),
            )


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


@pytest.mark.parametrize("udf_name, udf_url", get_public_udf_folders())
def test_public_udfs(udf_name: str, udf_url: str):
    try:
        udf = fused.load(udf_url)
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
                        return "vector_single"
                return "vector_single_none"
    except SyntaxError as e:
        print(f"Error parsing UDF code: {e}")
    return "auto" # Indicate parsing failed
