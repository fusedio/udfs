from typing import Any
import sys
import os
import argparse
import json
import logging
from fused.api.api import AnyBaseUdf
from loguru import logger
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount, Route
from mcp.server.sse import SseServerTransport
from mcp.server import Server
import uvicorn
import fused

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Configure fused environment
fused._env(os.getenv("FUSED_ENV", "unstable"))

DEFAULT_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "fused-api-client/1.0"
DEFAULT_TOKENS_TO_READ_FUSED_DOCS = [
    "fsh_7lMTze647XbSSUEjBLGNoy",  # Reading fused docs from prod
    "fsh_7hfbTGjNsv4ZzWfHEU1iHm",  # Listing code & name of public UDFs
]


def register_udf_tool(mcp: FastMCP, udf: AnyBaseUdf):
    """Register a UDF tool using provided metadata."""
    mcp_metadata = udf.metadata.get("fused:mcp")

    if not mcp_metadata:
        raise ValueError(
            f"No metadata provided for token '{udf.name}', skipping registration"
        )

    # Log the parameters from metadata
    logger.info(
        f"Registering tool '{udf.name}' with parameters: {mcp_metadata['parameters']}"
    )

    # Create docstring from metadata
    docstring = f"{mcp_metadata['description']}\n\nArgs:\n"

    if mcp_metadata["parameters"] is not None:
        # We can have UDFs that don't have any parameters, in which case we assume they'd have 'fused:mcp' to have {"parameters": None}
        logger.info(f"Metadata passed at this point: {mcp_metadata=}")
        for param in mcp_metadata["parameters"]:
            docstring += (
                f"    {param['name']}: {param.get('description', 'No description')}\n"
            )
    else:
        logger.info(
            f"No parameters received for {udf.name} so skipping creating docstring for parameters"
        )

    async def udf_tool(**params):
        """Dynamic UDF tool implementation."""
        # Log what parameters were received
        logger.info(f"Tool '{udf.name}' received parameters: {params}")

        try:
            result = fused.run(udf, **params)
            if isinstance(result, pd.DataFrame):
                return result.to_dict(orient="records")
            return result
        except Exception as e:
            logger.error(f"Error executing function: {e}")
            return f"Error: {str(e)}"

    # Set the docstring
    udf_tool.__doc__ = docstring

    # Register the tool with the token ID as name
    return mcp.tool(name=udf.name)(udf_tool)


def create_server_from_token_ids(
    token_ids: list[str] | None = None,
    server_name: str = "udf-server",
):
    """Create and configure an MCP server with tools from UDF tokens."""
    # Initialize FastMCP server
    mcp = FastMCP(server_name)

    # Register tools for each token ID
    if token_ids:
        for token_id in token_ids:
            token_id = token_id.strip()  # Remove any whitespace
            if token_id:
                try:
                    udf = fused.load(token_id)
                    register_udf_tool(mcp, udf)
                    logger.info(f"Successfully registered tool for token: {token_id}")
                except Exception as e:
                    logger.error(f"Error registering tool for token '{token_id}': {e}")

    return mcp


def create_server_from_folder_names(
    folder_names: list[str] | None = None,
    server_name: str = "udf-server",
):
    """Create and configure an MCP server with tools from local UDF folders."""
    # Initialize FastMCP server
    mcp = FastMCP(server_name)

    # Register tools for each folder name
    if folder_names:
        for udf_name in folder_names:
            udf_name = udf_name.strip()  # Remove any whitespace
            if udf_name:
                try:
                    # load UDF from folder
                    folder_path = f"{os.path.abspath(os.curdir)}/udfs/{udf_name}"
                    udf = fused.load(folder_path)

                    # register UDF tool to mcp
                    register_udf_tool(mcp, udf)
                    logger.info(f"Successfully registered tool for udf: {udf_name}")
                except Exception as e:
                    logger.error(f"Error registering tool for udf '{udf_name}': {e}")

    return mcp


def create_starlette_app(mcp_server: Server[Any], *, debug: bool = False) -> Starlette:
    """Create a Starlette application that can serve the provided mcp server with SSE."""
    sse = SseServerTransport("/messages/")

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
            request.scope,
            request.receive,
            request._send,
        ) as (read_stream, write_stream):
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options(),
            )

    return Starlette(
        debug=debug,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )


def run_server(
    server_name: str,
    runtime: str = "remote",
    host: str = "0.0.0.0",
    port: int = 8080,
    tokens: list[str] = None,
    udf_names: list[str] = None,
):
    """
    Run an MCP server with the specified configuration.

    Args:
        server_name: Name of the server
        runtime: Runtime to use (local, remote)
        host: Host to bind to
        port: Port to listen on
        tokens: List of token IDs to register as tools
        udf_names: List of UDF folder names to register as tools

    Raises:
        ValueError: If both tokens and udf_names are provided
    """
    if tokens and udf_names:
        raise ValueError("tokens and udf_names cannot be used together")

    # Create server instance based on input
    if udf_names:
        mcp_instance = create_server_from_folder_names(udf_names, server_name)
    else:
        token_list = tokens if tokens else DEFAULT_TOKENS_TO_READ_FUSED_DOCS
        mcp_instance = create_server_from_token_ids(token_list, server_name)

    try:
        logger.info(f"Starting MCP server with name: {server_name}")
        if runtime == "local":
            logger.info("Server running in local mode")
            mcp_instance.run("stdio")
        else:
            logger.info(f"Server will be available at http://{host}:{port}/sse")
            # Create and run Starlette app with SSE transport
            mcp_server = mcp_instance._mcp_server
            starlette_app = create_starlette_app(mcp_server)
            uvicorn.run(starlette_app, host=host, port=port)

    except Exception as e:
        logger.error(f"Error starting server: {e}")
        sys.exit(1)



