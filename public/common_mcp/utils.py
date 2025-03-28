import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import fused
import pandas as pd
import mcp.server.stdio
import mcp.types as types
import uvicorn
from fused.api.api import AnyBaseUdf
from loguru import logger
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount, Route

# Configure logging
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

if not os.getenv("FUSED_NO_SET_ENV"):
    # Configure fused environment
    fused._env(os.getenv("FUSED_ENV", "unstable"))

DEFAULT_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "fused-api-client/1.0"
DEFAULT_TOKENS_TO_READ_FUSED_DOCS = [
    "fsh_7lMTze647XbSSUEjBLGNoy",  # Reading fused docs from prod
    "fsh_7hfbTGjNsv4ZzWfHEU1iHm",  # Listing code & name of public UDFs
]

# Not limiting the amount of data from dataframes we can return back to Claude
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


class UdfMcpServer:
    """MCP server for Fused UDFs using direct Server API"""

    def __init__(self, server_name: str):
        """Initialize the UDF MCP server"""
        self.server = Server(server_name)
        self.registered_udfs: Dict[str, AnyBaseUdf] = {}
        self.registered_udf_tokens: Dict[str, str] = {}
        self.tool_schemas: Dict[str, Dict[str, Any]] = {}
        self._setup_handlers()

    def _setup_handlers(self):
        """Set up the MCP request handlers"""

        @self.server.list_tools()
        async def handle_list_tools() -> list[types.Tool]:
            """List all registered UDF tools"""
            tools = []

            for token_id, udf in self.registered_udfs.items():
                # Get the schema for this tool
                schema = self.tool_schemas.get(
                    token_id, {"type": "object", "properties": {}, "required": []}
                )

                # Create tool definition
                tools.append(
                    types.Tool(
                        name=token_id,
                        description=udf.metadata.get("fused:mcp", {}).get(
                            "description", f"Function: {token_id}"
                        ),
                        inputSchema=schema,
                    )
                )

            logger.info(f"Returning {len(tools)} tools")
            return tools

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: Dict[str, Any] = None
        ) -> list[types.TextContent]:
            """Execute a registered UDF tool"""
            try:
                # Check if this is a registered UDF
                if name not in self.registered_udfs:
                    return [
                        types.TextContent(
                            type="text", text=f"Error: Unknown UDF tool '{name}'"
                        )
                    ]

                # Get the UDF
                udf = self.registered_udf_tokens.get(name, self.registered_udfs[name])
                arguments = arguments or {}

                logger.info(f"Executing UDF '{name}' {'(with token)' if isinstance(udf, str) else '(with source code)'} with arguments: {arguments}")

                # Execute the UDF
                try:
                    result = fused.run(udf, **arguments)

                    # Need ot use df.to_string() rather than str(df) because str(df) truncates the output
                    return [types.TextContent(type="text", text=result.to_string())]

                except Exception as e:
                    logger.exception(f"Error executing UDF '{name}': {e}")
                    return [
                        types.TextContent(
                            type="text", text=f"Error executing UDF '{name}': {str(e)}"
                        )
                    ]

            except Exception as e:
                logger.exception(f"Error handling tool call: {e}")
                return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    def register_udf(self, udf: AnyBaseUdf, token: Optional[str] = None) -> bool:
        """Register a UDF as an MCP tool"""
        try:
            # Get the MCP metadata
            mcp_metadata = udf.metadata.get("fused:mcp")
            if not mcp_metadata:
                logger.warning(f"No MCP metadata for UDF '{udf.name}', skipping")
                return False

            # Log registration
            logger.info(f"Registering UDF tool '{udf.name}'")

            # Create schema from parameters
            schema = self._create_schema_from_parameters(udf.name, mcp_metadata)

            # Store the UDF and schema
            self.registered_udfs[udf.name] = udf
            if token is not None:
                self.registered_udf_tokens[udf.name] = token
            self.tool_schemas[udf.name] = schema

            logger.info(f"Successfully registered UDF tool '{udf.name}'")
            return True

        except Exception as e:
            logger.exception(f"Error registering UDF '{udf.name}': {e}")
            return False

    def _create_schema_from_parameters(
        self, udf_name: str, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a JSON Schema from UDF parameters"""
        # Initialize schema
        schema = {"type": "object", "properties": {}, "required": []}

        # Get parameters
        parameters_raw = metadata.get("parameters", [])

        # Check if parameters is a JSON string that needs to be deserialized
        if isinstance(parameters_raw, str):
            try:
                logger.debug(
                    f"Parameters for UDF '{udf_name}' is a string, attempting to parse as JSON"
                )
                parameters = json.loads(parameters_raw)
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Failed to parse parameters JSON for UDF '{udf_name}': {e}"
                )
                parameters = []
        else:
            parameters = parameters_raw

        if parameters is None:
            logger.info(f"No parameters defined for UDF '{udf_name}'")
            return schema

        # Process parameters
        for param in parameters:
            if not isinstance(param, dict) or "name" not in param:
                continue

            param_name = param["name"]
            param_type = param.get("type", "string").lower()
            param_desc = param.get("description", f"Parameter: {param_name}")
            required = param.get("required", True)

            if param_type in ("float", "double", "decimal", "number"):
                json_type = "number"
            elif param_type in ("int", "integer"):
                json_type = "number"
            elif param_type in ("bool", "boolean"):
                json_type = "boolean"
            elif param_type in ("array", "list"):
                json_type = "array"
            elif param_type in ("object", "dict", "map"):
                json_type = "object"
            else:
                json_type = "string"

            # Add property definition
            schema["properties"][param_name] = {
                "type": json_type,
                "description": param_desc,
            }

            # Add to required list if needed
            if required:
                schema["required"].append(param_name)

        logger.info(f"Created schema for UDF '{udf_name}': {schema}")
        return schema

    async def run_stdio(self):
        """Run the server with stdio transport"""
        logger.info("Starting MCP server with stdio transport")
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name=self.server.name,
                    server_version="1.0.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )

    def create_starlette_app(self, debug: bool = False) -> Starlette:
        """Create a Starlette application for SSE transport"""
        sse = SseServerTransport("/messages/")

        async def handle_sse(request: Request) -> None:
            async with sse.connect_sse(
                request.scope,
                request.receive,
                request._send,
            ) as (read_stream, write_stream):
                await self.server.run(
                    read_stream,
                    write_stream,
                    InitializationOptions(
                        server_name=self.server.name,
                        server_version="1.0.0",
                        capabilities=self.server.get_capabilities(
                            notification_options=NotificationOptions(),
                            experimental_capabilities={},
                        ),
                    ),
                )

        return Starlette(
            debug=debug,
            routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )


def create_server_from_token_ids(
    token_ids: List[str] = None,
    server_name: str = "udf-server",
) -> UdfMcpServer:
    """Create and configure an MCP server with tools from UDF tokens."""
    # Initialize server
    server = UdfMcpServer(server_name)

    # Register tools for each token ID
    if token_ids:
        for token_id in token_ids:
            token_id = token_id.strip()  # Remove any whitespace
            if token_id:
                try:
                    # Load the UDF
                    logger.info(f"Loading UDF from token: {token_id}")
                    udf = fused.load(token_id)

                    # Register the UDF
                    server.register_udf(udf, token_id)

                except Exception as e:
                    logger.exception(f"Error loading UDF from token '{token_id}': {e}")

    return server


def create_server_from_folder_names(
    folder_names: List[str] = None,
    server_name: str = "udf-server",
) -> UdfMcpServer:
    """Create and configure an MCP server with tools from local UDF folders."""
    # Initialize server
    server = UdfMcpServer(server_name)

    # Register tools for each folder name
    if folder_names:
        for udf_name in folder_names:
            udf_name = udf_name.strip()  # Remove any whitespace
            if udf_name:
                try:
                    # Load UDF from folder
                    folder_path = f"{os.path.abspath(os.curdir)}/udfs/{udf_name}"
                    logger.info(f"Loading UDF from folder: {folder_path}")
                    udf = fused.load(folder_path)

                    # Register the UDF
                    server.register_udf(udf)

                except Exception as e:
                    logger.exception(f"Error loading UDF from folder '{udf_name}': {e}")

    return server


def run_server(
    server_name: str,
    runtime: str = "remote",
    host: str = "0.0.0.0",
    port: int = 8080,
    tokens: List[str] = None,
    udf_names: List[str] = None,
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
        server = create_server_from_folder_names(udf_names, server_name)
    else:
        token_list = tokens if tokens else DEFAULT_TOKENS_TO_READ_FUSED_DOCS
        server = create_server_from_token_ids(token_list, server_name)

    try:
        logger.info(f"Starting MCP server with name: {server_name}")
        if runtime == "local":
            logger.info("Server running in local mode")
            asyncio.run(server.run_stdio())
        else:
            logger.info(f"Server will be available at http://{host}:{port}/sse")
            # Create and run Starlette app with SSE transport
            starlette_app = server.create_starlette_app(debug=True)
            uvicorn.run(starlette_app, host=host, port=port)

    except Exception as e:
        logger.exception(f"Error starting server: {e}")
        sys.exit(1)


def main():
    """Main entry point for the server"""
    parser = argparse.ArgumentParser(description="Run a UDF MCP server")
    parser.add_argument(
        "--tokens",
        required=False,
        help="Comma-separated list of token IDs to register as tools",
    )
    parser.add_argument(
        "--udf-names",
        help="Comma-separated list of UDF (folder) names to register as tools",
    )
    parser.add_argument(
        "--runtime",
        default="remote",
        required=False,
        help="Runtime to use (local, remote)",
    )
    parser.add_argument("--name", default="udf-server", help="Server name")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    # Set up logging based on debug flag
    if args.debug:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")

    # Process token and udf_names arguments
    tokens = args.tokens.split(",") if args.tokens else None
    udf_names = args.udf_names.split(",") if args.udf_names else None

    # Run the server
    run_server(
        server_name=args.name,
        runtime=args.runtime,
        host=args.host,
        port=args.port,
        tokens=tokens,
        udf_names=udf_names,
    )


if __name__ == "__main__":
    main()
