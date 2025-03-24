
@fused.udf
def udf():
    return
def main():
    # Parse command line arguments
    
    import argparse
    import json
    import logging
    import os
    import sys
    from typing import Any, Dict, List

    import httpx
    import requests
    import uvicorn
    from loguru import logger
    from mcp.server import Server, stdio
    from mcp.server.fastmcp import FastMCP
    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.routing import Mount, Route

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # API configuration
    API_BASE_URL = os.environ.get(
        "FUSED_BASE_URL",
        "https://unstable.fused.io/server",
    )
    DEFAULT_TIMEOUT = 30.0
    DEFAULT_USER_AGENT = "fused-api-client/1.0"

    DEFAULT_TOKENS_TO_READ_FUSED_DOCS = [
        "fsh_7lMTze647XbSSUEjBLGNoy",  # Reading fused docs from prod
        "fsh_7hfbTGjNsv4ZzWfHEU1iHm",  # Listing code & name of public UDFs
    ]


    def get_mcp_metadata_from_token(token: str):
        token_url = API_BASE_URL + f"/v1/udf/shared/by-token/{token}"
        print(f"Loading {token=} from {token_url=}")
        try:
            r = requests.get(token_url)
            print(f"Result: {r.status_code=}")
            r.raise_for_status()
            meta = r.json()
            # print(f"UDF: {meta}")

            # checking if we have the expected structure before trying to access
            if "udf_body" in meta:
                try:
                    body_data = json.loads(meta["udf_body"])

                    if "metadata" in body_data and "fused:mcp" in body_data["metadata"]:
                        metadata = body_data["metadata"]["fused:mcp"]
                        print(f"MCP Metadata: {metadata}")

                        # Handle missing description
                        if "description" not in metadata:
                            metadata["description"] = metadata.get(
                                "prompt", f"Function {token}"
                            )

                        return metadata

                    # If we have metadata but no fused:mcp, create a default using any available name
                    if "metadata" in body_data and "fused:name" in body_data["metadata"]:
                        udf_name = body_data["metadata"]["fused:name"]
                        logger.warning(
                            f"No 'fused:mcp' metadata for token '{token}' (name: {udf_name})"
                        )
                        return {"description": f"Function: {udf_name}", "parameters": None}
                except Exception as e:
                    logger.warning(f"Error parsing UDF body: {e}")

            # If we reach here, create a generic default metadata
            logger.warning(
                f"Missing expected metadata structure for token '{token}', using default"
            )
            return {"description": f"Function with token {token}", "parameters": None}

        except requests.exceptions.RequestException as e:
            logger.warning(f"HTTP error for token '{token}': {e}")
            return {"description": f"Function with token {token}", "parameters": None}
        except Exception as e:
            logger.warning(f"Unexpected error for token '{token}': {e}")
            return {"description": f"Function with token {token}", "parameters": None}


    def register_udf_tool(mcp, token_id, metadata):
        """Register a UDF tool using provided metadata."""
        if not metadata:
            logger.warning(
                f"No metadata provided for token '{token_id}', skipping registration"
            )
            return None

        # Log the parameters from metadata
        logger.info(
            f"Registering tool '{token_id}' with parameters: {metadata['parameters']}"
        )

        # Create docstring from metadata
        docstring = f"{metadata['description']}\n\nArgs:\n"

        if metadata["parameters"] is not None:
            # We can have UDFs that don't have any parameters, in which case we assume they'd have 'fused:mcp' to have {"parameters": None}
            logger.info(f"Metadata passed at this point: {metadata=}")
            for param in metadata["parameters"]:
                docstring += (
                    f"    {param['name']}: {param.get('description', 'No description')}\n"
                )
        else:
            logger.info(
                f"No parameters received for {token_id} so skipping creating docstring for parameters"
            )

        async def udf_tool(**params):
            """Dynamic UDF tool implementation."""
            # Log what parameters were received
            logger.info(f"Tool '{token_id}' received parameters: {params}")

            # Initialize API parameters dict
            api_params = {}

            # Copy all parameters directly to the API parameters
            try:
                for key, value in params.items():
                    if key != "return_code":  # Skip non-API parameters
                        api_params[key] = value
                        logger.info(f"Added parameter: {key}={value}")
            except Exception as e:
                api_params = {}
                logger.info(f"No parameters received for {token_id}")

            # Set appropriate output formats for better readability
            api_params.update(
                {
                    "dtype_out_raster": "png",
                    "dtype_out_vector": "json",  # Claude expects JSON?
                }
            )

            logger.info(f"API parameters for {token_id}: {api_params}")

            # Log the final parameters
            logger.info(f"Final API parameters for {token_id}: {api_params}")

            url = f"{API_BASE_URL}/v1/realtime-shared/{token_id}/run/file"
            logger.info(f"Executing API call to {url}")

            try:
                async with httpx.AsyncClient() as client:
                    logger.info(
                        f"Some parameters received for {token_id} so making GET request WITH params"
                    )
                    response = await client.get(
                        url,
                        headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "*/*"},
                        params=api_params,
                        timeout=DEFAULT_TIMEOUT,
                    )
                    response.raise_for_status()

                    logger.info(f"Received response status: {response.status_code}")
                    content_type = response.headers.get("content-type", "")
                    logger.info(f"Response content type: {content_type}")

                    # Handle different response types
                    if (
                        "application/octet-stream" in content_type
                        or "application/parquet" in content_type
                    ):
                        # This is binary data, likely Parquet - don't try to return it as text
                        return "Received binary data response. The data has been processed but cannot be displayed directly in text format."
                    elif "application/json" in content_type or "text/json" in content_type:
                        # Return formatted JSON
                        try:
                            json_data = response.json()
                            return f"Result from function with token {token_id}:\n\n{json.dumps(json_data, indent=2)}"
                        except:
                            return f"Result from function with token {token_id}:\n\n{response.text}"
                    else:
                        # Return as plain text
                        return f"Result from function with token {token_id}:\n\n{response.text}"
            except Exception as e:
                logger.error(f"Error executing function: {e}")
                return f"Error: {str(e)}"

        # Set the docstring
        udf_tool.__doc__ = docstring

        # Register the tool with the token ID as name
        return mcp.tool(name=token_id)(udf_tool)


    def create_server(
        token_ids: List[str] = None,
        server_name: str = "udf-server",
        metadata: dict[str, dict] = None,
    ):
        """Create and configure an MCP server with UDF tools."""
        # Initialize FastMCP server
        mcp = FastMCP(server_name)

        url = f"{API_BASE_URL}/v1/realtime-shared/{token_ids[0]}/run/file"
        logger.info(f"URL to get UDF file: {url}")

        # Register tools for each token ID
        if token_ids and metadata:
            for token_id in token_ids:
                token_id = token_id.strip()  # Remove any whitespace
                if token_id and token_id in metadata:  # Only process tokens with metadata
                    try:
                        logger.info(f"Registering tool for token: {token_id}")
                        logger.info(f"Metadata: {metadata[token_id]}")
                        register_udf_tool(mcp, token_id, metadata[token_id])
                        logger.info(f"Successfully registered tool for token: {token_id}")
                    except Exception as e:
                        logger.error(f"Error registering tool for token '{token_id}': {e}")

        return mcp


    def create_starlette_app(mcp_server: Server, *, debug: bool = False) -> Starlette:
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
 
 
 
    parser = argparse.ArgumentParser(description="Run an MCP server with UDF tools")
    parser.add_argument(
        "--tokens",
        required=False,
        help="Comma-separated list of token IDs to register as tools",
    )
    parser.add_argument(
        "--runtime", required=False, help="Runtime to use (local, remote)"
    )
    parser.add_argument("--name", default="udf-server", help="Server name")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--base-url", help="Override the API base URL")
    args = parser.parse_args()

    # Update base URL if provided
    # global API_BASE_URL
    if args.base_url:
        API_BASE_URL = args.base_url

    # Use default tokens if none provided
    if args.tokens:
        token_list = args.tokens.split(",")
    else:
        # Register both tools by default
        # token_list = ["fsh_2jzKsQRPM17SOl5zbyXVp4", "fsh_7fyUF3LvqqLChEf2A3euun"]
        token_list = DEFAULT_TOKENS_TO_READ_FUSED_DOCS

    # Fetch metadata for all tokens
    udf_metadata = {}
    for token in token_list:
        token_metadata = get_mcp_metadata_from_token(token)
        if token_metadata:
            udf_metadata[token] = token_metadata

    try:
        # Log the API base URL being used
        logger.info(f"Using API base URL: {API_BASE_URL}")
        logger.info(f"Available UDF metadata: {', '.join(udf_metadata.keys())}")

        # Create MCP server with specified tokens and metadata
        mcp_instance = create_server(token_list, args.name, udf_metadata)

        if args.runtime == "local":
            # starting mcp server
            mcp_instance.run("stdio")
        else:
            logger.info(f"Starting MCP server with name: {args.name}")
            logger.info(f"Tokens registered: {token_list}")
            logger.info(
                f"Server will be available at http://{args.host}:{args.port}/sse"
            )

            # Create and run Starlette app with SSE transport
            mcp_server = mcp_instance._mcp_server
            starlette_app = create_starlette_app(mcp_server, debug=True)
            uvicorn.run(starlette_app, host=args.host, port=args.port)

    except Exception as e:
        logger.error(f"Error starting server: {e}")
        sys.exit(1)

