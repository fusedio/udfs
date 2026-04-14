# ===================== CONFIGURATION =====================
SYSTEM_PROMPT = """You are a helpful data assistant. Be concise and friendly.
You have access to tools that query real data — always use them to answer questions.
If the available tools do not cover the user's question, say you cannot answer from the connected data.
Do not answer from general knowledge when no relevant tool exists.
For formatting response, always make sure Slack can render things properly:
- Only use a single * for *bold* not **double bold**. This will not render properly
- Write in _italic_ with underscores
- Use '-' to make bullet points, not '*'
Use markdown embeded links like [like this](https://fused.io) when answering with links"""
# =========================================================


@fused.udf(cache_max_age=0)
def udf(
    prompt: str = "Where can I start with Fused if I only have 5min?",
    thread_context: str = "",
    canvas_token: str = "fc_UPF2QNZmfRPnJ9fWoG7rC",
):
    import json
    import requests
    from urllib.parse import urlencode
    from job2 import ai
    from job2.ai import AiModel

    # NOTE: remove this when we're done with debugging
    # import logging
    # from logging import getLogger
    # file_handler = logging.FileHandler("/mount/canvas_bot.log")
    # logger = getLogger("LiteLLM")
    # logger.addHandler(file_handler)

    # try: 
    #     fused.secrets["openrouter_api_key"]
    # except:
    #     return "Not implemented without openrouter api key yet. Coming soon"
    # ai.configure(openrouter_api_key=fused.secrets["openrouter_api_key"])

    CANVAS_API_URL = f"https://unstable.udf.ai/{canvas_token}.api.json" # Need to change this to use udf.ai instead when deploying

    spec = _fetch_spec(CANVAS_API_URL)
    base_url = spec["servers"][0]["url"]
    tools, tool_routes = _spec_to_tools(spec)

    print(f"Canvas: {spec['info']['title']}")
    available_tools = [t['function']['name'] for t in tools]
    print(f"Tools: {available_tools}")
    tool_calls = []

    def tool_handler(name: str, params: dict):
        print(f"[Tool] {name}({params})")
        tool_calls.append({"name": name, "params": params})
        route = tool_routes.get(name)
        if not route:
            return f"Unknown tool: {name}"
        url_params = {k: v for k, v in params.items() if v is not None}
        url_params["format"] = "json"
        url = f"{base_url}{route}?{urlencode(url_params)}"
        print(f"[Tool] GET {url}")
        resp = requests.get(
            url, 
            timeout=30,
            headers={"Authorization": f"{fused.api.auth_scheme()}  {fused.api.access_token()}"}
        )
        result = resp.text[:8000]
        print(f"[Tool] Response: {len(resp.text)} chars")
        return result

    canvas_description = f"Canvas: {spec['info']['title']}\n"
    canvas_description += "Available tools and their purposes:\n"
    for t in tools:
        canvas_description += f"- {t['function']['name']}: {t['function'].get('description', '')[:100]}\n"

    system = SYSTEM_PROMPT + "\n\n" + canvas_description
    if thread_context:
        system += (
            "\n\n--- CONVERSATION HISTORY (background context only) ---\n"
            + thread_context
            + "\n--- END HISTORY ---\n\n"
            "The user's new message is below. "
            "You MUST call tools to get fresh data — never reuse numbers from the history above."
        )

    print(f"[Prompt] {prompt}")

    response = ai.run(
        prompt,
        system_prompt=system,
        model=AiModel.GPT_OSS_120B,
        tools=tools,
        tool_handler=tool_handler,
    )

    if not tool_calls:
        return (
            f"Here are the available tools I have access to: \n{available_tools=}"
            "\nPlease Ask me something covered by the available tools."
        )

    return response.text


@fused.cache
def _fetch_spec(api_url):
    import requests
    resp = requests.get(
        api_url, 
        timeout=15,
        headers={"Authorization": f"{fused.api.auth_scheme()}  {fused.api.access_token()}"}
    )
    resp.raise_for_status()
    return resp.json()


def _spec_to_tools(spec):
    """Convert OpenAPI spec paths into LLM tool definitions."""
    tools = []
    tool_routes = {}
    skip_params = {"cache_max_age", "format"}

    for path, methods in spec.get("paths", {}).items():
        for method, operation in methods.items():
            if method != "get":
                continue

            op_id = operation.get("operationId", path.strip("/"))
            name = op_id.replace("run_", "", 1)
            description = operation.get("description", operation.get("summary", ""))

            properties = {}
            for param in operation.get("parameters", []):
                if "$ref" in param:
                    continue
                pname = param.get("name", "")
                if pname in skip_params:
                    continue
                schema = param.get("schema", {})
                prop = {"type": schema.get("type", "string")}
                if "default" in schema:
                    prop["description"] = f"Default: {schema['default']}"
                properties[pname] = prop

            tool_def = {
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": {
                        "type": "object",
                        "properties": properties,
                    },
                },
            }
            tools.append(tool_def)
            tool_routes[name] = path

    return tools, tool_routes
