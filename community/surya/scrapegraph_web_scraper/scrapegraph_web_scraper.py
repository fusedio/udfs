@fused.udf
def udf(
    url: str = "https://www.usnews.com/best-colleges/rankings/national-universities",
    query: str = "Return the school names, tuition fees & number of enrolled ggraduates",
    output_schema: dict | None = None,
    pagination_pages: int | None = None,
    scroll_pages: int | None = None
):
    """
    Raw Scrapegraph.ai smart‑scraper call.

    Parameters
    ----------
    url : str
        URL of the page to scrape.
    query : str, optional
        Prompt sent to the smart‑scraper. Defaults to extracting NYC school
        names, ranks and addresses.
    output_schema: str, optional
        Json schema for the expected results. If none, will use scrapegraph.ai schema creation tool
    pagination_pages: int, optional
        Number of pages to paginate through when the target site uses pagination. Default is None
    scroll_pages: int, optional
        Number of scroll actions to perform for infinite scrolling pages. Default is None

    Returns
    -------
    pandas.DataFrame
        DataFrame where each row corresponds to a single record from the API
        result. If the request fails, an empty DataFrame is returned.
    """
    import pandas as pd
    import requests

    # ------------------------------------------------------------------
    # API key – required for the Scrapegraph.ai endpoint
    # ------------------------------------------------------------------
    try:
        api_key = fused.secrets["SCRAPEGRAPH_API_KEY"]
    except Exception:
        raise RuntimeError(
            "SCRAPEGRAPH_API environment variable not set. "
            "Please set it to your Scrapegraph.ai API key."
        )

    # Auto generate output schema with scrapegraph.ai if non is given
    output_schema = output_schema or generate_schema(query, api_key)

    # ------------------------------------------------------------------
    # Build request payload
    # ------------------------------------------------------------------
    payload = {"website_url": url, "user_prompt": query, "output_schema": output_schema}

    if pagination_pages:
        payload["total_pages"] = pagination_pages
    if scroll_pages:
        payload["number_of_scrolls"] = scroll_pages
    if pagination_pages and scroll_pages:
        raise RuntimeError("Cannot enable both pagination_pages and scroll_pages at the same time")

    print("🔧 Payload being sent to Scrapegraph.ai:")
    print(payload)

    # ------------------------------------------------------------------
    # Perform POST request – on any error return an empty DataFrame
    # ------------------------------------------------------------------
    try:
        data = smart_scrape(payload, api_key)
        pagination_enabled = pagination_pages is not None or scroll_pages is not None
        records = process_results(data, pagination_enabled)

        # Build the DataFrame from the list of dicts
        df = pd.DataFrame(records)
        print(f"🗂️ DataFrame shape after conversion: {df.shape}")

    except Exception as e:  # pragma: no cover
        print("❗ Exception encountered while calling Scrapegraph.ai:")
        print(e)
        df = pd.DataFrame()  # empty DF on failure
        print("⚠️ Returning empty DataFrame.")

    return df


@fused.cache
def generate_schema(query: str, api_key: str):
    import requests

    payload = {"user_prompt": query}
    headers = {
        "accept": "application/json",
        "SGAI-APIKEY": api_key,
        "Content-Type": "application/json",
    }

    response = requests.post(
        "https://api.scrapegraphai.com/v1/generate_schema",
        headers=headers,
        json=payload,
        timeout=50,
    )

    response.raise_for_status()
    data = response.json()
    schema = data.get("generated_schema", {})

    return schema


@fused.cache
def smart_scrape(payload: dict, api_key: str):
    import json
    import requests
    headers = {
        "accept": "application/json",
        "SGAI-APIKEY": api_key,
        "Content-Type": "application/json",
    }
    response = requests.post(
        "https://api.scrapegraphai.com/v1/smartscraper",
        headers=headers,
        json=payload,
        timeout=100,
    )
    print(f"📡 HTTP response status: {response.status_code}")

    response.raise_for_status()
    data = response.json()
    print("✅ JSON payload received from API:")
    print(data)
    return data


def process_results(data: dict, pagination_enabled: bool):
    # --------------------------------------------------------------
    # The API returns a dict with a `result` key that holds the records.
    # The top‑level key inside `result` may vary (e.g. "schools").
    # We locate the first list value and treat that as the rows.
    # --------------------------------------------------------------
    result = data.get("result", {})

    if pagination_enabled and "pages" in result:
        # we have multiple pages of data
        # process them individually and concat them together
        pages = result["pages"]
        records = []
        for i, page in enumerate(pages, start=1):
            print(f"Processing page {i}")
            inner_res = process_results(page, False)
            records.append(inner_res)
        return inner_res

    print(f"🔎 Type of `result` object: {type(result)}")

    # Find the first list inside the result dict (or use result directly if already a list)
    if isinstance(result, dict):
        # pick the first value that is a list
        records = next(
            (v for v in result.values() if isinstance(v, list)), []
        )
    elif isinstance(result, list):
        records = result
    else:
        records = []
        print("⚠️ Unexpected result format – falling back to empty list.")

    return records
