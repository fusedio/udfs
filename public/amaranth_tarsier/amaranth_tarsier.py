@fused.udf
def udf(
    url: str = "https://www.publicschoolreview.com/top-ranked-public-schools/new-york/tab/all/num/2",
    query: str = "Extract school names, ranks, and addresses for all NYC schools",
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
    api_key = fused.secrets["SCRAPEGRAPH_API_KEY"]


    # ------------------------------------------------------------------
    # Build request payload and headers
    # ------------------------------------------------------------------
    payload = {"website_url": url, "user_prompt": query}
    print("🔧 Payload being sent to Scrapegraph.ai:")
    print(payload)

    headers = {
        "accept": "application/json",
        "SGAI-APIKEY": api_key,
        "Content-Type": "application/json",
    }

    # ------------------------------------------------------------------
    # Perform POST request – on any error return an empty DataFrame
    # ------------------------------------------------------------------
    try:
        response = requests.post(
            "https://api.scrapegraphai.com/v1/smartscraper",
            headers=headers,
            json=payload,
            timeout=30,
        )
        print(f"📡 HTTP response status: {response.status_code}")

        response.raise_for_status()
        data = response.json()
        print("✅ JSON payload received from API:")
        print(data)

        # --------------------------------------------------------------
        # The API returns a dict with a `result` key that holds the records.
        # The top‑level key inside `result` may vary (e.g. "schools").
        # We locate the first list value and treat that as the rows.
        # --------------------------------------------------------------
        result = data.get("result", {})
        print(f"🔎 Type of `result` object: {type(result)}")

        # Find the first list inside the result dict (or use result directly if already a list)
        if isinstance(result, dict):
            # pick the first value that is a list
            records = next((v for v in result.values() if isinstance(v, list)), [])
            print(f"📂 Detected dict result – extracted first list with {len(records)} records.")
        elif isinstance(result, list):
            records = result
            print(f"📂 Result is already a list with {len(records)} records.")
        else:
            records = []
            print("⚠️ Unexpected result format – falling back to empty list.")

        # Build the DataFrame from the list of dicts
        df = pd.DataFrame(records)
        print(f"🗂️ DataFrame shape after conversion: {df.shape}")

    except Exception as e:  # pragma: no cover
        print("❗ Exception encountered while calling Scrapegraph.ai:")
        print(e)
        df = pd.DataFrame()  # empty DF on failure
        print("⚠️ Returning empty DataFrame.")

    return df
