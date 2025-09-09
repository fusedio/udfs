@fused.udf(cache_max_age="0s")
def udf(
    urls: list[str] = [
        "https://www.publicschoolreview.com/top-ranked-public-schools/new-york/tab/all/num/1",
        "https://www.publicschoolreview.com/top-ranked-public-schools/new-york/tab/all/num/2",
        "https://www.publicschoolreview.com/top-ranked-public-schools/new-york/tab/all/num/3",
    ],
    query: str = "Extract school names, ranks, and addresses for all NYC schools",
    pagination_pages: int | None = None,
    scroll_pages: int | None = None,
):
    """
    Given a list of URLs, scrape them based on a common output schema and return combined output
    """
    import pandas as pd

    try:
        api_key = fused.secrets["SCRAPEGRAPH_API_KEY"]
    except Exception:
        raise RuntimeError(
            "SCRAPEGRAPH_API environment variable not set. " "Please set it to your Scrapegraph.ai API key."
        )

    try:
        schema = generate_schema(query, api_key)
        scraper = fused.load("team/scrapegraph_web_scraper")
        res = fused.submit(
            scraper,
            [
                {
                    "url": url,
                    "query": query,
                    "output_schema": schema,
                    "pagination_pages": pagination_pages or 0,
                    "scroll_pages": scroll_pages or 0,
                }
                for url in urls
            ],
        )
        print(f"{res.shape=}")
        return res

    except Exception as e:  # pragma: no cover
        print("❗ Exception encountered while scraping")
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
        timeout=20,
    )

    response.raise_for_status()
    data = response.json()
    schema = data.get("generated_schema", {})

    return schema
