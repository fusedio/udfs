@fused.udf(cache_max_age="0s")
def udf(
    url: str = "https://www.publicschoolreview.com/top-ranked-public-schools",
    search_prompt: str = "best public schools in connecticut",
    extraction_prompt: str = "Extract school names, ranks, and addresses"
):
    """
    Given a URL, find all related links relevant to the query
    """
    import pandas as pd

    try:
        firecrawl_api_key = fused.secrets["FIRECRAWL_API_KEY"]
    except Exception:
        raise RuntimeError("FIRECRAWL_API_KEY environment variable not set.")

    urls = get_relevant_urls(url, search_prompt, firecrawl_api_key)

    best_url = url

    # use llms to get best matching URL from list
    if len(urls):
        urls.append(url)
        open_ai_best = get_best_url(urls, search_prompt)
        best_url = open_ai_best if open_ai_best else urls[0]
    is_paginated = best_url.rstrip('/').split('/')[-1].isdigit()

    print(f"{best_url=}{is_paginated=}")

    # scrape this URL
    scraping_udf = fused.load("scrapegraph_web_scraper")
    return fused.run(scraping_udf, url=best_url, query=extraction_prompt, pagination_pages=3 if is_paginated else 0)


@fused.cache
def get_relevant_urls(base_url: str, query: str, api_key: str) -> (str, bool):
    import requests

    # Find top 20 URLs linked from based_url based on query
    url = "https://api.firecrawl.dev/v2/map"
    payload = {
        "url": base_url,
        "search": query,
        "includeSubdomains": False,
        "limit": 20,
        "timeout": 20*1000,  # in ms
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    json = response.json()
    urls = [r["url"] for r in json["links"]]
    return urls


@fused.cache
def get_best_url(urls: list[str], query: str):
    try:
        import openai
        client = openai.OpenAI(api_key=fused.secrets["openai_fused"])
        # Construct a prompt that asks the model to pick the best URL from the list
        prompt = (
            "Given the user query:\n"
            f"{query}\n"
            "and the following list of URLs, select the most relevant URL (prefer paginated URLs ending with numbers) and return it exactly:\n"
            + "\n".join(urls)
        )
        response = client.chat.completions.create(
            model="gpt-5-nano",
            messages=[{"role": "user", "content": prompt}],
        )
        best_url = response.choices[0].message.content.strip()
        if best_url in urls:
            return best_url
    except Exception as e:
        print("error getting openai suggestions:", e)

    return None
