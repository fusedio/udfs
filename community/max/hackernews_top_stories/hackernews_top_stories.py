@fused.udf
def udf(topic: str = "openai"):
    """
    Search Hacker News for stories that match the given `topic` **and** its
    automatically‑generated synonyms.

    For the special case `ai` we return a curated list of terms
    (e.g., “artificial intelligence”, “machine learning”, …)
    to avoid unrelated results like “three‑toed sloth”.

    Results are returned as a Pandas DataFrame sorted by points,
    limited to the top 20 stories.
    """
    import requests
    import pandas as pd
    from datetime import datetime

    # ------------------------------------------------------------------
    # 1️⃣  Helper: fetch “means‑like” words (ml) – cached per term
    # ------------------------------------------------------------------
    @fused.cache
    def _ml_terms(term: str, max_terms: int = 5) -> list[str]:
        """Datamuse `ml` query – returns words that are similar in meaning."""
        try:
            resp = requests.get(
                "https://api.datamuse.com/words",
                params={"ml": term, "max": max_terms},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return [item["word"] for item in data if "word" in item]
        except Exception as e:
            print(f"ml‑term fetch error for '{term}': {e}")
            return []

    # ------------------------------------------------------------------
    # 2️⃣  Helper: fetch exact synonyms (syn) – cached per term
    # ------------------------------------------------------------------
    @fused.cache
    def _syn_terms(term: str, max_terms: int = 5) -> list[str]:
        """Datamuse `syn` query – returns exact synonyms."""
        try:
            resp = requests.get(
                "https://api.datamuse.com/words",
                params={"syn": term, "max": max_terms},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return [item["word"] for item in data if "word" in item]
        except Exception as e:
            print(f"syn‑term fetch error for '{term}': {e}")
            return []

    # ------------------------------------------------------------------
    # 3️⃣  Helper: combine ml + syn, deduplicate – cached
    # ------------------------------------------------------------------
    @fused.cache
    def _combined_synonyms(term: str, max_per_call: int = 5) -> list[str]:
        """
        For most terms: call both `ml` and `syn` endpoints, merge,
        de‑duplicate, and return a list of unique terms.

        For the special case `ai` we return a curated list of
        relevant phrases to avoid unrelated results.
        """
        # ---- special‑case handling for “ai” ---------------------------------
        if term.lower() == "ai":
            # Keep the original term and a few well‑known expansions.
            return [
                "ai",
                "artificial intelligence",
                "machine learning",
                "deep learning",
                "AI",
                "artificial intelligence",
                "machine learning",
                "deep learning",
            ]

        # ---- default behaviour (ml + syn) ---------------------------------
        ml = _ml_terms(term, max_per_call)
        syn = _syn_terms(term, max_per_call)

        # Preserve order: original term → ml results → syn results
        combined = [term] + ml + syn

        # Remove duplicates while preserving first occurrence
        seen = set()
        uniq = []
        for w in combined:
            if w not in seen:
                seen.add(w)
                uniq.append(w)
        return uniq

    # ------------------------------------------------------------------
    # 4️⃣  Helper: fetch HN stories for a single query (cached)
    # ------------------------------------------------------------------
    @fused.cache
    def _search_hn(query: str, limit: int = 10) -> list[dict]:
        """Call the Algolia HN API and return the raw list of hits."""
        url = "https://hn.algolia.com/api/v1/search"
        params = {
            "query": query,
            "tags": "story",
            "numericFilters": "created_at_i>0",
            "page": 0,
            "hitsPerPage": limit,
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json().get("hits", [])
        except Exception as e:
            print(f"Error fetching HN data for '{query}': {e}")
            return []

    # ------------------------------------------------------------------
    # 5️⃣  Build the list of queries (original + combined synonyms)
    # ------------------------------------------------------------------
    base_term = topic.strip()
    queries = _combined_synonyms(base_term, max_per_call=5)

    # Debug output – shows which queries are being executed
    print(f"Queries being executed: {queries}")

    # ------------------------------------------------------------------
    # 6️⃣  Gather results from all queries, deduplicate by objectID
    # ------------------------------------------------------------------
    all_hits: dict[str, dict] = {}
    for q in queries:
        for hit in _search_hn(q):
            obj_id = hit.get("objectID")
            if obj_id and obj_id not in all_hits:
                all_hits[obj_id] = hit

    # ------------------------------------------------------------------
    # 7️⃣  Convert to a DataFrame (include tags for categorisation)
    # ------------------------------------------------------------------
    if not all_hits:
        # Friendly empty DataFrame when nothing is found
        return pd.DataFrame(
            {
                "title": [f'No stories found for "{topic}"'],
                "url": [""],
                "author": [""],
                "points": [0],
                "comments": [0],
                "created_at": [""],
                "tags": [""],
            }
        )

    rows = []
    for obj_id, story in all_hits.items():
        rows.append(
            {
                "title": story.get("title", "No title"),
                "url": story.get(
                    "url",
                    f"https://news.ycombinator.com/item?id={obj_id}",
                ),
                "author": story.get("author", "Unknown"),
                "points": story.get("points", 0),
                "comments": story.get("num_comments", 0),
                "created_at": datetime.fromtimestamp(
                    story.get("created_at_i", 0)
                ).strftime("%Y-%m-%d %H:%M"),
                "tags": ", ".join(story.get("tags", [])),
            }
        )

    df = pd.DataFrame(rows)

    # Sort by popularity (points) – most interesting stories first
    df = df.sort_values("points", ascending=False).reset_index(drop=True)

    # Return only the top 20 stories
    df = df.head(20)

    return df