# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_common = _index._common
_catalog_row = _index._catalog_row
_facts_for = _index._facts_for
_load_index = _index._load_index
_sorted_slugs = _index._sorted_slugs
MarketError = _common.MarketError
_answer = _common._answer
_error_rows = _common._error_rows


@fused.udf(cache_max_age="10m")
def udf(q: str = "", category: str = "", cursor: str = "", limit: int = 24, client_version: str = ""):
    """Search the catalog: every word of `q` must match the name/slug, a tag
    or the description (case-insensitive substring); name beats tags beats
    description. `cursor` is an offset; `next_cursor` is on every row."""
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))
    try:
        limit = max(1, min(100, int(limit)))
    except (TypeError, ValueError):
        limit = 24
    cursor = str(cursor or "").strip()
    offset = int(cursor) if cursor.isdigit() else 0
    terms = [t for t in str(q or "").casefold().split() if t]
    hits = []
    for slug in _sorted_slugs(idx):
        row = idx["apps"][slug]
        if category and row["category"] != category:
            continue
        score, highlights = _score(row, terms)
        if score is not None:
            hits.append((score, slug, highlights))
    hits.sort(key=lambda h: -h[0])  # stable: ties keep name order
    page = hits[offset:offset + limit]
    nxt = str(offset + limit) if offset + limit < len(hits) else None
    facts = _facts_for(idx, [s for _sc, s, _h in page])
    out = []
    for i, (score, slug, highlights) in enumerate(page):
        r = _catalog_row(idx, slug, facts.get(slug))
        r.update({"rank": offset + i + 1, "score": score, "highlights": highlights, "next_cursor": nxt})
        out.append(r)
    return _answer(out, columns=["slug", "rank", "score", "highlights", "next_cursor"])


def _score(row, terms):
    """(score, highlights), or (None, []) when a term matches nothing."""
    if not terms:
        return 0, []
    name = f"{row['name']} {row['slug']}".casefold()
    tags = [t.casefold() for t in row["tags"]]
    desc = row["description"].casefold()
    score, fields = 0, {}
    for t in terms:
        if t in name:
            score += 3 + (2 if row["name"].casefold() == t or row["slug"] == t
                          else 1 if name.startswith(t) else 0)
            fields.setdefault("name", row["name"])
        elif any(t in tag for tag in tags):
            score += 2
            fields.setdefault("tags", ", ".join(row["tags"]))
        elif t in desc:
            score += 1
            at = desc.index(t)
            lo, hi = max(0, at - 60), min(len(desc), at + len(t) + 60)
            snip = row["description"][lo:hi]
            fields.setdefault("description", ("…" if lo else "") + snip + ("…" if hi < len(desc) else ""))
        else:
            return None, []
    return score, [{"field": k, "snippet": v} for k, v in fields.items()]
