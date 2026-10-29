# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_common = _index._common
_index_stamp = _index._index_stamp
_load_index = _index._load_index
_releases = _index._releases
MarketError = _common.MarketError
_answer = _common._answer
_error_rows = _common._error_rows


@fused.udf(cache_max_age="10m")
def udf(new_and_updated_limit: int = 12):
    """Home shelves: collections.json's featured list and shelves, plus a
    synthesized "new_and_updated" shelf (apps by newest release date)."""
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))
    try:
        limit = max(0, min(50, int(new_and_updated_limit)))
    except (TypeError, ValueError):
        limit = 12
    apps, v = idx["apps"], _index_stamp(idx)
    rows = [{"collection": "featured", "shelf_id": None, "title": "Featured", "position": i,
             "slug": s, **v}
            for i, s in enumerate([s for s in idx["featured"] if s in apps])]
    for shelf in idx["shelves"]:
        title = shelf.get("title") if isinstance(shelf.get("title"), str) else shelf["id"]
        slugs = [s for s in shelf.get("slugs") or [] if isinstance(s, str) and s in apps]
        rows += [{"collection": "shelf", "shelf_id": shelf["id"], "title": title, "position": i,
                  "slug": s, **v} for i, s in enumerate(slugs)]
    dated = []
    for s in apps:
        ats = [r["at"] for r in _releases(idx, s) if r["at"]]
        if ats:
            dated.append((max(ats), s))
    dated.sort(reverse=True)
    rows += [{"collection": "new_and_updated", "shelf_id": None, "title": "New and updated",
              "position": i, "slug": s, **v}
             for i, (_at, s) in enumerate(dated[:limit])]
    return _answer(rows)
