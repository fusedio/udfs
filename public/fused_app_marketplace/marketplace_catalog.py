import os

# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_stats = _index._stats
_common = _index._common
_catalog_row = _index._catalog_row
_facts_for = _index._facts_for
_load_index = _index._load_index
_persist_catalog = _index._persist_catalog
_sorted_slugs = _index._sorted_slugs
_stats_all = _stats._stats_all
MarketError = _common.MarketError
_answer = _common._answer
_error_rows = _common._error_rows


@fused.udf(cache_max_age="10m")
def udf(client_version: str = ""):
    """The reviewed marketplace catalog: one row per app listed in the index
    (apps/<slug>.json), pinned to its approved commit. Serve with
    ?format=json. Error = one row {error_code, error_message}."""
    import time
    t0 = time.monotonic()
    timing = {}
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))
    timing["load_index"] = round(time.monotonic() - t0, 3)
    t = time.monotonic()
    _stats_all()
    timing["stats_all"] = round(time.monotonic() - t, 3)
    slugs = _sorted_slugs(idx)
    t = time.monotonic()
    facts = _facts_for(idx, slugs)
    timing["facts_for"] = round(time.monotonic() - t, 3)
    t = time.monotonic()
    rows = [_catalog_row(idx, s, facts.get(s)) for s in slugs]
    timing["rows"] = round(time.monotonic() - t, 3)
    t = time.monotonic()
    _persist_catalog(idx, rows)
    timing["persist_catalog"] = round(time.monotonic() - t, 3)
    timing["total"] = round(time.monotonic() - t0, 3)
    print(f"marketplace_catalog timing {timing}")
    if str(client_version).startswith("timing"):
        return _answer([{"timing": timing, "pid": os.getpid()}])
    return _answer(rows)
