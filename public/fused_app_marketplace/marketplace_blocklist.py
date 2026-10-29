# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_common = _index._common
_index_stamp = _index._index_stamp
_load_index = _index._load_index
MarketError = _common.MarketError
_answer = _common._answer
_error_rows = _common._error_rows


@fused.udf(cache_max_age="10m")
def udf():
    """The index's blocklist.json as ONE self-describing row (§18.3,
    contract 5): {generated_at, catalog_version, stale, entries: [{slug,
    commit, level, reason, what_to_do, at, malformed}]}. Always exactly one
    row, also when nothing is blocked (`entries: []`), so a reader can tell
    this shape from the old one-row-per-entry list. `stale: true` = the last
    good list S3 kept (GitHub unreadable, or main's blocklist.json broken):
    keep the previous list. Entries are never dropped: a level other than
    "soft" is "hard"; a malformed slug/commit says `malformed: true`."""
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))

    def s(b, k):
        return b.get(k) if isinstance(b.get(k), str) else None

    entries = [{"slug": s(b, "slug"), "commit": s(b, "commit"),
                "level": "soft" if b.get("level") == "soft" else "hard",
                "reason": s(b, "reason"), "what_to_do": s(b, "what_to_do"), "at": s(b, "at"),
                "malformed": bool(b.get("malformed"))}
               for b in idx["blocked"] if isinstance(b, dict)]
    return _answer([{**_index_stamp(idx), "entries": entries}])
