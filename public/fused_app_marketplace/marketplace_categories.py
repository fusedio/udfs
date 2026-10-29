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
    """The index's categories.json with live app counts, by `order`."""
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))
    counts = {}
    for row in idx["apps"].values():
        counts[row["category"]] = counts.get(row["category"], 0) + 1

    def order(c):
        o = c.get("order")
        return o if isinstance(o, (int, float)) and not isinstance(o, bool) else 0

    return _answer([{"id": c["id"], "label": c.get("label") if isinstance(c.get("label"), str) else c["id"],
                     "order": order(c), "count": counts.get(c["id"], 0),
                     **_index_stamp(idx)}
                    for c in sorted(idx["categories"], key=order)])
