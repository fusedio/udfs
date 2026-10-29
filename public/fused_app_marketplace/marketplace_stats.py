from datetime import date, timedelta

# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_stats = _index._stats
_common = _index._common
_index_stamp = _index._index_stamp
_load_index = _index._load_index
_sorted_slugs = _index._sorted_slugs
_stats_all = _stats._stats_all
_stats_refresh = _stats._stats_refresh
MarketError = _common.MarketError
STATS_KINDS = _common.STATS_KINDS
STATS_WINDOW_DAYS = _common.STATS_WINDOW_DAYS
_S3_ERROR = _common._S3_ERROR
_answer = _common._answer
_error_rows = _common._error_rows
_s3_read_json = _common._s3_read_json
_utc_now = _common._utc_now


@fused.udf(cache_max_age="10m")
def udf():
    """Install and view counts per listed app (§18.12): one row per slug in the
    index — installs (fused-render asked fused_app_install), views (the
    public listing page was served), each all-time and over the last 30
    days — from the event store under S3_PREFIX/stats/. Serve with
    ?format=json. Error = one row {error_code, error_message}."""
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))
    _stats_refresh()
    st = _stats_all()
    week = {k: _last_days(k, 7) for k in STATS_KINDS}
    rows = []
    for slug in _sorted_slugs(idx):
        row = idx["apps"][slug]
        rows.append({"slug": slug, "name": row["name"], "version": row["version"],
                     "installs": int(st["installs"]["totals"].get(slug, 0)),
                     "installs_7d": int(week["installs"].get(slug, 0)),
                     "installs_30d": int(st["installs"]["window"].get(slug, 0)),
                     "views": int(st["views"]["totals"].get(slug, 0)),
                     "views_7d": int(week["views"].get(slug, 0)),
                     "views_30d": int(st["views"]["window"].get(slug, 0)),
                     "window_days": STATS_WINDOW_DAYS, **_index_stamp(idx)})
    return _answer(rows, columns=["slug", "name", "version", "installs", "installs_7d",
                                  "installs_30d", "views", "views_7d", "views_30d",
                                  "window_days", "catalog_version", "generated_at", "stale"])


def _last_days(kind, n):
    """{slug: count} over the last `n` UTC days (today included), summed from
    the per-day map the rollup keeps in stats/<kind>/current.json. The
    community home's Trending tab sorts by the 7-day install count. An
    unreadable or missing file answers {} (every count 0), never an error."""
    cur = _s3_read_json(f"stats/{kind}/current.json")
    if cur is _S3_ERROR or not isinstance(cur, dict):
        return {}
    cutoff = _utc_now().date() - timedelta(days=n - 1)
    out = {}
    for d, m in (cur.get("days") or {}).items():
        if not isinstance(m, dict):
            continue
        try:
            if date.fromisoformat(d) < cutoff:
                continue
        except ValueError:
            continue
        for slug, c in m.items():
            out[slug] = out.get(slug, 0) + int(c or 0)
    return out
