# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_stats = _index._stats
_common = _index._common
_block_level = _index._block_level
_index_stamp = _index._index_stamp
_load_index = _index._load_index
_record_event = _stats._record_event
_stats_refresh = _stats._stats_refresh
MarketError = _common.MarketError
_SLUG_RE = _common._SLUG_RE
_answer = _common._answer
_error_rows = _common._error_rows


@fused.udf(cache_max_age="0s")
def udf(slug: str = "sine-starter"):
    """fused-render asks this when it STARTS an install: the reviewed row as
    the index has it right now (repo, commit, version, block level) — the
    client's own catalog copy can be minutes old — and the ask is counted as
    one install (§18.12). The request carries the slug and nothing else; the
    event kept is {slug, commit, at}. Serve with ?format=json; error = one
    row {error_code, error_message} like every marketplace UDF."""
    slug = str(slug or "").strip()
    if not _SLUG_RE.fullmatch(slug):
        return _error_rows("bad_slug", "That isn't a marketplace slug.")
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))
    row = idx["apps"].get(slug)
    if row is None:
        return _error_rows("app_not_found", f'The app "{slug}" isn\'t in the marketplace.')
    out = {"slug": slug, "name": row["name"], "repo": row["repo"], "commit": row["commit"],
           "release_id": row["commit"], "branch": row["branch"], "version": row["version"],
           "blocked": _block_level(idx, slug, row["commit"]),
           **_index_stamp(idx)}
    out["counted"] = _record_event("installs", slug, row["commit"])
    _stats_refresh()
    return _answer([out])
