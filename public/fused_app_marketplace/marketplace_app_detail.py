# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_common = _index._common
_app_facts = _index._app_facts
_catalog_row = _index._catalog_row
_load_index = _index._load_index
MarketError = _common.MarketError
RAW_BASE = _common.RAW_BASE
_answer = _common._answer
_error_rows = _common._error_rows


@fused.udf(cache_max_age="10m")
def udf(slug: str = "sine-starter", client_version: str = ""):
    """One app: its catalog row plus the readme and file list at the approved
    commit, and its releases (newest first). Unknown slug → app_not_found."""
    try:
        idx = _load_index()
    except MarketError as e:
        return _error_rows(e.code, str(e))
    slug = str(slug or "").strip()
    if slug not in idx["apps"]:
        return _error_rows("app_not_found", f'The app "{slug}" isn\'t in the marketplace.')
    row = idx["apps"][slug]
    facts = _app_facts(row["repo"], row["commit"])
    out = _catalog_row(idx, slug, facts)
    out.update({"readme_markdown": (facts or {}).get("readme"),
                "readme_base_url": f"{RAW_BASE}/{row['repo']}/{row['commit']}/",
                "files": (facts or {}).get("files") or []})
    return _answer([out])
