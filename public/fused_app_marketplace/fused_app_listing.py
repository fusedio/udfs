import html as _html
from datetime import datetime

# fused-render deep link that installs the reviewed copy of a listing.
FUSED_RENDER_MARKET_LINK = "fused-render://open?marketplace={slug}"
FUSED_RENDER_DOWNLOAD_PAGE = "https://www.fused.io/render"

# Shared helpers live in the utils folder. market_index loads
# market_stats_store, which loads market_common; take all three from
# market_index so each exists once (MarketError and the S3 sentinels are
# compared by identity).
_index = fused.load("market_index")
_stats = _index._stats
_common = _index._common
_app_facts = _index._app_facts
_catalog_row = _index._catalog_row
_load_index = _index._load_index
_record_event = _stats._record_event
_stats_refresh = _stats._stats_refresh
MarketError = _common.MarketError
RAW_BASE = _common.RAW_BASE

# The landing page look, shared with fused_app_github.
_ui = fused.load("app_page_ui")
_ARROW = _ui._ARROW
_JS = _ui._JS
_VERIFIED = _ui._VERIFIED
_callout = _ui._callout
_cat_label = _ui._cat_label
_cell = _ui._cell
_compact = _ui._compact
_head = _ui._head
_human = _ui._human
_info_row = _ui._info_row
_js = _ui._js
_state_page = _ui._state_page


@fused.udf(cache_max_age="0s")
def udf(slug: str = "sine-starter"):
    """The public landing page of a reviewed marketplace listing: the app at
    its APPROVED commit (readme, icon, preview, files), the review line, What's
    new, remix lineage, contributors, and the install deep link."""
    slug = str(slug or "").strip()
    try:
        idx = _load_index()
    except MarketError as e:
        return _listing_error(slug, str(e))
    if slug not in idx["apps"]:
        return _listing_error(slug, "This app isn't in the Fused Community (it may have been delisted).")
    row = idx["apps"][slug]
    facts = _app_facts(row["repo"], row["commit"])
    app = _catalog_row(idx, slug, facts)
    _record_event("views", slug, row["commit"])
    _stats_refresh()
    remix = idx["apps"].get(app["remix_of"]) if app["remix_of"] else None
    block = next((b for b in idx["blocked"] if b["slug"] == slug and b["commit"] == row["commit"]
                  and b["level"] == app["blocked"]), None) if app["blocked"] else None
    return _listing_page(app, facts or {}, remix, block)


def _fmt_date(iso):
    try:
        return datetime.strptime(iso[:10], "%Y-%m-%d").strftime("%b %-d, %Y")
    except Exception:
        return None


def _listing_page(app, facts, remix, block):
    """The store's detail page as standalone HTML: hero (icon, name + reviewed
    tick, description, author, tags, Install), the block notice, the info
    strip, the preview, then README / files / releases beside the Information
    list."""
    e = _html.escape

    def q(s):
        return e(str(s), quote=True)

    repo, commit, slug = app["repo"], app["commit"], app["slug"]
    title = e(app["name"])
    tree = f"https://github.com/{repo}/tree/{commit}"
    hard = block is not None and block["level"] == "hard"
    author = app.get("author") or {}
    owner = repo.split("/")[0]
    aname = e(author.get("name") or owner)
    agh = author.get("github") or owner
    author_html = f'<a href="https://github.com/{q(agh)}" target="_blank" rel="noopener">{aname}</a>'
    remix_link = f'<a href="?slug={q(remix["slug"])}">{e(remix["name"])}</a>' if remix else ""
    byline = f"by {author_html}" + (f" · Remix of {remix_link}" if remix_link else "")
    icon = (f'<img alt="" src="{q(app["icon_url"])}">' if app["icon_url"] else ".fused")
    tags = [t for t in app.get("tags") or [] if t and t != app["category"]]
    tags_html = ('<ul class="tags">' + "".join(f'<li class="tag">{e(t)}</li>' for t in tags) + "</ul>") if tags else ""
    released = _fmt_date(app["last_release_at"] or "")
    installs = int(app.get("installs") or 0)
    install_txt = f"{_compact(installs)} install{'s' if installs != 1 else ''}"
    n = int(app.get("contributor_count") or 0)
    remixes = int(app.get("remix_count") or 0)
    cat = e(_cat_label(app["category"]))
    size = _human(app["size_bytes"]) if app["size_bytes"] else "—"

    # One primary action, its note under it (the store's hero note).
    if hard:
        action = ""
        note = '<span class="err">Withdrawn by Fused</span> · no longer installable'
    else:
        action = (f'<a class="pill" id="install-btn" href="{q(FUSED_RENDER_MARKET_LINK.format(slug=slug))}">Install</a>')
        note = ("Reviewed by Fused" + (f" · release {e(released)}" if released else "") + f" · {install_txt}")
    notice = ""
    if block is not None:
        reason = e(block.get("reason") or "Fused flagged this release.")
        todo = e(block.get("what_to_do") or "")
        head = "Fused withdrew this release" if hard else "Fused flagged this release"
        notice = _callout("hard" if hard else "warn", head, reason + (f"\n{todo}" if todo else ""))
    shot = (f'<img class="stage" alt="{title} screenshot" src="{q(app["preview_url"])}">'
            if app["preview_url"] else "")

    strip = "".join([
        _cell("Category", cat, "Fused Community"),
        _cell("Version", f"v{e(app['version'])}", "Latest"),
        _cell("Released", e(released) if released else "—", "Reviewed by Fused"),
        _cell("Installs", _compact(installs), "Install" if installs == 1 else "Installs", f"{installs} installs"),
        _cell("Remixes", str(remixes), f"Remix of {e(remix['name'])}" if remix else ("Remix" if remixes == 1 else "Remixes")),
    ])

    whats_new = (f'<section class="section"><div class="new"><h2>What\'s new in v{e(app["version"])}</h2>'
                 f'<p>{e(app["whats_new"])}</p></div></section>' if app["whats_new"] else "")
    files = (facts.get("files") or [])[:500]
    more = max(0, int(app["file_count"] or 0) - len(files))
    files_html = (
        '<section class="section"><div class="section-head"><h2 class="section-title">Files</h2>'
        '<span class="section-count" id="files-foot"></span></div>'
        '<table class="files"><thead><tr><th>Path</th><th class="s">Size</th></tr></thead><tbody id="files-body"></tbody>'
        + (f'<tfoot><tr><td colspan="2">… and {more} more</td></tr></tfoot>' if more else "")
        + "</table></section>"
    )
    rel_items = []
    for r in app.get("releases") or []:
        when = e(_fmt_date(r.get("at") or "") or "Release")
        ver = f'<span class="release-v">v{e(r["version"])}</span>' if r.get("version") else ""
        cur = '<span class="release-tag current">Current</span>' if r.get("commit") == commit else ""
        tag = '<span class="release-tag">Blocked</span>' if r.get("blocked") else ""
        rel_items.append(f'<li class="release{" blocked" if r.get("blocked") else ""}"><div class="release-head">'
                         f'<span class="release-when">{when}</span>{ver}{cur}{tag}</div>'
                         f'<p class="release-new">{e(r.get("whats_new") or "No notes for this release yet.")}</p></li>')
    releases = (f'<section class="section"><div class="section-head"><h2 class="section-title">Releases</h2></div>'
                f'<ol class="releases">{"".join(rel_items)}</ol></section>' if rel_items else "")

    info = "".join([
        _info_row("Version", f"v{e(app['version'])}"),
        _info_row("Released", e(released)) if released else "",
        _info_row("Remix of", remix_link) if remix_link else "",
        _info_row("Category", cat),
        _info_row("Developer", author_html),
        _info_row("Size", size),
        _info_row("Files", str(app["file_count"] or len(files) or "—")),
        _info_row("Contributors", f"{n} credited" if n > 1 else "The author"),
        _info_row("Python", "Required (runs locally)" if app.get("requires_python") else "Not required"),
        _info_row("Source", f'<a href="{q(tree)}" target="_blank" rel="noopener">GitHub {_ARROW}</a>'),
    ])
    script = (f"var README={_js(facts.get('readme'))},FILES={_js(files)},ENTRY={_js(facts.get('entry'))},"
              f"NAME={_js(app['name'])},TOTAL={int(app['size_bytes'] or 0)},RAW={_js(f'{RAW_BASE}/{repo}/{commit}/')},"
              f"EMPTY={_js('This app has no README.')};")
    return (
        _head(title) + '<div class="page">'
        f'<div class="top"><span class="brand">Fused Community</span>'
        f'<a class="toplink" href="{q(tree)}" target="_blank" rel="noopener">View on GitHub {_ARROW}</a></div>'
        f'<div class="hero"><div class="icon">{icon}</div><div class="hero-text">'
        f'<h1 class="name">{title}{"" if hard else _VERIFIED}</h1>'
        f'<p class="sub">{e(app["description"])}</p><div class="author">{byline}</div>{tags_html}</div>'
        f'<div class="actions"><div class="action-row">{action}</div><div class="note">{note}</div></div></div>'
        f"{notice}"
        f'<div class="inst" id="inst" hidden><p><b>Didn\'t open?</b> You may need fused-render first.</p>'
        f'<a class="pill alt" href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Download fused-render</a></div>'
        f'<dl class="strip">{strip}</dl>'
        f"{shot}"
        f'<div class="body"><div class="main">{whats_new}'
        f'<section class="section"><div class="md" id="readme"></div></section>'
        f"{files_html}{releases}</div>"
        f'<aside class="aside"><section><h3 class="side-title">Information</h3><dl class="info">{info}</dl></section></aside></div>'
        f'<div class="foot">Reviewed release of <a href="{q(tree)}" target="_blank" rel="noopener">{e(repo)}</a> @ {e(commit[:7])}'
        f' · <a href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Get fused-render</a></div>'
        f"</div><script>{script}{_JS}</script></body></html>"
    )


def _listing_error(slug, message):
    return _state_page("Fused Community", slug or "No app named", message,
                       ("Get fused-render", FUSED_RENDER_DOWNLOAD_PAGE))
