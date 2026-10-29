import io, json, re, zipfile
import html as _html

ICON_NAMES = ("icon.png", "icon.svg", "icon.jpg", "icon.jpeg", "logo.png", "logo.svg")
SHOT_NAMES = ("screenshot.png", "screenshot.jpg", "screenshot.jpeg", "preview.png")
MIME = {"png": "image/png", "svg": "image/svg+xml", "jpg": "image/jpeg", "jpeg": "image/jpeg"}

# fused-render deep link: fused-render://open?git=https://github.com/<owner>/<repo>
FUSED_RENDER_GIT_LINK = "fused-render://open?git=https://github.com/{repo}"
FUSED_RENDER_DOWNLOAD_PAGE = "https://www.fused.io/render"

# The landing page look, shared with fused_app_listing.
_ui = fused.load("app_page_ui")
_ARROW = _ui._ARROW
_JS = _ui._JS
_callout = _ui._callout
_cell = _ui._cell
_head = _ui._head
_human = _ui._human
_img = _ui._img
_info_row = _ui._info_row
_js = _ui._js
_state_page = _ui._state_page


@fused.udf(cache_max_age="10m")
def udf(repo: str = "vasu2001/sine-starter"):
    """Show a landing page for a public GitHub repo that *is* a Fused app.

    The repo source is read from GitHub's zip archive and rendered: README,
    icon, screenshot, files, plus a fused-render `git=` install deep link.
    """
    repo = _clean_repo(repo)
    if not repo:
        return _error(repo, "Pass repo as owner/name, e.g. vasu2001/sine-starter")
    try:
        data = _fetch_zip(repo)
    except Exception as e:
        return _error(repo, f"Could not fetch repo from GitHub: {e}")

    zf = zipfile.ZipFile(io.BytesIO(data))
    members = [i for i in zf.infolist() if not i.is_dir()]
    top = members[0].filename.split("/", 1)[0] + "/" if members else ""
    rel = lambda n: n[len(top):] if n.startswith(top) else n

    app = _read_repo(zf, members, rel)
    return _page(repo, app)


def _clean_repo(repo):
    repo = (repo or "").strip()
    repo = re.sub(r"^(https?://)?(www\.)?github\.com/", "", repo).strip("/")
    repo = re.sub(r"\.git$", "", repo)
    parts = repo.split("/")
    if len(parts) < 2 or not all(re.fullmatch(r"[A-Za-z0-9_.-]+", p) for p in parts[:2]):
        return ""
    return "/".join(parts[:2])


@fused.cache(cache_max_age="10m")
def _fetch_zip(repo):
    import requests
    r = requests.get(f"https://codeload.github.com/{repo}/zip/HEAD", timeout=30)
    if r.status_code == 404:
        raise ValueError("repo not found or not public")
    r.raise_for_status()
    if len(r.content) > 50_000_000:
        raise ValueError("repo archive is larger than 50 MB")
    return r.content


def _read_repo(zf, members, rel):
    names = {rel(i.filename).lower(): i for i in members}
    manifest = {}
    for m in ("manifest.json", "fused.json", "app.json"):
        if m in names:
            try:
                manifest = json.loads(zf.read(names[m].filename).decode("utf-8"))
                break
            except Exception:
                pass

    def find(cands, cap):
        for c in cands:
            for prefix in ("", "files/", "public/", "assets/"):
                i = names.get(prefix + c)
                if i and i.file_size <= cap:
                    return (MIME[c.rsplit(".", 1)[-1]], zf.read(i.filename))
        return None

    readme = None
    for c in ("readme.md", "readme.markdown", "readme"):
        if c in names:
            readme = zf.read(names[c].filename).decode("utf-8", "replace")[:200_000]
            break
    files = sorted(({"name": rel(i.filename), "size": i.file_size} for i in members), key=lambda f: f["name"])
    return {
        "name": manifest.get("name") or "",
        "entry": manifest.get("entry") or ("index.html" if "index.html" in names else ""),
        "readme": readme,
        "icon": find(ICON_NAMES, 512_000),
        "shot": find(SHOT_NAMES, 4_000_000),
        "files": files,
        "total_size": sum(f["size"] for f in files),
    }


def _page(repo, app):
    """The store's detail layout for an UNREVIEWED app read straight from its
    GitHub repo's tip: no reviewed tick, a plain "not reviewed" callout, and
    the git= install deep link."""
    e = _html.escape

    def q(s):
        return e(str(s), quote=True)

    owner, name = repo.split("/", 1)
    title = e(app["name"] or name)
    gh = f"https://github.com/{repo}"
    zip_url = f"https://codeload.github.com/{repo}/zip/HEAD"
    deeplink = FUSED_RENDER_GIT_LINK.format(repo=repo)
    icon = _img(app["icon"], "", "") if app["icon"] else ".fused"
    shot = _img(app["shot"], "stage", f"{app['name'] or name} screenshot") if app["shot"] else ""
    files = [{"path": f["name"], "size": f["size"]} for f in app["files"][:500]]
    more = max(0, len(app["files"]) - len(files))
    entry = app["entry"] or ""
    size = _human(app["total_size"]) if app["total_size"] else "—"
    owner_link = f'<a href="https://github.com/{q(owner)}" target="_blank" rel="noopener">{e(owner)}</a>'
    repo_link = f'<a href="{q(gh)}" target="_blank" rel="noopener">{e(repo)}</a>'

    strip = "".join([
        _cell("Source", "GitHub", e(repo), repo),
        _cell("Branch", "Default", "Latest commit"),
        _cell("Files", str(len(app["files"])), size),
        _cell("Entry", e(entry) if entry else "—", "Opens first" if entry else "No index.html"),
        _cell("Review", "None", "Not reviewed by Fused"),
    ])
    notice = _callout(
        "warn", "Not reviewed by Fused",
        f"This installs the latest commit of {repo_link}'s default branch straight from GitHub. "
        "Fused hasn't checked this code — read it before you run it.")
    files_html = (
        '<section class="section"><div class="section-head"><h2 class="section-title">Files</h2>'
        '<span class="section-count" id="files-foot"></span></div>'
        '<table class="files"><thead><tr><th>Path</th><th class="s">Size</th></tr></thead><tbody id="files-body"></tbody>'
        + (f'<tfoot><tr><td colspan="2">… and {more} more</td></tr></tfoot>' if more else "")
        + "</table></section>"
    )
    info = "".join([
        _info_row("Source", "GitHub · unreviewed"),
        _info_row("Repository", repo_link),
        _info_row("Developer", owner_link),
        _info_row("Size", size),
        _info_row("Files", str(len(app["files"]))),
        _info_row("Entry", e(entry) if entry else "—"),
        _info_row("Archive", f'<a href="{q(zip_url)}">Download .zip {_ARROW}</a>'),
    ])
    script = (f"var README={_js(app['readme'])},FILES={_js(files)},ENTRY={_js(entry)},"
              f"NAME={_js(app['name'] or name)},TOTAL={int(app['total_size'] or 0)},"
              f"RAW={_js(f'https://raw.githubusercontent.com/{repo}/HEAD/')},EMPTY={_js('This repo has no README.')};")
    return (
        _head(title) + '<div class="page">'
        f'<div class="top"><span class="brand">Fused app on GitHub</span>'
        f'<a class="toplink" href="{q(gh)}" target="_blank" rel="noopener">View on GitHub {_ARROW}</a></div>'
        f'<div class="hero"><div class="icon">{icon}</div><div class="hero-text">'
        f'<h1 class="name">{title}</h1>'
        f'<p class="sub">A Fused app, installed straight from its GitHub repository.</p>'
        f'<div class="author">by {owner_link} · {repo_link}</div></div>'
        f'<div class="actions"><div class="action-row">'
        f'<a class="pill" id="install-btn" href="{q(deeplink)}">Install</a>'
        f'<a class="pill alt" href="{q(gh)}" target="_blank" rel="noopener">GitHub</a></div>'
        f'<div class="note"><span class="warn">Not reviewed by Fused</span> · from GitHub</div></div></div>'
        f"{notice}"
        f'<div class="inst" id="inst" hidden><p><b>Didn\'t open?</b> You may need fused-render first.</p>'
        f'<a class="pill alt" href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Download fused-render</a></div>'
        f'<dl class="strip">{strip}</dl>'
        f"{shot}"
        f'<div class="body"><div class="main">'
        f'<section class="section"><div class="md" id="readme"></div></section>'
        f"{files_html}</div>"
        f'<aside class="aside"><section><h3 class="side-title">Information</h3><dl class="info">{info}</dl></section></aside></div>'
        f'<div class="foot">Tip of {repo_link} · not reviewed by Fused'
        f' · <a href="{FUSED_RENDER_DOWNLOAD_PAGE}" target="_blank" rel="noopener">Get fused-render</a></div>'
        f"</div><script>{script}{_JS}</script></body></html>"
    )


def _error(repo, message):
    return _state_page("Fused app on GitHub", repo or "Invalid repo", message,
                       ("Get fused-render", FUSED_RENDER_DOWNLOAD_PAGE))
